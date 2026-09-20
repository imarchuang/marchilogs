package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Options configures a Storage.
type Options struct {
	// StreamFields are low-cardinality identity fields used for physical grouping.
	// High-cardinality fields (trace_id, user_id) must NOT be listed here.
	StreamFields []string

	// MaxRowsPerBlock flushes a stream block when it reaches this many rows.
	MaxRowsPerBlock int

	// InmemoryDataFlushInterval is how often buffered data is flushed to disk parts
	// so it survives unclean shutdown (VictoriaLogs-style durability window).
	// Default 5s; values in (0, 1s) are raised to 1s. Set to a negative duration to disable.
	InmemoryDataFlushInterval time.Duration

	// EnableWAL turns on the write-ahead log for unflushed buffers (default off).
	// When disabled, unclean shutdown can lose data not yet Flush()'d to parts
	// (bounded by InmemoryDataFlushInterval when periodic flush is enabled).
	EnableWAL bool

	// WALSync fsyncs the WAL after each Append batch (default true when WAL enabled).
	// Set false for faster ingest with softer durability (OS buffer).
	WALSync *bool

	// MergeMinParts starts a small→big merge when a day has at least this many small parts.
	// Default 4.
	MergeMinParts int

	// MergeMaxPartsPerJob caps how many small parts one merge consumes. Default 8.
	MergeMaxPartsPerJob int

	// MergeCheckInterval is how often the background worker looks for merge work.
	// Default 10s; ≤0 disables the worker (manual/tests can still call runMergePass).
	MergeCheckInterval time.Duration

	// MergeDisable turns off the background merge worker only.
	// Manual runMergePass still works (tests / force-merge later).
	MergeDisable bool
}

func (o *Options) withDefaults() Options {
	out := *o
	if len(out.StreamFields) == 0 {
		out.StreamFields = []string{"service", "host"}
	}
	if out.MaxRowsPerBlock <= 0 {
		out.MaxRowsPerBlock = 1024
	}
	if out.InmemoryDataFlushInterval == 0 {
		out.InmemoryDataFlushInterval = 5 * time.Second
	} else if out.InmemoryDataFlushInterval > 0 && out.InmemoryDataFlushInterval < time.Second {
		out.InmemoryDataFlushInterval = time.Second
	}
	if out.WALSync == nil {
		v := true
		out.WALSync = &v
	}
	out.withMergeDefaults()
	return out
}

// Query is the minimal prune+filter request.
// Path: day partitions → matching streams → overlapping blocks → row filter.
type Query struct {
	Start time.Time // inclusive; zero = no lower bound
	End   time.Time // inclusive; zero = no upper bound

	// StreamEq matches streams whose tags contain all of these field=value pairs (AND subset).
	// Example: {service: "api"} matches host=h1,service=api and host=h2,service=api.
	StreamEq map[string]string

	// Contains does a case-sensitive substring match on _msg after unpacking the block.
	Contains string

	Limit int // 0 = no limit
}

// Storage is the most basic marchilogs store:
//
//	data/partitions/YYYYMMDD/parts/<id>/blocks/<stream>/...
//	data/wal/wal.log + checkpoint   (optional; EnableWAL)
type Storage struct {
	root string
	opts Options

	mu      sync.RWMutex
	buffers map[string]*memBlock // key: partition+"\x00"+stream
	partSeq map[string]int       // next part id per partition
	wal     *wal
	// manifests: day partition → active part ids (source of truth for Search).
	manifests map[string][]string

	flushStop chan struct{}
	flushDone chan struct{}

	mergeStop chan struct{}
	mergeDone chan struct{}
}

// Open creates or opens a storage rooted at dir.
func Open(dir string, opts Options) (*Storage, error) {
	opts = opts.withDefaults()
	if err := os.MkdirAll(filepath.Join(dir, "partitions"), 0o755); err != nil {
		return nil, err
	}
	s := &Storage{
		root:      dir,
		opts:      opts,
		buffers:   make(map[string]*memBlock),
		partSeq:   make(map[string]int),
		manifests: make(map[string][]string),
	}
	if err := s.cleanupOrphanPublishing(); err != nil {
		return nil, err
	}
	if err := s.loadManifestsLocked(); err != nil {
		return nil, err
	}
	if err := s.loadPartSeq(); err != nil {
		return nil, err
	}
	if opts.EnableWAL {
		w, err := openWAL(dir, *opts.WALSync)
		if err != nil {
			return nil, err
		}
		s.wal = w
		if err := s.replayWAL(); err != nil {
			_ = w.close()
			return nil, err
		}
	}
	s.startPeriodicFlush()
	s.startPeriodicMerge()
	return s, nil
}

func (s *Storage) startPeriodicFlush() {
	every := s.opts.InmemoryDataFlushInterval
	if every <= 0 {
		return
	}
	s.flushStop = make(chan struct{})
	s.flushDone = make(chan struct{})
	go func() {
		defer close(s.flushDone)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				_ = s.Flush()
			case <-s.flushStop:
				return
			}
		}
	}()
}

func (s *Storage) replayWAL() error {
	if s.wal == nil {
		return nil
	}
	return s.wal.replayAfterCheckpoint(func(rec walRecord) error {
		e := Entry{
			Time:   time.Unix(0, rec.TimeNS).UTC(),
			Fields: rec.Fields,
		}
		e = e.normalize(s.opts.StreamFields)
		s.applyLocked(e, rec.Seq)
		return nil
	})
}

// publishingPrefix marks in-progress part directories. Readers must ignore them;
// a successful Flush renames to a numeric id (atomic publish).
const publishingPrefix = ".publishing-"

func isPublishedPartDir(name string) bool {
	if name == "" || strings.HasPrefix(name, ".") {
		return false
	}
	_, err := strconv.Atoi(name)
	return err == nil
}

func (s *Storage) cleanupOrphanPublishing() error {
	partsRoot := filepath.Join(s.root, "partitions")
	days, err := os.ReadDir(partsRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, d := range days {
		if !d.IsDir() {
			continue
		}
		partsDir := filepath.Join(partsRoot, d.Name(), "parts")
		entries, err := os.ReadDir(partsDir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := e.Name()
			if strings.HasPrefix(name, publishingPrefix) || strings.HasPrefix(name, ".merging-") {
				if err := os.RemoveAll(filepath.Join(partsDir, name)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *Storage) loadPartSeq() error {
	for day, ids := range s.manifests {
		if max := maxPartID(ids); max > s.partSeq[day] {
			s.partSeq[day] = max
		}
	}
	// Also scan dirs so seq stays ahead of orphan/tmp leftovers.
	partsRoot := filepath.Join(s.root, "partitions")
	days, err := os.ReadDir(partsRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, d := range days {
		if !d.IsDir() {
			continue
		}
		partDir := filepath.Join(partsRoot, d.Name(), "parts")
		entries, err := os.ReadDir(partDir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		maxID := s.partSeq[d.Name()]
		for _, e := range entries {
			if !e.IsDir() || !isPublishedPartDir(e.Name()) {
				continue
			}
			id, err := strconv.Atoi(e.Name())
			if err != nil {
				continue
			}
			if id > maxID {
				maxID = id
			}
		}
		s.partSeq[d.Name()] = maxID
	}
	return nil
}

func bufKey(partition, stream string) string {
	return partition + "\x00" + stream
}

// Append adds log entries into in-memory stream blocks (flushes when full).
// With WAL enabled, records are fsynced to wal.log before becoming queryable in buffers.
func (s *Storage) Append(entries ...Entry) error {
	if len(entries) == 0 {
		return nil
	}
	normalized := make([]Entry, 0, len(entries))
	for _, raw := range entries {
		normalized = append(normalized, raw.normalize(s.opts.StreamFields))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var seqs []uint64
	if s.wal != nil {
		var err error
		seqs, err = s.wal.appendEntries(normalized)
		if err != nil {
			return err
		}
	} else {
		seqs = make([]uint64, len(normalized))
	}

	for i, e := range normalized {
		part := partitionName(e.Time)
		s.applyLocked(e, seqs[i])
		key := bufKey(part, e.Fields[FieldStream])
		if b := s.buffers[key]; b != nil && b.rows() >= s.opts.MaxRowsPerBlock {
			if err := s.flushPartitionLocked(part); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Storage) applyLocked(e Entry, seq uint64) {
	part := partitionName(e.Time)
	stream := e.Fields[FieldStream]
	key := bufKey(part, stream)
	b := s.buffers[key]
	if b == nil {
		b = newMemBlock(stream)
		s.buffers[key] = b
	}
	b.addWithSeq(e, seq)
}

// Flush writes all in-memory blocks to disk as one new part per partition touched.
func (s *Storage) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	parts := make(map[string]struct{})
	for key := range s.buffers {
		parts[strings.SplitN(key, "\x00", 2)[0]] = struct{}{}
	}
	for part := range parts {
		if err := s.flushPartitionLocked(part); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) flushPartitionLocked(partition string) error {
	type item struct {
		key string
		b   *memBlock
	}
	var items []item
	prefix := partition + "\x00"
	for key, b := range s.buffers {
		if strings.HasPrefix(key, prefix) && b.rows() > 0 {
			items = append(items, item{key: key, b: b})
		}
	}
	if len(items) == 0 {
		return nil
	}
	sort.Slice(items, func(i, j int) bool { return items[i].b.stream < items[j].b.stream })

	partsParent := filepath.Join(s.root, "partitions", partition, "parts")
	if err := os.MkdirAll(partsParent, 0o755); err != nil {
		return err
	}

	s.partSeq[partition]++
	id := s.partSeq[partition]
	finalName := fmt.Sprintf("%06d", id)
	finalDir := filepath.Join(partsParent, finalName)
	tmpDir := filepath.Join(partsParent, publishingPrefix+finalName)
	_ = os.RemoveAll(tmpDir)

	rollback := func() {
		_ = os.RemoveAll(tmpDir)
		s.partSeq[partition]--
	}

	blocksDir := filepath.Join(tmpDir, "blocks")
	if err := os.MkdirAll(blocksDir, 0o755); err != nil {
		rollback()
		return err
	}

	infos := make([]streamFlushInfo, 0, len(items))
	var tMin, tMax int64
	for i, it := range items {
		dir := filepath.Join(blocksDir, safeStreamDir(it.b.stream))
		if err := it.b.writeTo(dir); err != nil {
			rollback()
			return err
		}
		infos = append(infos, streamFlushInfo{
			ID:        it.b.stream,
			TimeMinNS: it.b.timeMin,
			TimeMaxNS: it.b.timeMax,
			Rows:      it.b.rows(),
		})
		if i == 0 {
			tMin, tMax = it.b.timeMin, it.b.timeMax
		} else {
			if it.b.timeMin < tMin {
				tMin = it.b.timeMin
			}
			if it.b.timeMax > tMax {
				tMax = it.b.timeMax
			}
		}
	}

	if err := writeJSON(filepath.Join(tmpDir, "meta.json"), buildPartMeta(infos, tMin, tMax)); err != nil {
		rollback()
		return err
	}

	// Atomic publish: part dir rename, then manifest swap (Search visibility).
	if err := os.Rename(tmpDir, finalDir); err != nil {
		rollback()
		return fmt.Errorf("publish part %s: %w", finalName, err)
	}
	if err := s.addPartToManifestLocked(partition, finalName); err != nil {
		// Part is on disk but invisible; Open will treat it as orphan and delete.
		rollback()
		_ = os.RemoveAll(finalDir)
		return err
	}

	removed := make(map[string]struct{}, len(items))
	for _, it := range items {
		removed[it.key] = struct{}{}
	}
	// Persist WAL checkpoint before dropping buffers so a crash cannot replay
	// already-published rows back into memory (duplicate with disk parts).
	if err := s.advanceWALCheckpointLocked(removed); err != nil {
		return err
	}
	for _, it := range items {
		delete(s.buffers, it.key)
	}
	if s.wal != nil {
		return s.wal.compactLocked()
	}
	return nil
}

// advanceWALCheckpointLocked sets checkpoint to min(unflushedSeq)-1, treating
// removedKeys as already gone from buffers. Caller holds write lock.
func (s *Storage) advanceWALCheckpointLocked(removedKeys map[string]struct{}) error {
	if s.wal == nil {
		return nil
	}
	minUnflushed := s.wal.nextSeq
	for key, b := range s.buffers {
		if _, skip := removedKeys[key]; skip {
			continue
		}
		for _, seq := range b.seqs {
			if seq > 0 && seq < minUnflushed {
				minUnflushed = seq
			}
		}
	}
	var newCP uint64
	if minUnflushed == 0 {
		newCP = 0
	} else {
		newCP = minUnflushed - 1
	}
	return s.wal.setCheckpoint(newCP)
}

// Close stops background workers, flushes buffers to parts, checkpoints WAL, and closes the WAL file.
func (s *Storage) Close() error {
	s.stopPeriodicMerge()
	if s.flushStop != nil {
		close(s.flushStop)
		<-s.flushDone
		s.flushStop = nil
	}
	flushErr := s.Flush()
	s.mu.Lock()
	var closeErr error
	if s.wal != nil {
		closeErr = s.wal.close()
		s.wal = nil
	}
	s.mu.Unlock()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// Search runs: in-memory buffers (under RLock, no clone) + on-disk parts.
// Buffered rows are visible immediately; Flush is not required for queryability.
// See QUERY_CONCURRENCY.md for alternative designs (prune-then-clone, generational COW).
func (s *Storage) Search(q Query) ([]Entry, error) {
	s.mu.RLock()
	out := s.searchBuffersRLocked(q, nil)
	if q.Limit > 0 && len(out) >= q.Limit {
		s.mu.RUnlock()
		return out[:q.Limit], nil
	}
	diskParts, err := s.listPublishedPartsLocked(q.Start, q.End)
	s.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	partNames := keysOf(diskParts)
	for _, partName := range partNames {
		for _, peName := range diskParts[partName] {
			partDir := filepath.Join(s.root, "partitions", partName, "parts", peName)
			candidates, err := s.streamsForPart(partDir, q.StreamEq)
			if err != nil {
				return nil, err
			}
			if len(candidates) == 0 {
				continue
			}
			blocksDir := filepath.Join(partDir, "blocks")
			for _, sm := range candidates {
				refs, err := s.blockRefsForStream(blocksDir, sm)
				if err != nil {
					return nil, err
				}
				refs = filterBlockRefs(refs, q.Start, q.End)
				for _, ref := range refs {
					dir := filepath.Join(partDir, ref.Path)
					b, err := readBlock(dir)
					if err != nil {
						return nil, err
					}
					for i := 0; i < b.rows(); i++ {
						e := b.row(i)
						if !q.Start.IsZero() && e.Time.Before(q.Start) {
							continue
						}
						if !q.End.IsZero() && e.Time.After(q.End) {
							continue
						}
						if q.Contains != "" && !strings.Contains(e.Msg(), q.Contains) {
							continue
						}
						out = append(out, e)
						if q.Limit > 0 && len(out) >= q.Limit {
							return out, nil
						}
					}
				}
			}
		}
	}
	return out, nil
}

// listPublishedPartsLocked returns partition → active part dir names from manifests.
// Caller must hold s.mu for read or write.
func (s *Storage) listPublishedPartsLocked(start, end time.Time) (map[string][]string, error) {
	out := make(map[string][]string)
	for day, ids := range s.manifests {
		if len(ids) == 0 {
			continue
		}
		if !partitionOverlapsQuery(day, start, end) {
			continue
		}
		out[day] = append([]string(nil), ids...)
	}
	return out, nil
}

func keysOf(m map[string][]string) []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

func (s *Storage) streamsForPart(partDir string, streamEq map[string]string) (map[string]streamMeta, error) {
	var meta partMeta
	err := readJSON(filepath.Join(partDir, "meta.json"), &meta)
	if err == nil && len(meta.Streams) > 0 {
		if len(meta.ByTag) > 0 || len(streamEq) == 0 {
			return matchStreamIDs(meta, streamEq), nil
		}
		// legacy: streams listed but no by_tag — filter by parsed tags
		out := make(map[string]streamMeta)
		for _, sm := range meta.Streams {
			if sm.Tags == nil {
				sm.Tags = ParseStreamTags(sm.ID)
			}
			if sm.Dir == "" {
				sm.Dir = safeStreamDir(sm.ID)
			}
			if streamMatchesTags(sm.ID, streamEq) {
				out[sm.ID] = sm
			}
		}
		return out, nil
	}

	// very old / missing meta: scan block dirs and subset-match on directory name
	blocksDir := filepath.Join(partDir, "blocks")
	blockDirs, err := os.ReadDir(blocksDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make(map[string]streamMeta)
	for _, bd := range blockDirs {
		if !bd.IsDir() {
			continue
		}
		// directory name is safeStreamDir(streamID); for our safe alphabet it equals streamID
		id := bd.Name()
		if !streamMatchesTags(id, streamEq) {
			continue
		}
		out[id] = streamMeta{ID: id, Dir: bd.Name(), Tags: ParseStreamTags(id)}
	}
	return out, nil
}

// blockRefsForStream returns block path/time refs from part index, or peeks block meta.json only.
func (s *Storage) blockRefsForStream(blocksDir string, sm streamMeta) ([]blockRef, error) {
	if len(sm.Blocks) > 0 {
		return sm.Blocks, nil
	}
	// Legacy parts without blocks[]: read meta.json only (still no .col files).
	dirName := sm.Dir
	if dirName == "" {
		dirName = safeStreamDir(sm.ID)
	}
	abs := filepath.Join(blocksDir, dirName)
	bm, err := readBlockMeta(abs)
	if err != nil {
		return nil, err
	}
	return []blockRef{{
		Path:      filepath.Join("blocks", dirName),
		TimeMinNS: bm.TimeMinNS,
		TimeMaxNS: bm.TimeMaxNS,
		Rows:      bm.Rows,
	}}, nil
}

func (s *Storage) listPartitions(start, end time.Time) ([]string, error) {
	root := filepath.Join(s.root, "partitions")
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		day, err := parsePartitionName(e.Name())
		if err != nil {
			continue
		}
		dayEnd := day.Add(24*time.Hour - time.Nanosecond)
		if !end.IsZero() && day.After(end) {
			continue
		}
		if !start.IsZero() && dayEnd.Before(start) {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}
