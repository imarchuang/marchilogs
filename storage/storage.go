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
}

func (o *Options) withDefaults() Options {
	out := *o
	if len(out.StreamFields) == 0 {
		out.StreamFields = []string{"service", "host"}
	}
	if out.MaxRowsPerBlock <= 0 {
		out.MaxRowsPerBlock = 1024
	}
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
type Storage struct {
	root string
	opts Options

	mu      sync.Mutex
	buffers map[string]*memBlock // key: partition+"\x00"+stream
	partSeq map[string]int       // next part id per partition
}

// Open creates or opens a storage rooted at dir.
func Open(dir string, opts Options) (*Storage, error) {
	opts = opts.withDefaults()
	if err := os.MkdirAll(filepath.Join(dir, "partitions"), 0o755); err != nil {
		return nil, err
	}
	s := &Storage{
		root:    dir,
		opts:    opts,
		buffers: make(map[string]*memBlock),
		partSeq: make(map[string]int),
	}
	if err := s.cleanupOrphanPublishing(); err != nil {
		return nil, err
	}
	if err := s.loadPartSeq(); err != nil {
		return nil, err
	}
	return s, nil
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
			if !e.IsDir() || !strings.HasPrefix(e.Name(), publishingPrefix) {
				continue
			}
			if err := os.RemoveAll(filepath.Join(partsDir, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Storage) loadPartSeq() error {
	partsRoot := filepath.Join(s.root, "partitions")
	days, err := os.ReadDir(partsRoot)
	if err != nil {
		return err
	}
	for _, d := range days {
		if !d.IsDir() {
			continue
		}
		maxID := 0
		partDir := filepath.Join(partsRoot, d.Name(), "parts")
		entries, err := os.ReadDir(partDir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
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
func (s *Storage) Append(entries ...Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, raw := range entries {
		e := raw.normalize(s.opts.StreamFields)
		part := partitionName(e.Time)
		stream := e.Fields[FieldStream]
		key := bufKey(part, stream)
		b := s.buffers[key]
		if b == nil {
			b = newMemBlock(stream)
			s.buffers[key] = b
		}
		b.add(e)
		if b.rows() >= s.opts.MaxRowsPerBlock {
			if err := s.flushPartitionLocked(part); err != nil {
				return err
			}
		}
	}
	return nil
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

	// Atomic publish: part becomes visible to readers only after rename succeeds.
	if err := os.Rename(tmpDir, finalDir); err != nil {
		rollback()
		return fmt.Errorf("publish part %s: %w", finalName, err)
	}

	for _, it := range items {
		delete(s.buffers, it.key)
	}
	return nil
}

// Close flushes and releases the storage.
func (s *Storage) Close() error {
	return s.Flush()
}

// Search runs: in-memory parts + on-disk parts.
// Buffered rows are visible immediately via inmemoryPart snapshots; Flush is not required.
// Mem snapshot and the set of published disk parts are captured under one lock so a concurrent
// Flush cannot make the same rows appear twice (once from mem, once from the newly published part).
func (s *Storage) Search(q Query) ([]Entry, error) {
	s.mu.Lock()
	memParts := s.snapshotInmemoryLocked()
	diskParts, err := s.listPublishedPartsLocked(q.Start, q.End)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}

	partitions := mergePartitionNames(keysOf(diskParts), memParts, q.Start, q.End)

	var out []Entry
	for _, partName := range partitions {
		if ip := memParts[partName]; ip != nil {
			out = searchInmemoryPart(ip, q, out)
			if q.Limit > 0 && len(out) >= q.Limit {
				return out[:q.Limit], nil
			}
		}

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

// listPublishedPartsLocked returns partition → published part dir names.
// Caller must hold s.mu (pairs with mem snapshot for a consistent Search view).
func (s *Storage) listPublishedPartsLocked(start, end time.Time) (map[string][]string, error) {
	days, err := s.listPartitions(start, end)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string][]string{}, nil
		}
		return nil, err
	}
	out := make(map[string][]string, len(days))
	for _, day := range days {
		partsDir := filepath.Join(s.root, "partitions", day, "parts")
		entries, err := os.ReadDir(partsDir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		var names []string
		for _, e := range entries {
			if e.IsDir() && isPublishedPartDir(e.Name()) {
				names = append(names, e.Name())
			}
		}
		sort.Strings(names)
		if len(names) > 0 {
			out[day] = names
		}
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
