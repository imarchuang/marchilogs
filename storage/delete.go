package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const deletesFileName = "deletes.json"

// DeleteSpec is a logical tombstone: rows matching all set predicates become invisible to Search.
type DeleteSpec struct {
	Start    time.Time
	End      time.Time
	StreamEq map[string]string
	Contains string
}

// deleteRecord is persisted per day under partitions/YYYYMMDD/deletes.json.
type deleteRecord struct {
	ID        string            `json:"id"`
	CreatedNS int64             `json:"created_ns"`
	StartNS   int64             `json:"start_ns,omitempty"`
	EndNS     int64             `json:"end_ns,omitempty"`
	StreamEq  map[string]string `json:"stream_eq,omitempty"`
	Contains  string            `json:"contains,omitempty"`
}

type dayDeletesFile struct {
	Deletes []deleteRecord `json:"deletes"`
}

func deletesPath(root, day string) string {
	return filepath.Join(root, "partitions", day, deletesFileName)
}

func (r deleteRecord) hidesEntry(e Entry) bool {
	ts := e.Time.UnixNano()
	if r.StartNS != 0 && ts < r.StartNS {
		return false
	}
	if r.EndNS != 0 && ts > r.EndNS {
		return false
	}
	if len(r.StreamEq) > 0 {
		if stream := e.Fields[FieldStream]; stream != "" {
			if !streamMatchesTags(stream, r.StreamEq) {
				return false
			}
		} else {
			for k, v := range r.StreamEq {
				if v == "" {
					continue
				}
				if e.Fields[k] != v {
					return false
				}
			}
		}
	}
	if r.Contains != "" && !strings.Contains(e.Msg(), r.Contains) {
		return false
	}
	return true
}

func (r deleteRecord) mayHideBlock(streamID string, timeMinNS, timeMaxNS int64) bool {
	if r.StartNS != 0 && timeMaxNS < r.StartNS {
		return false
	}
	if r.EndNS != 0 && timeMinNS > r.EndNS {
		return false
	}
	if !streamMatchesTags(streamID, r.StreamEq) {
		return false
	}
	// Contains cannot be decided at block level without bloom; keep block.
	return true
}

func entryHiddenByDeletes(e Entry, dels []deleteRecord) bool {
	for _, d := range dels {
		if d.hidesEntry(e) {
			return true
		}
	}
	return false
}

func writeDayDeletesAtomic(path string, dels []deleteRecord) error {
	if dels == nil {
		dels = []deleteRecord{}
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := writeJSON(tmp, dayDeletesFile{Deletes: dels}); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readDayDeletes(path string) ([]deleteRecord, error) {
	var f dayDeletesFile
	if err := readJSON(path, &f); err != nil {
		return nil, err
	}
	return f.Deletes, nil
}

func (s *Storage) loadDeletesLocked() error {
	s.deletes = make(map[string][]deleteRecord)
	s.deleteSeq = make(map[string]int)
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
		day := d.Name()
		if _, err := parsePartitionName(day); err != nil {
			continue
		}
		recs, err := readDayDeletes(deletesPath(s.root, day))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		s.deletes[day] = recs
		for _, r := range recs {
			if n := parseDeleteSeq(r.ID); n > s.deleteSeq[day] {
				s.deleteSeq[day] = n
			}
		}
	}
	return nil
}

func parseDeleteSeq(id string) int {
	id = strings.TrimPrefix(id, "d-")
	n, err := strconv.Atoi(id)
	if err != nil {
		return 0
	}
	return n
}

// Delete appends tombstones for overlapping day partitions and purges matching in-memory rows.
func (s *Storage) Delete(spec DeleteSpec) (int, error) {
	if spec.Start.IsZero() && spec.End.IsZero() && len(spec.StreamEq) == 0 && spec.Contains == "" {
		return 0, fmt.Errorf("delete requires at least one of start, end, stream_eq, contains")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	purged := s.purgeBuffersMatchingLocked(spec)

	days, err := s.daysForDeleteLocked(spec.Start, spec.End)
	if err != nil {
		return purged, err
	}
	rec := deleteRecord{
		CreatedNS: time.Now().UTC().UnixNano(),
		Contains:  spec.Contains,
	}
	if !spec.Start.IsZero() {
		rec.StartNS = spec.Start.UnixNano()
	}
	if !spec.End.IsZero() {
		rec.EndNS = spec.End.UnixNano()
	}
	if len(spec.StreamEq) > 0 {
		rec.StreamEq = make(map[string]string, len(spec.StreamEq))
		for k, v := range spec.StreamEq {
			rec.StreamEq[k] = v
		}
	}

	for _, day := range days {
		s.deleteSeq[day]++
		rec.ID = fmt.Sprintf("d-%06d", s.deleteSeq[day])
		// copy for append (ID differs per day)
		dayRec := rec
		cur := append([]deleteRecord(nil), s.deletes[day]...)
		cur = append(cur, dayRec)
		if err := writeDayDeletesAtomic(deletesPath(s.root, day), cur); err != nil {
			return purged, fmt.Errorf("write deletes %s: %w", day, err)
		}
		s.deletes[day] = cur
	}
	return purged, nil
}

func (s *Storage) daysForDeleteLocked(start, end time.Time) ([]string, error) {
	seen := make(map[string]struct{})
	var out []string
	add := func(day string) {
		if _, ok := seen[day]; ok {
			return
		}
		if !partitionOverlapsQuery(day, start, end) {
			return
		}
		seen[day] = struct{}{}
		out = append(out, day)
	}
	for day := range s.manifests {
		add(day)
	}
	for day := range s.deletes {
		add(day)
	}
	// Also discover on-disk days (empty manifest edge).
	partsRoot := filepath.Join(s.root, "partitions")
	if entries, err := os.ReadDir(partsRoot); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				if _, err := parsePartitionName(e.Name()); err == nil {
					add(e.Name())
				}
			}
		}
	}
	// Ensure at least the start/end calendar days exist as tombstone targets
	// when the window is set but no partition dirs yet (mem-only was purged).
	if len(out) == 0 {
		if !start.IsZero() {
			add(partitionName(start))
		}
		if !end.IsZero() {
			add(partitionName(end))
		}
	}
	if len(out) == 0 && start.IsZero() && end.IsZero() {
		// Global delete with no days yet: still OK (buffers already purged).
		return nil, nil
	}
	sort.Strings(out)
	return out, nil
}

func (s *Storage) purgeBuffersMatchingLocked(spec DeleteSpec) int {
	n := 0
	for key, b := range s.buffers {
		if b == nil || b.rows() == 0 {
			continue
		}
		kept := newMemBlock(b.stream)
		for i := 0; i < b.rows(); i++ {
			e := b.row(i)
			if deleteSpecHides(spec, e) {
				n++
				continue
			}
			kept.addWithSeq(e, b.seqs[i])
		}
		if kept.rows() == 0 {
			delete(s.buffers, key)
		} else {
			s.buffers[key] = kept
		}
	}
	return n
}

func deleteSpecHides(spec DeleteSpec, e Entry) bool {
	r := deleteRecord{
		Contains: spec.Contains,
		StreamEq: spec.StreamEq,
	}
	if !spec.Start.IsZero() {
		r.StartNS = spec.Start.UnixNano()
	}
	if !spec.End.IsZero() {
		r.EndNS = spec.End.UnixNano()
	}
	return r.hidesEntry(e)
}

func (s *Storage) deletesForDayLocked(day string) []deleteRecord {
	return append([]deleteRecord(nil), s.deletes[day]...)
}

// deletesOverlappingQuery returns tombstones for days that overlap the query window.
func (s *Storage) deletesOverlappingQueryLocked(start, end time.Time) map[string][]deleteRecord {
	out := make(map[string][]deleteRecord)
	for day, dels := range s.deletes {
		if len(dels) == 0 {
			continue
		}
		if !partitionOverlapsQuery(day, start, end) {
			continue
		}
		out[day] = append([]deleteRecord(nil), dels...)
	}
	return out
}
