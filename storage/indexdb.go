package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

const indexdbFileName = "indexdb.json"

// dayIndexDB is a per-day stream catalog: which active parts can match a tag.
// Unlike part meta by_tag (stream ids), this maps tag → part ids so Search can
// skip whole parts before opening their meta.json (VL indexdb-inspired, minimal).
type dayIndexDB struct {
	ByTag map[string][]string `json:"by_tag"` // "service=api" → ["000001","000003"]
}

func indexdbPath(root, day string) string {
	return filepath.Join(root, "partitions", day, indexdbFileName)
}

func emptyDayIndexDB() *dayIndexDB {
	return &dayIndexDB{ByTag: make(map[string][]string)}
}

func writeDayIndexDBAtomic(path string, idx *dayIndexDB) error {
	if idx == nil {
		idx = emptyDayIndexDB()
	}
	if idx.ByTag == nil {
		idx.ByTag = make(map[string][]string)
	}
	for k := range idx.ByTag {
		sort.Strings(idx.ByTag[k])
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := writeJSON(tmp, idx); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readDayIndexDB(path string) (*dayIndexDB, error) {
	var idx dayIndexDB
	if err := readJSON(path, &idx); err != nil {
		return nil, err
	}
	if idx.ByTag == nil {
		idx.ByTag = make(map[string][]string)
	}
	return &idx, nil
}

// loadIndexDBsLocked loads or rebuilds per-day indexdb beside manifests.
func (s *Storage) loadIndexDBsLocked() error {
	s.indexdbs = make(map[string]*dayIndexDB)
	for day := range s.manifests {
		if err := s.ensureDayIndexDBLocked(day); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) ensureDayIndexDBLocked(day string) error {
	path := indexdbPath(s.root, day)
	idx, err := readDayIndexDB(path)
	if err == nil {
		s.indexdbs[day] = idx
		return nil
	}
	if !os.IsNotExist(err) {
		// corrupt → rebuild
	}
	return s.rebuildDayIndexDBLocked(day)
}

// rebuildDayIndexDBLocked scans active part metas and rewrites indexdb.json.
func (s *Storage) rebuildDayIndexDBLocked(day string) error {
	idx := emptyDayIndexDB()
	for _, partID := range s.manifests[day] {
		var meta partMeta
		path := filepath.Join(s.root, "partitions", day, "parts", partID, "meta.json")
		if err := readJSON(path, &meta); err != nil {
			return fmt.Errorf("indexdb rebuild %s/%s: %w", day, partID, err)
		}
		addPartToIndexDB(idx, partID, meta)
	}
	if err := writeDayIndexDBAtomic(indexdbPath(s.root, day), idx); err != nil {
		return err
	}
	s.indexdbs[day] = idx
	return nil
}

func addPartToIndexDB(idx *dayIndexDB, partID string, meta partMeta) {
	seen := make(map[string]struct{})
	addTag := func(tk string) {
		if _, ok := seen[tk]; ok {
			return
		}
		seen[tk] = struct{}{}
		for _, id := range idx.ByTag[tk] {
			if id == partID {
				return
			}
		}
		idx.ByTag[tk] = append(idx.ByTag[tk], partID)
	}
	if len(meta.ByTag) > 0 {
		for tk := range meta.ByTag {
			addTag(tk)
		}
		return
	}
	for _, sm := range meta.Streams {
		tags := sm.Tags
		if tags == nil {
			tags = ParseStreamTags(sm.ID)
		}
		for k, v := range tags {
			addTag(tagKey(k, v))
		}
	}
}

// partsMatchingStreamEq returns part ids that may contain streams matching streamEq.
// Empty streamEq → all parts. Missing indexdb → all parts (safe).
func (s *Storage) partsMatchingStreamEq(day string, parts []string, streamEq map[string]string) []string {
	if len(streamEq) == 0 || len(parts) == 0 {
		return parts
	}
	idx := s.indexdbs[day]
	if idx == nil || len(idx.ByTag) == 0 {
		return parts
	}

	var candidates map[string]struct{}
	first := true
	for k, v := range streamEq {
		if v == "" {
			continue
		}
		ids := idx.ByTag[tagKey(k, v)]
		set := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			set[id] = struct{}{}
		}
		if first {
			candidates = set
			first = false
			continue
		}
		for id := range candidates {
			if _, ok := set[id]; !ok {
				delete(candidates, id)
			}
		}
	}
	if first {
		return parts
	}

	active := make(map[string]struct{}, len(parts))
	for _, id := range parts {
		active[id] = struct{}{}
	}
	out := make([]string, 0, len(candidates))
	for id := range candidates {
		if _, ok := active[id]; ok {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}
