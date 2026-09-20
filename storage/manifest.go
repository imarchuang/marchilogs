package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	manifestFileName = "manifest.json"
	partTierSmall    = "small"
	partTierBig      = "big"
)

// dayManifest lists part ids that are visible to Search for one day partition.
type dayManifest struct {
	Parts []string `json:"parts"`
}

func manifestPath(root, day string) string {
	return filepath.Join(root, "partitions", day, manifestFileName)
}

func readDayManifest(path string) (dayManifest, error) {
	var m dayManifest
	err := readJSON(path, &m)
	if err != nil {
		return dayManifest{}, err
	}
	return m, nil
}

func writeDayManifestAtomic(path string, m dayManifest) error {
	if m.Parts == nil {
		m.Parts = []string{}
	}
	sort.Strings(m.Parts)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := writeJSON(tmp, m); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// loadManifestsLocked loads or rebuilds per-day manifests into s.manifests.
// Also removes orphan part directories not listed in the manifest.
func (s *Storage) loadManifestsLocked() error {
	s.manifests = make(map[string][]string)
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
		parts, err := s.ensureDayManifestLocked(day)
		if err != nil {
			return err
		}
		s.manifests[day] = parts
		if err := s.cleanupOrphanPartsLocked(day, parts); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) ensureDayManifestLocked(day string) ([]string, error) {
	path := manifestPath(s.root, day)
	m, err := readDayManifest(path)
	if err == nil {
		return append([]string(nil), m.Parts...), nil
	}
	if !os.IsNotExist(err) {
		// Corrupt or unreadable: rebuild from parts dir.
	}
	partsDir := filepath.Join(s.root, "partitions", day, "parts")
	entries, err := os.ReadDir(partsDir)
	if err != nil {
		if os.IsNotExist(err) {
			if err := writeDayManifestAtomic(path, dayManifest{Parts: nil}); err != nil {
				return nil, err
			}
			return nil, nil
		}
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		if e.IsDir() && isPublishedPartDir(e.Name()) {
			ids = append(ids, e.Name())
		}
	}
	sort.Strings(ids)
	if err := writeDayManifestAtomic(path, dayManifest{Parts: ids}); err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *Storage) cleanupOrphanPartsLocked(day string, active []string) error {
	activeSet := make(map[string]struct{}, len(active))
	for _, id := range active {
		activeSet[id] = struct{}{}
	}
	partsDir := filepath.Join(s.root, "partitions", day, "parts")
	entries, err := os.ReadDir(partsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if !isPublishedPartDir(name) {
			continue
		}
		if _, ok := activeSet[name]; ok {
			continue
		}
		// Orphan: on disk but not in manifest (crash after part rename, before manifest).
		_ = os.RemoveAll(filepath.Join(partsDir, name))
	}
	return nil
}

// addPartToManifestLocked appends partID and atomically rewrites the day manifest.
func (s *Storage) addPartToManifestLocked(day, partID string) error {
	cur := append([]string(nil), s.manifests[day]...)
	for _, id := range cur {
		if id == partID {
			return nil
		}
	}
	cur = append(cur, partID)
	sort.Strings(cur)
	if err := writeDayManifestAtomic(manifestPath(s.root, day), dayManifest{Parts: cur}); err != nil {
		return fmt.Errorf("manifest add %s/%s: %w", day, partID, err)
	}
	s.manifests[day] = cur
	return s.rebuildDayIndexDBLocked(day)
}

// replaceManifestPartsLocked removes removeIDs and adds addIDs in one manifest write.
func (s *Storage) replaceManifestPartsLocked(day string, removeIDs, addIDs []string) error {
	remove := make(map[string]struct{}, len(removeIDs))
	for _, id := range removeIDs {
		remove[id] = struct{}{}
	}
	var next []string
	for _, id := range s.manifests[day] {
		if _, drop := remove[id]; drop {
			continue
		}
		next = append(next, id)
	}
	next = append(next, addIDs...)
	// dedupe
	seen := make(map[string]struct{}, len(next))
	out := make([]string, 0, len(next))
	for _, id := range next {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Strings(out)
	if err := writeDayManifestAtomic(manifestPath(s.root, day), dayManifest{Parts: out}); err != nil {
		return err
	}
	s.manifests[day] = out
	return s.rebuildDayIndexDBLocked(day)
}

func maxPartID(ids []string) int {
	max := 0
	for _, id := range ids {
		n, err := strconv.Atoi(id)
		if err != nil {
			continue
		}
		if n > max {
			max = n
		}
	}
	return max
}
