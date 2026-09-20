package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestIndexDBPrunesUnrelatedParts(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{StreamFields: []string{"service", "host"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Date(2026, 3, 19, 19, 0, 0, 0, time.UTC)
	// Part 1: only api
	if err := s.Append(Entry{Time: now, Fields: map[string]string{"_msg": "api-log", "service": "api", "host": "h1"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	// Part 2: only worker
	if err := s.Append(Entry{Time: now, Fields: map[string]string{"_msg": "worker-log", "service": "worker", "host": "h2"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}

	idxPath := filepath.Join(dir, "partitions", "20260319", indexdbFileName)
	if _, err := os.Stat(idxPath); err != nil {
		t.Fatalf("indexdb missing: %v", err)
	}
	idx, err := readDayIndexDB(idxPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.ByTag["service=api"]) != 1 || idx.ByTag["service=api"][0] != "000001" {
		t.Fatalf("api tag → parts: %#v", idx.ByTag["service=api"])
	}
	if len(idx.ByTag["service=worker"]) != 1 || idx.ByTag["service=worker"][0] != "000002" {
		t.Fatalf("worker tag → parts: %#v", idx.ByTag["service=worker"])
	}

	rows, st, err := s.SearchWithStats(Query{
		StreamEq: map[string]string{"service": "api"},
		Contains: "api-log",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %#v", rows)
	}
	if st.PartsScanned != 1 {
		t.Fatalf("parts_scanned=%d want 1", st.PartsScanned)
	}
	if st.PartsPrunedIndexDB != 1 {
		t.Fatalf("parts_pruned_indexdb=%d want 1 (%+v)", st.PartsPrunedIndexDB, st)
	}
}

func TestIndexDBRebuildAfterMerge(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{
		StreamFields:        []string{"service"},
		MergeMinParts:       2,
		MergeMaxPartsPerJob: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Date(2026, 3, 19, 20, 0, 0, 0, time.UTC)
	for i := 0; i < 2; i++ {
		_ = s.Append(Entry{Time: now, Fields: map[string]string{"_msg": "x", "service": "api"}})
		_ = s.Flush()
	}
	if err := s.ForceMerge("20260319"); err != nil {
		t.Fatal(err)
	}

	idx, err := readDayIndexDB(filepath.Join(dir, "partitions", "20260319", indexdbFileName))
	if err != nil {
		t.Fatal(err)
	}
	parts := idx.ByTag["service=api"]
	if len(parts) != 1 {
		t.Fatalf("after merge want 1 part in indexdb, got %#v", parts)
	}
	s.mu.RLock()
	man := append([]string(nil), s.manifests["20260319"]...)
	s.mu.RUnlock()
	if parts[0] != man[0] {
		t.Fatalf("indexdb part %s != manifest %v", parts[0], man)
	}
}

func TestIndexDBReloadedOnOpen(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{StreamFields: []string{"service"}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 3, 19, 21, 0, 0, 0, time.UTC)
	_ = s.Append(Entry{Time: now, Fields: map[string]string{"_msg": "persist", "service": "api"}})
	_ = s.Flush()
	_ = s.Close()

	// Delete indexdb to force rebuild on Open.
	_ = os.Remove(filepath.Join(dir, "partitions", "20260319", indexdbFileName))

	s2, err := openTest(dir, Options{StreamFields: []string{"service"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, err := os.Stat(filepath.Join(dir, "partitions", "20260319", indexdbFileName)); err != nil {
		t.Fatalf("indexdb should be rebuilt: %v", err)
	}
	got, err := s2.Search(Query{StreamEq: map[string]string{"service": "api"}, Contains: "persist"})
	if err != nil || len(got) != 1 {
		t.Fatalf("got %#v err=%v", got, err)
	}
}
