package storage

import (
	"testing"
	"time"
)

func TestSearchWithStatsCountsPartsBlocksRows(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{StreamFields: []string{"service"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Date(2026, 3, 19, 17, 0, 0, 0, time.UTC)
	// Two flushes → two parts, one stream each time.
	for i := 0; i < 2; i++ {
		if err := s.Append(Entry{
			Time:   now,
			Fields: map[string]string{"_msg": "stats-row hello", "service": "api"},
		}); err != nil {
			t.Fatal(err)
		}
		if err := s.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	rows, st, err := s.SearchWithStats(Query{Contains: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || st.RowsReturned != 2 {
		t.Fatalf("rows=%d st=%+v", len(rows), st)
	}
	if st.PartsScanned != 2 {
		t.Fatalf("parts_scanned=%d want 2", st.PartsScanned)
	}
	if st.BlocksSeen != 2 || st.BlocksScanned != 2 {
		t.Fatalf("blocks seen/scanned=%d/%d want 2/2", st.BlocksSeen, st.BlocksScanned)
	}
	if st.RowsScanned < 2 {
		t.Fatalf("rows_scanned=%d want ≥2", st.RowsScanned)
	}

	// Bloom should skip both blocks for an impossible needle.
	_, st2, err := s.SearchWithStats(Query{Contains: "nomatchxyz"})
	if err != nil {
		t.Fatal(err)
	}
	if st2.BlocksSkippedBloom != 2 {
		t.Fatalf("blocks_skipped_bloom=%d want 2 (%+v)", st2.BlocksSkippedBloom, st2)
	}
	if st2.BlocksScanned != 0 {
		t.Fatalf("blocks_scanned=%d want 0 after bloom skip", st2.BlocksScanned)
	}
}

func TestSearchWithStatsCountsMemBlocks(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{StreamFields: []string{"service"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Date(2026, 3, 19, 18, 0, 0, 0, time.UTC)
	if err := s.Append(Entry{Time: now, Fields: map[string]string{"_msg": "only-mem", "service": "api"}}); err != nil {
		t.Fatal(err)
	}
	rows, st, err := s.SearchWithStats(Query{Contains: "only-mem"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %#v", rows)
	}
	if st.MemBlocksScanned != 1 || st.PartsScanned != 0 {
		t.Fatalf("want mem-only scan, got %+v", st)
	}
}
