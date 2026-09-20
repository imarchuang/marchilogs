package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCompactionMergesSmallPartsWithoutDupes(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{
		StreamFields:        []string{"service"},
		MergeMinParts:       4,
		MergeMaxPartsPerJob: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 4; i++ {
		msg := fmt.Sprintf("row-%d", i)
		if err := s.Append(Entry{
			Time:   now.Add(time.Duration(i) * time.Second),
			Fields: map[string]string{"_msg": msg, "service": "api"},
		}); err != nil {
			t.Fatal(err)
		}
		if err := s.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	s.mu.RLock()
	before := append([]string(nil), s.manifests["20260319"]...)
	s.mu.RUnlock()
	if len(before) != 4 {
		t.Fatalf("want 4 small parts before merge, got %v", before)
	}

	if err := s.runMergePass(); err != nil {
		t.Fatal(err)
	}

	s.mu.RLock()
	after := append([]string(nil), s.manifests["20260319"]...)
	s.mu.RUnlock()
	if len(after) != 1 {
		t.Fatalf("want 1 part after merge, got %v", after)
	}

	var meta partMeta
	if err := readJSON(filepath.Join(dir, "partitions", "20260319", "parts", after[0], "meta.json"), &meta); err != nil {
		t.Fatal(err)
	}
	if meta.Tier != partTierBig {
		t.Fatalf("tier=%q want big", meta.Tier)
	}

	got, err := s.Search(Query{Contains: "row-"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("want 4 rows after merge, got %d %#v", len(got), got)
	}

	// Input part dirs should be gone (orphan cleanup / explicit delete).
	for _, id := range before {
		p := filepath.Join(dir, "partitions", "20260319", "parts", id)
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("input part %s should be deleted, err=%v", id, err)
		}
	}
}

func TestCompactionSkipsWhenBelowMinParts(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{
		StreamFields:  []string{"service"},
		MergeMinParts: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Date(2026, 3, 19, 11, 0, 0, 0, time.UTC)
	for i := 0; i < 2; i++ {
		_ = s.Append(Entry{Time: now, Fields: map[string]string{"_msg": "x", "service": "api"}})
		_ = s.Flush()
	}
	if err := s.runMergePass(); err != nil {
		t.Fatal(err)
	}
	s.mu.RLock()
	ids := append([]string(nil), s.manifests["20260319"]...)
	s.mu.RUnlock()
	if len(ids) != 2 {
		t.Fatalf("want still 2 parts, got %v", ids)
	}
}

func TestForceMergeIgnoresMinParts(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{
		StreamFields:  []string{"service"},
		MergeMinParts: 8, // background would not merge 2 parts
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Date(2026, 3, 19, 14, 0, 0, 0, time.UTC)
	for i := 0; i < 2; i++ {
		_ = s.Append(Entry{Time: now, Fields: map[string]string{"_msg": fmt.Sprintf("f-%d", i), "service": "api"}})
		_ = s.Flush()
	}

	if err := s.ForceMerge("20260319"); err != nil {
		t.Fatal(err)
	}
	s.mu.RLock()
	ids := append([]string(nil), s.manifests["20260319"]...)
	s.mu.RUnlock()
	if len(ids) != 1 {
		t.Fatalf("force merge want 1 part, got %v", ids)
	}

	got, err := s.Search(Query{Contains: "f-"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 rows, got %#v", got)
	}
}

func TestForceMergeRejectsBadDay(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{StreamFields: []string{"service"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.ForceMerge("not-a-day"); err == nil {
		t.Fatal("want error for bad day")
	}
}
