package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRetentionDropsOldDayPartitions(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{
		StreamFields:    []string{"service"},
		RetentionPeriod: 48 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	old := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	keep := time.Date(2026, 3, 19, 12, 0, 0, 0, time.UTC)
	if err := s.Append(Entry{Time: old, Fields: map[string]string{"_msg": "old-row", "service": "api"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(Entry{Time: keep, Fields: map[string]string{"_msg": "keep-row", "service": "api"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}

	// Freeze "now" via applyRetentionAt so 48h from Mar 20 keeps Mar 19, drops Mar 10.
	now := time.Date(2026, 3, 20, 15, 0, 0, 0, time.UTC)
	dropped, err := s.applyRetentionAt(now)
	if err != nil {
		t.Fatal(err)
	}
	if dropped != 1 {
		t.Fatalf("dropped=%d want 1", dropped)
	}

	oldDir := filepath.Join(dir, "partitions", "20260310")
	if _, err := os.Stat(oldDir); !os.IsNotExist(err) {
		t.Fatalf("old day dir should be gone, err=%v", err)
	}
	keepDir := filepath.Join(dir, "partitions", "20260319")
	if _, err := os.Stat(keepDir); err != nil {
		t.Fatalf("keep day dir: %v", err)
	}

	got, err := s.Search(Query{Contains: "old-row"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("old rows should be gone, got %#v", got)
	}
	got, err = s.Search(Query{Contains: "keep-row"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want keep-row, got %#v", got)
	}
}

func TestRetentionDisabledWhenPeriodZero(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{
		StreamFields:    []string{"service"},
		RetentionPeriod: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	day := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = s.Append(Entry{Time: day, Fields: map[string]string{"_msg": "x", "service": "api"}})
	_ = s.Flush()

	n, err := s.applyRetentionAt(time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("disabled retention should drop 0, got %d", n)
	}
}
