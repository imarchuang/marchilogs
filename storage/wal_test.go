package storage

import (
	"testing"
	"time"
)

func TestWALSurvivesCrashWithoutFlush(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{
		EnableWAL:       true,
		StreamFields:    []string{"service", "host"},
		MaxRowsPerBlock: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 3, 19, 12, 0, 0, 0, time.UTC)
	if err := s.Append(Entry{
		Time:   now,
		Fields: map[string]string{"_msg": "durably buffered", "service": "api", "host": "h1"},
	}); err != nil {
		t.Fatal(err)
	}
	// Simulate crash: close WAL handle without Flush/Close flush path.
	s.mu.Lock()
	_ = s.wal.close()
	s.wal = nil
	s.buffers = nil // drop memory
	s.mu.Unlock()

	s2, err := Open(dir, Options{
		EnableWAL:       true,
		StreamFields:    []string{"service", "host"},
		MaxRowsPerBlock: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	got, err := s2.Search(Query{Contains: "durably"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Msg() != "durably buffered" {
		t.Fatalf("want WAL replay hit, got %#v", got)
	}
}

func TestWALCheckpointAvoidsDuplicateAfterFlush(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{
		EnableWAL:       true,
		StreamFields:    []string{"service"},
		MaxRowsPerBlock: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 3, 19, 13, 0, 0, 0, time.UTC)
	if err := s.Append(Entry{
		Time:   now,
		Fields: map[string]string{"_msg": "once", "service": "api"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir, Options{
		EnableWAL:       true,
		StreamFields:    []string{"service"},
		MaxRowsPerBlock: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	got, err := s2.Search(Query{Contains: "once"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want exactly 1 row after flush+reopen, got %#v", got)
	}
}

func TestWALPartialFlushCheckpoint(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{
		EnableWAL:       true,
		StreamFields:    []string{"service"},
		MaxRowsPerBlock: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}

	d1 := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	d2 := time.Date(2026, 3, 20, 10, 0, 0, 0, time.UTC)
	if err := s.Append(
		Entry{Time: d1, Fields: map[string]string{"_msg": "day1", "service": "api"}},
		Entry{Time: d2, Fields: map[string]string{"_msg": "day2", "service": "api"}},
	); err != nil {
		t.Fatal(err)
	}
	// Flush only day1 partition.
	s.mu.Lock()
	if err := s.flushPartitionLocked("20260319"); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	s.mu.Unlock()

	// Crash without flushing day2.
	s.mu.Lock()
	_ = s.wal.close()
	s.wal = nil
	s.buffers = make(map[string]*memBlock)
	s.mu.Unlock()

	s2, err := Open(dir, Options{EnableWAL: true, StreamFields: []string{"service"}, MaxRowsPerBlock: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	got, err := s2.Search(Query{})
	if err != nil {
		t.Fatal(err)
	}
	msgs := map[string]int{}
	for _, e := range got {
		msgs[e.Msg()]++
	}
	if msgs["day1"] != 1 || msgs["day2"] != 1 {
		t.Fatalf("want day1 from disk + day2 from WAL once each, got %v (%#v)", msgs, got)
	}
}

func TestWALDisabledByDefault(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{StreamFields: []string{"service"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Append(Entry{Fields: map[string]string{"_msg": "x", "service": "a"}}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	hasWAL := s.wal != nil
	s.mu.Unlock()
	if hasWAL {
		t.Fatal("expected nil wal by default")
	}
}
