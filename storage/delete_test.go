package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDeleteHidesMemAndDisk(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{
		StreamFields:    []string{"service", "host"},
		MaxRowsPerBlock: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	if err := s.Append(
		Entry{Time: now, Fields: map[string]string{"_msg": "keep me", "service": "api", "host": "h1"}},
		Entry{Time: now, Fields: map[string]string{"_msg": "drop secret", "service": "api", "host": "h1"}},
		Entry{Time: now, Fields: map[string]string{"_msg": "other stream", "service": "worker", "host": "h2"}},
	); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(Entry{Time: now.Add(time.Hour), Fields: map[string]string{
		"_msg": "mem secret", "service": "api", "host": "h1",
	}}); err != nil {
		t.Fatal(err)
	}

	purged, err := s.Delete(DeleteSpec{Contains: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if purged != 1 {
		t.Fatalf("purged_mem=%d want 1", purged)
	}

	got, st, err := s.SearchWithStats(Query{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 visible rows, got %#v", got)
	}
	for _, e := range got {
		if e.Msg() == "drop secret" || e.Msg() == "mem secret" {
			t.Fatalf("tombstoned row still visible: %q", e.Msg())
		}
	}
	if st.RowsSuppressedDelete < 1 {
		t.Fatalf("want RowsSuppressedDelete>=1, got %+v", st)
	}

	path := filepath.Join(dir, "partitions", "20260319", "deletes.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("deletes.json missing: %v", err)
	}
}

func TestDeletePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{StreamFields: []string{"service"}, MaxRowsPerBlock: 1000}
	s, err := openTest(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 3, 19, 12, 0, 0, 0, time.UTC)
	if err := s.Append(Entry{Time: now, Fields: map[string]string{"_msg": "gone", "service": "api"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Delete(DeleteSpec{StreamEq: map[string]string{"service": "api"}}); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	s2, err := openTest(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	got, err := s2.Search(Query{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty after reopen, got %#v", got)
	}
}

func TestDeleteRequiresPredicate(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{StreamFields: []string{"service"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.Delete(DeleteSpec{}); err == nil {
		t.Fatal("expected error for empty DeleteSpec")
	}
}

func TestDeleteStreamSubsetOnDisk(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{
		StreamFields:    []string{"service", "host"},
		MaxRowsPerBlock: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	if err := s.Append(
		Entry{Time: now, Fields: map[string]string{"_msg": "a", "service": "api", "host": "h1"}},
		Entry{Time: now, Fields: map[string]string{"_msg": "b", "service": "api", "host": "h2"}},
	); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Delete(DeleteSpec{StreamEq: map[string]string{"service": "api", "host": "h1"}}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Search(Query{StreamEq: map[string]string{"service": "api"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Msg() != "b" {
		t.Fatalf("want only h2 row, got %#v", got)
	}
}
