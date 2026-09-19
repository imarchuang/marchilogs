package storage

import (
	"path/filepath"
	"testing"
	"time"
)

func TestAppendSearchPrune(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{
		StreamFields:    []string{"service", "host"},
		MaxRowsPerBlock: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	t1 := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 3, 19, 11, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 3, 20, 9, 0, 0, 0, time.UTC)

	err = s.Append(
		Entry{Time: t1, Fields: map[string]string{"_msg": "hello error", "service": "api", "host": "h1"}},
		Entry{Time: t2, Fields: map[string]string{"_msg": "ok", "service": "api", "host": "h1"}},
		Entry{Time: t2, Fields: map[string]string{"_msg": "error from worker", "service": "worker", "host": "h2"}},
		Entry{Time: t3, Fields: map[string]string{"_msg": "next day error", "service": "api", "host": "h1"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}

	// day layout exists
	if _, err := filepath.Glob(filepath.Join(dir, "partitions", "20260319", "parts", "*", "blocks", "*")); err != nil {
		t.Fatal(err)
	}

	// time partition prune: only Mar 19
	got, err := s.Search(Query{
		Start: t1,
		End:   t2,
		Limit: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 rows on Mar19 window, got %d", len(got))
	}

	// stream prune
	got, err = s.Search(Query{
		Start:    t1,
		End:      t3,
		StreamEq: map[string]string{"service": "api", "host": "h1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 api/h1 rows, got %d", len(got))
	}
	for _, e := range got {
		if e.Fields["service"] != "api" || e.Fields["host"] != "h1" {
			t.Fatalf("stream leak: %+v", e.Fields)
		}
	}

	// contains filter after unpack
	got, err = s.Search(Query{
		Start:    t1,
		End:      t3,
		Contains: "error",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 error rows, got %d: %#v", len(got), got)
	}
}

func TestStreamIDStable(t *testing.T) {
	a := StreamID([]string{"service", "host"}, map[string]string{"host": "h1", "service": "api"})
	b := StreamID([]string{"host", "service"}, map[string]string{"service": "api", "host": "h1"})
	if a != b || a != "host=h1,service=api" {
		t.Fatalf("got %q %q", a, b)
	}
}

func TestStreamSubsetMatch(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{StreamFields: []string{"service", "host"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Date(2026, 3, 19, 12, 0, 0, 0, time.UTC)
	if err := s.Append(
		Entry{Time: now, Fields: map[string]string{"_msg": "a1", "service": "api", "host": "h1"}},
		Entry{Time: now, Fields: map[string]string{"_msg": "a2", "service": "api", "host": "h2"}},
		Entry{Time: now, Fields: map[string]string{"_msg": "w1", "service": "worker", "host": "h1"}},
	); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}

	got, err := s.Search(Query{StreamEq: map[string]string{"service": "api"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("service=api: want 2, got %d %#v", len(got), got)
	}
	for _, e := range got {
		if e.Fields["service"] != "api" {
			t.Fatalf("leak: %+v", e.Fields)
		}
	}

	got, err = s.Search(Query{StreamEq: map[string]string{"service": "api", "host": "h1"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Msg() != "a1" {
		t.Fatalf("api+h1: want a1, got %#v", got)
	}
}

func TestMatchStreamIDsIntersect(t *testing.T) {
	meta := buildPartMeta([]string{
		"host=h1,service=api",
		"host=h2,service=api",
		"host=h1,service=worker",
	}, 1, 2)
	got := matchStreamIDs(meta, map[string]string{"service": "api"})
	if len(got) != 2 {
		t.Fatalf("want 2, got %#v", got)
	}
	got = matchStreamIDs(meta, map[string]string{"service": "api", "host": "h2"})
	if len(got) != 1 || got["host=h2,service=api"].ID == "" {
		t.Fatalf("want h2/api, got %#v", got)
	}
}
