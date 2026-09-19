package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSearchSeesInmemoryWithoutFlush(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{
		StreamFields:    []string{"service", "host"},
		MaxRowsPerBlock: 1000, // avoid auto-flush
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	if err := s.Append(
		Entry{Time: now, Fields: map[string]string{"_msg": "hot path", "service": "api", "host": "h1"}},
		Entry{Time: now, Fields: map[string]string{"_msg": "other", "service": "worker", "host": "h2"}},
	); err != nil {
		t.Fatal(err)
	}

	// No Flush: nothing should be published on disk yet.
	partsDir := filepath.Join(dir, "partitions", "20260319", "parts")
	if entries, err := os.ReadDir(partsDir); err == nil && len(entries) > 0 {
		t.Fatalf("expected no published parts before Flush, got %v", entries)
	}

	got, err := s.Search(Query{
		StreamEq: map[string]string{"service": "api"},
		Contains: "hot",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Msg() != "hot path" {
		t.Fatalf("want inmemory hit, got %#v", got)
	}

	// Still no disk parts after Search.
	if entries, err := os.ReadDir(partsDir); err == nil && len(entries) > 0 {
		for _, e := range entries {
			if isPublishedPartDir(e.Name()) {
				t.Fatalf("Search must not flush; found published part %s", e.Name())
			}
		}
	}
}

func TestSearchMergesInmemoryAndDisk(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{
		StreamFields:    []string{"service", "host"},
		MaxRowsPerBlock: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	tDisk := time.Date(2026, 3, 19, 9, 0, 0, 0, time.UTC)
	tMem := time.Date(2026, 3, 19, 11, 0, 0, 0, time.UTC)

	if err := s.Append(Entry{Time: tDisk, Fields: map[string]string{"_msg": "from-disk", "service": "api", "host": "h1"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(Entry{Time: tMem, Fields: map[string]string{"_msg": "from-mem", "service": "api", "host": "h1"}}); err != nil {
		t.Fatal(err)
	}

	got, err := s.Search(Query{StreamEq: map[string]string{"service": "api", "host": "h1"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want disk+mem rows, got %#v", got)
	}
	msgs := map[string]bool{}
	for _, e := range got {
		msgs[e.Msg()] = true
	}
	if !msgs["from-disk"] || !msgs["from-mem"] {
		t.Fatalf("missing rows: %#v", got)
	}
}

func TestInmemoryStreamSubsetAndTimePrune(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{
		StreamFields:    []string{"service", "host", "cluster"},
		MaxRowsPerBlock: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	morning := time.Date(2026, 3, 19, 9, 0, 0, 0, time.UTC)
	evening := time.Date(2026, 3, 19, 18, 0, 0, 0, time.UTC)
	if err := s.Append(
		Entry{Time: morning, Fields: map[string]string{"_msg": "am-api1", "service": "api1", "host": "h1", "cluster": "infra"}},
		Entry{Time: evening, Fields: map[string]string{"_msg": "pm-api1", "service": "api1", "host": "h2", "cluster": "infra"}},
		Entry{Time: morning, Fields: map[string]string{"_msg": "am-api2", "service": "api2", "host": "h1"}},
	); err != nil {
		t.Fatal(err)
	}

	got, err := s.Search(Query{
		Start:    morning,
		End:      morning.Add(time.Hour),
		StreamEq: map[string]string{"service": "api1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Msg() != "am-api1" {
		t.Fatalf("want only morning api1 from mem, got %#v", got)
	}
}
