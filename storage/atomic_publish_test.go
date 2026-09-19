package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFlushAtomicPublishIgnoresInProgressDirs(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{StreamFields: []string{"service", "host"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	if err := s.Append(Entry{
		Time:   now,
		Fields: map[string]string{"_msg": "published", "service": "api", "host": "h1"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}

	partsDir := filepath.Join(dir, "partitions", "20260319", "parts")
	entries, err := os.ReadDir(partsDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), publishingPrefix) {
			t.Fatalf("flush left publishing dir visible: %s", e.Name())
		}
		if !isPublishedPartDir(e.Name()) {
			t.Fatalf("unexpected part dir %q", e.Name())
		}
	}

	// Plant an incomplete in-progress part that would look searchable if readers weren't careful.
	orphan := filepath.Join(partsDir, publishingPrefix+"000099")
	blockDir := filepath.Join(orphan, "blocks", "host=h9,service=evil")
	if err := os.MkdirAll(blockDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(orphan, "meta.json"), buildPartMetaFromIDs([]string{"host=h9,service=evil"}, 1, 2)); err != nil {
		t.Fatal(err)
	}
	// Minimal fake columns so a naive reader could decode if it opened this dir.
	b := newMemBlock("host=h9,service=evil")
	b.add(Entry{Time: now, Fields: map[string]string{"_msg": "orphan-leak", "service": "evil", "host": "h9", "_stream": "host=h9,service=evil"}})
	if err := b.writeTo(blockDir); err != nil {
		t.Fatal(err)
	}

	got, err := s.Search(Query{Contains: "orphan"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("in-progress part must be invisible to Search, got %#v", got)
	}

	got, err = s.Search(Query{Contains: "published"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Msg() != "published" {
		t.Fatalf("want published row, got %#v", got)
	}
}

func TestOpenCleansOrphanPublishingDirs(t *testing.T) {
	dir := t.TempDir()
	partsDir := filepath.Join(dir, "partitions", "20260319", "parts")
	orphan := filepath.Join(partsDir, publishingPrefix+"000007")
	if err := os.MkdirAll(filepath.Join(orphan, "blocks"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(orphan, "meta.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("Open should remove orphan publishing dir, stat err=%v", err)
	}
}

func TestFlushKeepsBuffersIfPublishFails(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{StreamFields: []string{"service"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Date(2026, 3, 19, 11, 0, 0, 0, time.UTC)
	if err := s.Append(Entry{Time: now, Fields: map[string]string{"_msg": "retry-me", "service": "api"}}); err != nil {
		t.Fatal(err)
	}

	// Occupy the final destination so rename fails after tmp write.
	partsParent := filepath.Join(dir, "partitions", "20260319", "parts")
	if err := os.MkdirAll(partsParent, 0o755); err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(partsParent, "000001")
	if err := os.WriteFile(blocker, []byte("not-a-dir"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := s.Flush(); err == nil {
		t.Fatal("expected Flush to fail when final path is blocked")
	}

	// Buffer must still hold the row so a later Flush can succeed.
	_ = os.Remove(blocker)
	if err := s.Flush(); err != nil {
		t.Fatalf("retry Flush: %v", err)
	}
	got, err := s.Search(Query{Contains: "retry-me"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 row after retry, got %#v", got)
	}
}
