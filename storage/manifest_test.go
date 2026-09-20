package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestManifestMakesPartsSearchable(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{StreamFields: []string{"service"}})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	if err := s.Append(Entry{Time: now, Fields: map[string]string{"_msg": "via-manifest", "service": "api"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}

	manPath := filepath.Join(dir, "partitions", "20260319", "manifest.json")
	m, err := readDayManifest(manPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Parts) != 1 || m.Parts[0] != "000001" {
		t.Fatalf("manifest: %#v", m)
	}

	// Part on disk but removed from manifest must not be searchable.
	if err := writeDayManifestAtomic(manPath, dayManifest{Parts: nil}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.manifests["20260319"] = nil
	s.mu.Unlock()

	got, err := s.Search(Query{Contains: "via-manifest"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("empty manifest must hide parts, got %#v", got)
	}
	_ = s.Close()
}

func TestOpenRebuildsManifestAndDropsOrphans(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{StreamFields: []string{"service"}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 3, 19, 11, 0, 0, 0, time.UTC)
	if err := s.Append(Entry{Time: now, Fields: map[string]string{"_msg": "kept", "service": "api"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Plant orphan part not in manifest.
	orphan := filepath.Join(dir, "partitions", "20260319", "parts", "000099")
	if err := os.MkdirAll(filepath.Join(orphan, "blocks"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(orphan, "meta.json"), buildPartMetaFromIDs([]string{"host=x,service=y"}, 1, 2)); err != nil {
		t.Fatal(err)
	}

	s2, err := openTest(dir, Options{StreamFields: []string{"service"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan part should be removed on Open, err=%v", err)
	}
	got, err := s2.Search(Query{Contains: "kept"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want kept row, got %#v", got)
	}
}

func TestFlushWritesSmallTier(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{StreamFields: []string{"service"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Date(2026, 3, 19, 12, 0, 0, 0, time.UTC)
	_ = s.Append(Entry{Time: now, Fields: map[string]string{"_msg": "t", "service": "api"}})
	_ = s.Flush()

	var meta partMeta
	if err := readJSON(filepath.Join(dir, "partitions", "20260319", "parts", "000001", "meta.json"), &meta); err != nil {
		t.Fatal(err)
	}
	if meta.Tier != partTierSmall {
		t.Fatalf("tier=%q want small", meta.Tier)
	}
}
