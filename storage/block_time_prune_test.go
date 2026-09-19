package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFilterBlockRefsByTime(t *testing.T) {
	morning := time.Date(2026, 3, 19, 9, 0, 0, 0, time.UTC)
	noon := time.Date(2026, 3, 19, 12, 0, 0, 0, time.UTC)
	evening := time.Date(2026, 3, 19, 18, 0, 0, 0, time.UTC)

	refs := []blockRef{
		{Path: "blocks/a", TimeMinNS: morning.UnixNano(), TimeMaxNS: morning.Add(time.Hour).UnixNano()},
		{Path: "blocks/b", TimeMinNS: evening.UnixNano(), TimeMaxNS: evening.Add(time.Hour).UnixNano()},
	}

	got := filterBlockRefs(refs, noon, noon.Add(time.Hour))
	if len(got) != 0 {
		t.Fatalf("noon window should skip both, got %#v", got)
	}
	got = filterBlockRefs(refs, morning, morning.Add(2*time.Hour))
	if len(got) != 1 || got[0].Path != "blocks/a" {
		t.Fatalf("want only morning block, got %#v", got)
	}
	got = filterBlockRefs(refs, time.Time{}, time.Time{})
	if len(got) != 2 {
		t.Fatalf("unbounded should keep all, got %#v", got)
	}
}

func TestSearchPrunesBlockBeforeColumnRead(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{StreamFields: []string{"service", "host"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	morning := time.Date(2026, 3, 19, 9, 0, 0, 0, time.UTC)
	evening := time.Date(2026, 3, 19, 18, 0, 0, 0, time.UTC)

	if err := s.Append(
		Entry{Time: morning, Fields: map[string]string{"_msg": "am", "service": "api", "host": "h1"}},
		Entry{Time: evening, Fields: map[string]string{"_msg": "pm", "service": "api", "host": "h2"}},
	); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}

	partDir := filepath.Join(dir, "partitions", "20260319", "parts", "000001")
	metaPath := filepath.Join(partDir, "meta.json")
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	var meta partMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatal(err)
	}
	if len(meta.Streams) != 2 {
		t.Fatalf("want 2 streams in meta, got %#v", meta.Streams)
	}
	for _, sm := range meta.Streams {
		if len(sm.Blocks) != 1 {
			t.Fatalf("stream %s: want 1 block ref, got %#v", sm.ID, sm.Blocks)
		}
		if sm.Blocks[0].TimeMinNS == 0 || sm.Blocks[0].Path == "" {
			t.Fatalf("incomplete block ref: %#v", sm.Blocks[0])
		}
	}

	var eveningBlockDir string
	for _, sm := range meta.Streams {
		if sm.Tags["host"] == "h2" {
			eveningBlockDir = filepath.Join(partDir, sm.Blocks[0].Path)
			break
		}
	}
	if eveningBlockDir == "" {
		t.Fatal("evening stream block path not found in meta")
	}

	// Sabotage evening columnar files. If Search still opens this block for a
	// morning-only query, readBlock will fail — catching "read columns then filter" regressions.
	eveningTimeCol := filepath.Join(eveningBlockDir, "_time.col")
	if err := os.Remove(eveningTimeCol); err != nil {
		t.Fatalf("remove evening _time.col: %v", err)
	}
	// Also drop string columns so any partial open path fails loudly.
	entries, err := os.ReadDir(eveningBlockDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".col" {
			_ = os.Remove(filepath.Join(eveningBlockDir, e.Name()))
		}
	}

	// Control: evening window must fail because columns are gone.
	_, err = s.Search(Query{
		Start: evening,
		End:   evening.Add(time.Hour),
	})
	if err == nil {
		t.Fatal("expected evening query to fail after deleting evening .col files")
	}

	// Morning window must still succeed — proves evening block was never opened.
	got, err := s.Search(Query{
		Start: morning,
		End:   morning.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("morning query must not touch evening columns: %v", err)
	}
	if len(got) != 1 || got[0].Msg() != "am" {
		t.Fatalf("want only am, got %#v", got)
	}
}
