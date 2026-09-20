package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBloomTrigramRejectsAbsentSubstring(t *testing.T) {
	bf := buildMsgBloom([]string{"hello world", "request ok"})
	if !bf.mightContainSubstring("hello") {
		t.Fatal("present substring must pass")
	}
	if bf.mightContainSubstring("zzzzz") {
		t.Fatal("absent substring should be rejected")
	}
	if !bf.mightContainSubstring("zz") {
		t.Fatal("short needle must not be rejected")
	}
}

func TestBloomSkipsBlockOnContainsMiss(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{StreamFields: []string{"service"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Date(2026, 3, 19, 16, 0, 0, 0, time.UTC)
	if err := s.Append(Entry{Time: now, Fields: map[string]string{"_msg": "alpha beta gamma", "service": "api"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}

	blockDir := filepath.Join(dir, "partitions", "20260319", "parts", "000001", "blocks", "service=api")
	if _, err := os.Stat(filepath.Join(blockDir, bloomFileName)); err != nil {
		t.Fatalf("expected bloom file: %v", err)
	}

	// Sabotage: remove columns so a full read would fail; bloom must skip the block.
	for _, name := range []string{"_msg.col", "_time.col", "service.col", "_stream.col"} {
		_ = os.Remove(filepath.Join(blockDir, name))
	}

	got, err := s.Search(Query{Contains: "nomatchxyz"})
	if err != nil {
		t.Fatalf("bloom skip should avoid read errors: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty, got %#v", got)
	}

	// Present substring still needs columns — restore and verify hit.
	b := newMemBlock("service=api")
	b.add(Entry{Time: now, Fields: map[string]string{"_msg": "alpha beta gamma", "service": "api", "_stream": "service=api"}})
	if err := b.writeTo(blockDir); err != nil {
		t.Fatal(err)
	}
	got, err = s.Search(Query{Contains: "beta"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want hit, got %#v", got)
	}
}

func TestBloomRoundTrip(t *testing.T) {
	bf := buildMsgBloom([]string{"compaction merge"})
	raw := bf.marshal()
	got, err := unmarshalBloom(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !got.mightContainSubstring("merge") {
		t.Fatal("round-trip lost membership")
	}
}
