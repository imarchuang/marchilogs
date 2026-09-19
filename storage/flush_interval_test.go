package storage

import (
	"path/filepath"
	"testing"
	"time"
)

func TestInmemoryDataFlushIntervalDefaultAndMin(t *testing.T) {
	o := (&Options{}).withDefaults()
	if o.InmemoryDataFlushInterval != 5*time.Second {
		t.Fatalf("default interval: got %v", o.InmemoryDataFlushInterval)
	}
	o = (&Options{InmemoryDataFlushInterval: 200 * time.Millisecond}).withDefaults()
	if o.InmemoryDataFlushInterval != time.Second {
		t.Fatalf("min 1s: got %v", o.InmemoryDataFlushInterval)
	}
	o = (&Options{InmemoryDataFlushInterval: -1}).withDefaults()
	if o.InmemoryDataFlushInterval != -1 {
		t.Fatalf("negative should stay disabled: got %v", o.InmemoryDataFlushInterval)
	}
}

func TestPeriodicFlushPublishesParts(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{
		StreamFields:              []string{"service"},
		MaxRowsPerBlock:           1000,
		InmemoryDataFlushInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Date(2026, 3, 19, 15, 0, 0, 0, time.UTC)
	if err := s.Append(Entry{
		Time:   now,
		Fields: map[string]string{"_msg": "tick-flush", "service": "api"},
	}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		matches, _ := filepath.Glob(filepath.Join(dir, "partitions", "20260319", "parts", "[0-9]*"))
		if len(matches) > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("expected periodic flush to publish a part within ~3s")
}
