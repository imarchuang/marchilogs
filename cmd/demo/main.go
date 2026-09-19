package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/marchi/marchilogs/storage"
)

func main() {
	dir := filepath.Join(os.TempDir(), "marchilogs-demo")
	_ = os.RemoveAll(dir)

	s, err := storage.Open(dir, storage.Options{
		StreamFields:    []string{"service", "host"},
		MaxRowsPerBlock: 100,
	})
	if err != nil {
		panic(err)
	}
	defer s.Close()

	now := time.Now().UTC()
	_ = s.Append(
		storage.Entry{Time: now, Fields: map[string]string{"_msg": "boot ok", "service": "api", "host": "h1"}},
		storage.Entry{Time: now, Fields: map[string]string{"_msg": "request failed error", "service": "api", "host": "h1"}},
		storage.Entry{Time: now, Fields: map[string]string{"_msg": "worker tick", "service": "worker", "host": "h2"}},
	)
	_ = s.Flush()

	rows, err := s.Search(storage.Query{
		Start:    now.Add(-time.Hour),
		End:      now.Add(time.Hour),
		StreamEq: map[string]string{"service": "api", "host": "h1"},
		Contains: "error",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("data root:", dir)
	fmt.Println("hits:", len(rows))
	for _, r := range rows {
		fmt.Printf("  %s stream=%s msg=%s\n", r.Time.Format(time.RFC3339), r.Fields["_stream"], r.Msg())
	}
}
