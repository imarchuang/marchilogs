package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/marchi/marchilogs/storage"
)

func main() {
	addr := flag.String("addr", envOr("MARCHILOGS_ADDR", ":8080"), "listen address")
	dataDir := flag.String("storageDataPath", envOr("MARCHILOGS_DATA", "/data"), "storage root directory")
	streamFields := flag.String("streamFields", envOr("MARCHILOGS_STREAM_FIELDS", "service,host"), "comma-separated stream fields")
	maxRows := flag.Int("maxRowsPerBlock", 1024, "flush a stream block after this many rows")
	flushInterval := flag.Duration("inmemoryDataFlushInterval",
		envDurationOr("MARCHILOGS_INMEMORY_DATA_FLUSH_INTERVAL", 5*time.Second),
		"interval for guaranteed flush of in-memory data to disk (VictoriaLogs-style); min 1s; 0 disables")
	mergeCheck := flag.Duration("mergeCheckInterval",
		envDurationOr("MARCHILOGS_MERGE_CHECK_INTERVAL", 10*time.Second),
		"how often to look for small-part compaction; 0 disables background merge")
	mergeMinParts := flag.Int("mergeMinParts", envIntOr("MARCHILOGS_MERGE_MIN_PARTS", 4),
		"start merge when a day has at least this many small parts")
	flag.Parse()

	fields := splitCSV(*streamFields)
	interval := *flushInterval
	if interval == 0 {
		interval = -1 // disable periodic flush in storage
	}
	mergeEvery := *mergeCheck
	if mergeEvery == 0 {
		mergeEvery = -1
	}
	store, err := storage.Open(*dataDir, storage.Options{
		StreamFields:              fields,
		MaxRowsPerBlock:           *maxRows,
		InmemoryDataFlushInterval: interval,
		MergeCheckInterval:        mergeEvery,
		MergeMinParts:             *mergeMinParts,
	})
	if err != nil {
		log.Fatalf("open storage: %v", err)
	}
	defer store.Close()

	if interval > 0 {
		log.Printf("inmemory data flush interval %s (min enforced at 1s)", interval)
	}
	if mergeEvery > 0 {
		log.Printf("merge check every %s (minParts=%d)", mergeEvery, *mergeMinParts)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/insert", handleInsert(store))
	mux.HandleFunc("/flush", handleFlush(store))
	mux.HandleFunc("/query", handleQuery(store, fields))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "marchilogs\n\n"+
			"POST /insert           — Append only (queryable from memory)\n"+
			"POST /insert?flush=1   — Append then flush to disk\n"+
			"POST /flush            — Flush buffered rows to disk\n"+
			"GET  /query            — start,end,contains,limit + stream field equals\n"+
			"GET  /healthz\n"+
			"\nDurability: in-memory buffers flush to disk every -inmemoryDataFlushInterval (default 5s).\n"+
			"Compaction: small parts merge into big when count ≥ -mergeMinParts (default 4).\n")
	})

	srv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		log.Printf("marchilogs listening on %s (data=%s streamFields=%v)", *addr, *dataDir, fields)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Printf("shutting down…")
	_ = store.Flush()
	_ = srv.Close()
}

func handleInsert(store *storage.Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		entries, err := decodeEntries(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(entries) == 0 {
			http.Error(w, "no entries", http.StatusBadRequest)
			return
		}
		if err := store.Append(entries...); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		flushed := false
		if wantFlush(r) {
			if err := store.Flush(); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			flushed = true
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"inserted": len(entries),
			"flushed":  flushed,
		})
	}
}

func handleFlush(store *storage.Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		if err := store.Flush(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"flushed": true})
	}
}

func wantFlush(r *http.Request) bool {
	v := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("flush")))
	switch v {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func handleQuery(store *storage.Storage, streamFields []string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			http.Error(w, "GET or POST", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		start, err := parseTimeParam(q.Get("start"))
		if err != nil {
			http.Error(w, "bad start: "+err.Error(), http.StatusBadRequest)
			return
		}
		end, err := parseTimeParam(q.Get("end"))
		if err != nil {
			http.Error(w, "bad end: "+err.Error(), http.StatusBadRequest)
			return
		}
		limit := 100
		if v := q.Get("limit"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				http.Error(w, "bad limit", http.StatusBadRequest)
				return
			}
			limit = n
		}
		streamEq := map[string]string{}
		for _, f := range streamFields {
			if v := q.Get(f); v != "" {
				streamEq[f] = v
			}
		}
		rows, err := store.Search(storage.Query{
			Start:    start,
			End:      end,
			StreamEq: streamEq,
			Contains: q.Get("contains"),
			Limit:    limit,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		enc := json.NewEncoder(w)
		for _, e := range rows {
			obj := map[string]string{}
			for k, v := range e.Fields {
				obj[k] = v
			}
			obj["_time"] = e.Time.UTC().Format(time.RFC3339Nano)
			if err := enc.Encode(obj); err != nil {
				return
			}
		}
	}
}

func decodeEntries(r io.Reader) ([]storage.Entry, error) {
	br := bufio.NewReader(r)
	b, err := br.Peek(1)
	if err != nil {
		return nil, err
	}
	if b[0] == '[' {
		var raw []map[string]any
		if err := json.NewDecoder(br).Decode(&raw); err != nil {
			return nil, fmt.Errorf("json array: %w", err)
		}
		out := make([]storage.Entry, 0, len(raw))
		for _, m := range raw {
			e, err := mapToEntry(m)
			if err != nil {
				return nil, err
			}
			out = append(out, e)
		}
		return out, nil
	}
	var out []storage.Entry
	dec := json.NewDecoder(br)
	for {
		var m map[string]any
		err := dec.Decode(&m)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("json: %w", err)
		}
		e, err := mapToEntry(m)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

func mapToEntry(m map[string]any) (storage.Entry, error) {
	fields := make(map[string]string, len(m))
	var t time.Time
	for k, v := range m {
		if k == "_time" {
			switch x := v.(type) {
			case string:
				parsed, err := parseTimeParam(x)
				if err != nil {
					return storage.Entry{}, fmt.Errorf("_time: %w", err)
				}
				t = parsed
			case float64:
				t = time.Unix(int64(x), 0).UTC()
			default:
				return storage.Entry{}, fmt.Errorf("unsupported _time type %T", v)
			}
			continue
		}
		fields[k] = fmt.Sprint(v)
	}
	return storage.Entry{Time: t, Fields: fields}, nil
}

func parseTimeParam(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC(), nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		if n < 1e12 {
			return time.Unix(n, 0).UTC(), nil
		}
		return time.Unix(0, n).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("unsupported time %q", s)
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDurationOr(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func envIntOr(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
