package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marchi/marchilogs/storage"
)

func TestInsertDefaultDoesNotFlush(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.Open(dir, storage.Options{
		StreamFields:              []string{"service", "host"},
		MaxRowsPerBlock:           1000,
		InmemoryDataFlushInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/insert", handleInsert(store))
	mux.HandleFunc("/flush", handleFlush(store))
	mux.HandleFunc("/query", handleQuery(store, []string{"service", "host"}))

	body := `{"_msg":"buffered","service":"api","host":"h1"}`
	req := httptest.NewRequest(http.MethodPost, "/insert", strings.NewReader(body))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("insert status=%d body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["flushed"] != false {
		t.Fatalf("default insert must not flush: %#v", resp)
	}

	qreq := httptest.NewRequest(http.MethodGet, "/query?service=api&contains=buffered", nil)
	qrr := httptest.NewRecorder()
	mux.ServeHTTP(qrr, qreq)
	if qrr.Code != http.StatusOK {
		t.Fatalf("query status=%d body=%s", qrr.Code, qrr.Body.String())
	}
	if !strings.Contains(qrr.Body.String(), "buffered") {
		t.Fatalf("expected inmemory hit, got %q", qrr.Body.String())
	}

	if n := countPublishedParts(dir); n != 0 {
		t.Fatalf("expected 0 published parts after append-only insert, got %d", n)
	}
}

func TestInsertFlushQueryParamAndEndpoint(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.Open(dir, storage.Options{
		StreamFields:              []string{"service"},
		MaxRowsPerBlock:           1000,
		InmemoryDataFlushInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/insert", handleInsert(store))
	mux.HandleFunc("/flush", handleFlush(store))

	now := time.Now().UTC().Format(time.RFC3339Nano)
	body := `{"_msg":"persist-me","service":"api","_time":"` + now + `"}`
	req := httptest.NewRequest(http.MethodPost, "/insert?flush=1", strings.NewReader(body))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("insert?flush=1 status=%d body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp["flushed"] != true {
		t.Fatalf("want flushed=true, got %#v", resp)
	}
	if n := countPublishedParts(dir); n < 1 {
		t.Fatal("expected a published part after insert?flush=1")
	}

	req2 := httptest.NewRequest(http.MethodPost, "/insert", strings.NewReader(`{"_msg":"later","service":"api"}`))
	rr2 := httptest.NewRecorder()
	mux.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatal(rr2.Body.String())
	}
	before := countPublishedParts(dir)

	freq := httptest.NewRequest(http.MethodPost, "/flush", nil)
	frr := httptest.NewRecorder()
	mux.ServeHTTP(frr, freq)
	if frr.Code != http.StatusOK {
		t.Fatalf("flush status=%d body=%s", frr.Code, frr.Body.String())
	}
	if after := countPublishedParts(dir); after <= before {
		t.Fatalf("POST /flush should publish buffered part: before=%d after=%d", before, after)
	}
}

func TestWantFlush(t *testing.T) {
	cases := map[string]bool{
		"":      false,
		"0":     false,
		"false": false,
		"1":     true,
		"true":  true,
		"yes":   true,
		"on":    true,
	}
	for v, want := range cases {
		req := httptest.NewRequest(http.MethodPost, "/insert?flush="+v, nil)
		if got := wantFlush(req); got != want {
			t.Fatalf("flush=%q: want %v got %v", v, want, got)
		}
	}
}

func countPublishedParts(root string) int {
	n := 0
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || !info.IsDir() {
			return nil
		}
		// only count .../parts/<id>
		if filepath.Base(filepath.Dir(path)) != "parts" {
			return nil
		}
		name := info.Name()
		if strings.HasPrefix(name, ".") {
			return nil
		}
		if _, err := strconv.Atoi(name); err == nil {
			n++
		}
		return nil
	})
	return n
}
