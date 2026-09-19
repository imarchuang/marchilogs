package storage

import (
	"sort"
	"testing"
	"time"
)

// TestComplexStreamQueryScenarios covers mixed stream cardinality shapes:
//
//	host=h1,service=api1,cluster=infra
//	host=h2,service=api1,cluster=infra
//	host=h1,service=api2,cluster=default
//	host=h1,service=api1          (no cluster)
//	host=h1,service=api2          (no cluster)
func TestComplexStreamQueryScenarios(t *testing.T) {
	dir := t.TempDir()
	s, err := openTest(dir, Options{
		StreamFields:    []string{"host", "service", "cluster"},
		MaxRowsPerBlock: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Date(2026, 9, 19, 14, 0, 0, 0, time.UTC)

	type seed struct {
		msg    string
		fields map[string]string
		wantID string
	}
	seeds := []seed{
		{
			msg:    "msg-h1-api1-infra",
			fields: map[string]string{"_msg": "msg-h1-api1-infra", "host": "h1", "service": "api1", "cluster": "infra"},
			wantID: "cluster=infra,host=h1,service=api1",
		},
		{
			msg:    "msg-h2-api1-infra",
			fields: map[string]string{"_msg": "msg-h2-api1-infra", "host": "h2", "service": "api1", "cluster": "infra"},
			wantID: "cluster=infra,host=h2,service=api1",
		},
		{
			msg:    "msg-h1-api2-default",
			fields: map[string]string{"_msg": "msg-h1-api2-default", "host": "h1", "service": "api2", "cluster": "default"},
			wantID: "cluster=default,host=h1,service=api2",
		},
		{
			msg:    "msg-h1-api1-nocluster",
			fields: map[string]string{"_msg": "msg-h1-api1-nocluster", "host": "h1", "service": "api1"},
			wantID: "host=h1,service=api1",
		},
		{
			msg:    "msg-h1-api2-nocluster",
			fields: map[string]string{"_msg": "msg-h1-api2-nocluster", "host": "h1", "service": "api2"},
			wantID: "host=h1,service=api2",
		},
	}

	entries := make([]Entry, 0, len(seeds))
	for _, sd := range seeds {
		gotID := StreamID([]string{"host", "service", "cluster"}, sd.fields)
		if gotID != sd.wantID {
			t.Fatalf("stream id for %s: want %q, got %q", sd.msg, sd.wantID, gotID)
		}
		entries = append(entries, Entry{Time: now, Fields: sd.fields})
	}
	if err := s.Append(entries...); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name     string
		streamEq map[string]string
		contains string
		wantMsgs []string
	}{
		{
			name:     "no stream filter returns all",
			wantMsgs: []string{"msg-h1-api1-infra", "msg-h2-api1-infra", "msg-h1-api2-default", "msg-h1-api1-nocluster", "msg-h1-api2-nocluster"},
		},
		{
			name:     "service=api1 matches infra and no-cluster streams",
			streamEq: map[string]string{"service": "api1"},
			wantMsgs: []string{"msg-h1-api1-infra", "msg-h2-api1-infra", "msg-h1-api1-nocluster"},
		},
		{
			name:     "service=api2 matches default and no-cluster streams",
			streamEq: map[string]string{"service": "api2"},
			wantMsgs: []string{"msg-h1-api2-default", "msg-h1-api2-nocluster"},
		},
		{
			name:     "cluster=infra",
			streamEq: map[string]string{"cluster": "infra"},
			wantMsgs: []string{"msg-h1-api1-infra", "msg-h2-api1-infra"},
		},
		{
			name:     "cluster=default",
			streamEq: map[string]string{"cluster": "default"},
			wantMsgs: []string{"msg-h1-api2-default"},
		},
		{
			name:     "host=h1 across services and clusters",
			streamEq: map[string]string{"host": "h1"},
			wantMsgs: []string{"msg-h1-api1-infra", "msg-h1-api2-default", "msg-h1-api1-nocluster", "msg-h1-api2-nocluster"},
		},
		{
			name:     "host=h2 only infra api1",
			streamEq: map[string]string{"host": "h2"},
			wantMsgs: []string{"msg-h2-api1-infra"},
		},
		{
			name:     "host=h1 AND service=api1 (with and without cluster)",
			streamEq: map[string]string{"host": "h1", "service": "api1"},
			wantMsgs: []string{"msg-h1-api1-infra", "msg-h1-api1-nocluster"},
		},
		{
			name:     "host=h1 AND service=api2",
			streamEq: map[string]string{"host": "h1", "service": "api2"},
			wantMsgs: []string{"msg-h1-api2-default", "msg-h1-api2-nocluster"},
		},
		{
			name:     "full triple host+service+cluster",
			streamEq: map[string]string{"host": "h1", "service": "api1", "cluster": "infra"},
			wantMsgs: []string{"msg-h1-api1-infra"},
		},
		{
			name:     "service=api1 AND cluster=infra",
			streamEq: map[string]string{"service": "api1", "cluster": "infra"},
			wantMsgs: []string{"msg-h1-api1-infra", "msg-h2-api1-infra"},
		},
		{
			name:     "impossible: service=api1 AND cluster=default",
			streamEq: map[string]string{"service": "api1", "cluster": "default"},
			wantMsgs: nil,
		},
		{
			name:     "impossible: host=h2 AND service=api2",
			streamEq: map[string]string{"host": "h2", "service": "api2"},
			wantMsgs: nil,
		},
		{
			name:     "cluster=infra does not match streams without cluster tag",
			streamEq: map[string]string{"cluster": "infra", "host": "h1", "service": "api1"},
			wantMsgs: []string{"msg-h1-api1-infra"}, // not nocluster
		},
		{
			name:     "contains filter after stream prune",
			streamEq: map[string]string{"service": "api1"},
			contains: "nocluster",
			wantMsgs: []string{"msg-h1-api1-nocluster"},
		},
		{
			name:     "unknown service",
			streamEq: map[string]string{"service": "api9"},
			wantMsgs: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.Search(Query{
				StreamEq: tc.streamEq,
				Contains: tc.contains,
				Limit:    100,
			})
			if err != nil {
				t.Fatal(err)
			}
			gotMsgs := make([]string, 0, len(got))
			for _, e := range got {
				gotMsgs = append(gotMsgs, e.Msg())
				// every hit must satisfy StreamEq subset
				for k, v := range tc.streamEq {
					if e.Fields[k] != v {
						t.Fatalf("row %q missing tag %s=%s: fields=%v", e.Msg(), k, v, e.Fields)
					}
				}
			}
			sort.Strings(gotMsgs)
			want := append([]string(nil), tc.wantMsgs...)
			sort.Strings(want)
			if len(gotMsgs) != len(want) {
				t.Fatalf("want %v\ngot  %v", want, gotMsgs)
			}
			for i := range want {
				if gotMsgs[i] != want[i] {
					t.Fatalf("want %v\ngot  %v", want, gotMsgs)
				}
			}
		})
	}
}

func TestComplexStreamIndexByTag(t *testing.T) {
	ids := []string{
		"cluster=infra,host=h1,service=api1",
		"cluster=infra,host=h2,service=api1",
		"cluster=default,host=h1,service=api2",
		"host=h1,service=api1",
		"host=h1,service=api2",
	}
	meta := buildPartMetaFromIDs(ids, 1, 2)

	if got := matchStreamIDs(meta, map[string]string{"service": "api1"}); len(got) != 3 {
		t.Fatalf("service=api1: want 3 streams, got %#v", got)
	}
	if got := matchStreamIDs(meta, map[string]string{"cluster": "infra"}); len(got) != 2 {
		t.Fatalf("cluster=infra: want 2, got %#v", got)
	}
	if got := matchStreamIDs(meta, map[string]string{"host": "h1", "service": "api1"}); len(got) != 2 {
		t.Fatalf("h1+api1: want 2 (infra + no-cluster), got %#v", got)
	}
	if got := matchStreamIDs(meta, map[string]string{"host": "h1", "service": "api1", "cluster": "infra"}); len(got) != 1 {
		t.Fatalf("full triple: want 1, got %#v", got)
	}
	if _, ok := matchStreamIDs(meta, map[string]string{"cluster": "infra"})["host=h1,service=api1"]; ok {
		t.Fatal("no-cluster stream must not match cluster=infra")
	}
}
