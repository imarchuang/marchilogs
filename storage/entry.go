package storage

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Entry is one flat log record. All values are strings (VL-style schema-free model).
type Entry struct {
	Time   time.Time
	Fields map[string]string
}

const (
	FieldMsg    = "_msg"
	FieldTime   = "_time"
	FieldStream = "_stream"
)

// StreamID builds a stable stream identity from low-cardinality stream fields.
// Example: service=api,host=h1
func StreamID(streamFields []string, fields map[string]string) string {
	parts := make([]string, 0, len(streamFields))
	for _, k := range streamFields {
		v := fields[k]
		if v == "" {
			continue
		}
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	if len(parts) == 0 {
		return "_default"
	}
	return strings.Join(parts, ",")
}

func (e Entry) Msg() string {
	if e.Fields == nil {
		return ""
	}
	return e.Fields[FieldMsg]
}

func (e Entry) normalize(streamFields []string) Entry {
	out := Entry{
		Time:   e.Time,
		Fields: make(map[string]string, len(e.Fields)+2),
	}
	for k, v := range e.Fields {
		if k == "" || v == "" || k == FieldTime || k == FieldStream {
			continue
		}
		out.Fields[k] = v
	}
	if out.Time.IsZero() {
		out.Time = time.Now().UTC()
	} else {
		out.Time = out.Time.UTC()
	}
	if out.Fields[FieldMsg] == "" {
		out.Fields[FieldMsg] = "missing _msg"
	}
	out.Fields[FieldStream] = StreamID(streamFields, out.Fields)
	return out
}

func partitionName(t time.Time) string {
	return t.UTC().Format("20060102")
}

func parsePartitionName(name string) (time.Time, error) {
	t, err := time.ParseInLocation("20060102", name, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("bad partition %q: %w", name, err)
	}
	return t, nil
}
