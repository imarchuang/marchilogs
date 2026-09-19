package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// blockMeta is stored beside columnar files for one stream's block.
type blockMeta struct {
	Stream    string   `json:"stream"`
	Rows      int      `json:"rows"`
	TimeMinNS int64    `json:"time_min_ns"`
	TimeMaxNS int64    `json:"time_max_ns"`
	Fields    []string `json:"fields"`
}

// memBlock accumulates rows for one stream until flush.
type memBlock struct {
	stream  string
	times   []int64
	fields  map[string][]string // columnar in-memory
	timeMin int64
	timeMax int64
}

func newMemBlock(stream string) *memBlock {
	return &memBlock{
		stream:  stream,
		fields:  make(map[string][]string),
		timeMin: 0,
		timeMax: 0,
	}
}

func (b *memBlock) rows() int { return len(b.times) }

func (b *memBlock) add(e Entry) {
	ts := e.Time.UnixNano()
	b.times = append(b.times, ts)
	if b.rows() == 1 {
		b.timeMin, b.timeMax = ts, ts
	} else {
		if ts < b.timeMin {
			b.timeMin = ts
		}
		if ts > b.timeMax {
			b.timeMax = ts
		}
	}
	n := len(b.times)
	for k, v := range e.Fields {
		col := b.fields[k]
		if col == nil {
			col = make([]string, n-1) // pad missing past rows
		}
		for len(col) < n-1 {
			col = append(col, "")
		}
		col = append(col, v)
		b.fields[k] = col
	}
	// pad fields that were missing on this row
	for k, col := range b.fields {
		if len(col) < n {
			b.fields[k] = append(col, "")
		}
	}
}

func (b *memBlock) overlaps(start, end time.Time) bool {
	if b.rows() == 0 {
		return false
	}
	return timeRangeOverlaps(start, end, b.timeMin, b.timeMax)
}

func writeStringColumn(path string, values []string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(len(values)))
	if _, err := f.Write(buf[:]); err != nil {
		return err
	}
	for _, v := range values {
		b := []byte(v)
		binary.LittleEndian.PutUint32(buf[:], uint32(len(b)))
		if _, err := f.Write(buf[:]); err != nil {
			return err
		}
		if _, err := f.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func readStringColumn(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var buf [4]byte
	if _, err := io.ReadFull(f, buf[:]); err != nil {
		return nil, err
	}
	n := int(binary.LittleEndian.Uint32(buf[:]))
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		if _, err := io.ReadFull(f, buf[:]); err != nil {
			return nil, err
		}
		ln := int(binary.LittleEndian.Uint32(buf[:]))
		b := make([]byte, ln)
		if _, err := io.ReadFull(f, b); err != nil {
			return nil, err
		}
		out = append(out, string(b))
	}
	return out, nil
}

func writeTimeColumn(path string, times []int64) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(times)))
	if _, err := f.Write(hdr[:]); err != nil {
		return err
	}
	var buf [8]byte
	for _, t := range times {
		binary.LittleEndian.PutUint64(buf[:], uint64(t))
		if _, err := f.Write(buf[:]); err != nil {
			return err
		}
	}
	return nil
}

func readTimeColumn(path string) ([]int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var hdr [4]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return nil, err
	}
	n := int(binary.LittleEndian.Uint32(hdr[:]))
	out := make([]int64, n)
	var buf [8]byte
	for i := 0; i < n; i++ {
		if _, err := io.ReadFull(f, buf[:]); err != nil {
			return nil, err
		}
		out[i] = int64(binary.LittleEndian.Uint64(buf[:]))
	}
	return out, nil
}

func safeStreamDir(stream string) string {
	// filesystem-safe: replace path separators
	b := make([]byte, 0, len(stream))
	for i := 0; i < len(stream); i++ {
		c := stream[i]
		switch c {
		case '/', '\\', ':', '<', '>', '|', '?', '*', '"':
			b = append(b, '_')
		default:
			b = append(b, c)
		}
	}
	if len(b) == 0 {
		return "_default"
	}
	return string(b)
}

func (b *memBlock) writeTo(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	fieldNames := make([]string, 0, len(b.fields))
	for k := range b.fields {
		fieldNames = append(fieldNames, k)
	}
	// stable order not required for correctness
	meta := blockMeta{
		Stream:    b.stream,
		Rows:      b.rows(),
		TimeMinNS: b.timeMin,
		TimeMaxNS: b.timeMax,
		Fields:    fieldNames,
	}
	if err := writeJSON(filepath.Join(dir, "meta.json"), meta); err != nil {
		return err
	}
	if err := writeTimeColumn(filepath.Join(dir, "_time.col"), b.times); err != nil {
		return err
	}
	for k, col := range b.fields {
		name := fieldFileName(k)
		if err := writeStringColumn(filepath.Join(dir, name), col); err != nil {
			return fmt.Errorf("write field %s: %w", k, err)
		}
	}
	return nil
}

func fieldFileName(field string) string {
	return safeStreamDir(field) + ".col"
}

// readBlockMeta loads only the block's meta.json (no columnar files).
func readBlockMeta(dir string) (blockMeta, error) {
	var meta blockMeta
	err := readJSON(filepath.Join(dir, "meta.json"), &meta)
	return meta, err
}

func readBlock(dir string) (*memBlock, error) {
	meta, err := readBlockMeta(dir)
	if err != nil {
		return nil, err
	}
	times, err := readTimeColumn(filepath.Join(dir, "_time.col"))
	if err != nil {
		return nil, err
	}
	b := &memBlock{
		stream:  meta.Stream,
		times:   times,
		fields:  make(map[string][]string, len(meta.Fields)),
		timeMin: meta.TimeMinNS,
		timeMax: meta.TimeMaxNS,
	}
	for _, k := range meta.Fields {
		col, err := readStringColumn(filepath.Join(dir, fieldFileName(k)))
		if err != nil {
			return nil, fmt.Errorf("read field %s: %w", k, err)
		}
		b.fields[k] = col
	}
	return b, nil
}

func (b *memBlock) row(i int) Entry {
	fields := make(map[string]string, len(b.fields))
	for k, col := range b.fields {
		if i < len(col) && col[i] != "" {
			fields[k] = col[i]
		}
	}
	return Entry{
		Time:   time.Unix(0, b.times[i]).UTC(),
		Fields: fields,
	}
}
