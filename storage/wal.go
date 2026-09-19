package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	walMagic      = uint32(0x4D4C5741) // MLWA
	walVersion    = uint8(1)
	walFileName   = "wal.log"
	walCheckpoint = "checkpoint"
	walDirName    = "wal"
)

// walRecord is one durable log line before it lands in a published part.
type walRecord struct {
	Seq    uint64            `json:"seq"`
	TimeNS int64             `json:"time_ns"`
	Fields map[string]string `json:"fields"`
}

type wal struct {
	dir        string
	f          *os.File
	nextSeq    uint64 // next seq to assign (1-based records)
	checkpoint uint64 // records with seq <= checkpoint are durable in parts (skip on replay)
	syncWrite  bool
}

func openWAL(root string, syncWrite bool) (*wal, error) {
	dir := filepath.Join(root, walDirName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &wal{dir: dir, syncWrite: syncWrite, nextSeq: 1}
	cp, err := readCheckpoint(filepath.Join(dir, walCheckpoint))
	if err != nil {
		return nil, err
	}
	w.checkpoint = cp

	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	w.f = f

	maxSeq, err := w.scanMaxSeq()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if maxSeq >= w.nextSeq {
		w.nextSeq = maxSeq + 1
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, err
	}
	return w, nil
}

func (w *wal) close() error {
	if w == nil || w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

func readCheckpoint(path string) (uint64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if len(b) == 0 {
		return 0, nil
	}
	var cp uint64
	if _, err := fmt.Sscanf(string(b), "%d", &cp); err != nil {
		return 0, fmt.Errorf("checkpoint: %w", err)
	}
	return cp, nil
}

func (w *wal) setCheckpoint(cp uint64) error {
	if cp < w.checkpoint {
		return nil
	}
	path := filepath.Join(w.dir, walCheckpoint)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(fmt.Sprintf("%d\n", cp)), 0o644); err != nil {
		return err
	}
	if w.syncWrite {
		f, err := os.OpenFile(tmp, os.O_RDWR, 0)
		if err == nil {
			_ = f.Sync()
			_ = f.Close()
		}
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	w.checkpoint = cp
	return nil
}

func (w *wal) scanMaxSeq() (uint64, error) {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	var max uint64
	err := w.forEachRecord(func(rec walRecord) error {
		if rec.Seq > max {
			max = rec.Seq
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return max, nil
}

// appendEntries writes normalized entries to the WAL and returns assigned seqs.
func (w *wal) appendEntries(entries []Entry) ([]uint64, error) {
	seqs := make([]uint64, len(entries))
	for i, e := range entries {
		seq := w.nextSeq
		w.nextSeq++
		rec := walRecord{
			Seq:    seq,
			TimeNS: e.Time.UnixNano(),
			Fields: e.Fields,
		}
		if err := w.writeRecord(rec); err != nil {
			return nil, err
		}
		seqs[i] = seq
	}
	if w.syncWrite {
		if err := w.f.Sync(); err != nil {
			return nil, err
		}
	}
	return seqs, nil
}

func (w *wal) writeRecord(rec walRecord) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	crc := crc32.ChecksumIEEE(payload)
	var hdr [4 + 1 + 4]byte
	binary.LittleEndian.PutUint32(hdr[0:4], walMagic)
	hdr[4] = walVersion
	binary.LittleEndian.PutUint32(hdr[5:9], uint32(len(payload)))
	if _, err := w.f.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.f.Write(payload); err != nil {
		return err
	}
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	_, err = w.f.Write(crcBuf[:])
	return err
}

func (w *wal) forEachRecord(fn func(walRecord) error) error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	for {
		var hdr [9]byte
		_, err := io.ReadFull(w.f, hdr[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil // truncated tail ignored
		}
		if err != nil {
			return err
		}
		magic := binary.LittleEndian.Uint32(hdr[0:4])
		if magic != walMagic {
			return fmt.Errorf("wal: bad magic %x", magic)
		}
		if hdr[4] != walVersion {
			return fmt.Errorf("wal: unsupported version %d", hdr[4])
		}
		n := binary.LittleEndian.Uint32(hdr[5:9])
		payload := make([]byte, n)
		if _, err := io.ReadFull(w.f, payload); err != nil {
			return nil // truncated
		}
		var crcBuf [4]byte
		if _, err := io.ReadFull(w.f, crcBuf[:]); err != nil {
			return nil
		}
		want := binary.LittleEndian.Uint32(crcBuf[:])
		got := crc32.ChecksumIEEE(payload)
		if want != got {
			return fmt.Errorf("wal: crc mismatch")
		}
		var rec walRecord
		if err := json.Unmarshal(payload, &rec); err != nil {
			return err
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
}

func (w *wal) replayAfterCheckpoint(fn func(walRecord) error) error {
	return w.forEachRecord(func(rec walRecord) error {
		if rec.Seq <= w.checkpoint {
			return nil
		}
		return fn(rec)
	})
}

// compactLocked rewrites wal.log keeping only seq > checkpoint.
func (w *wal) compactLocked() error {
	tmpPath := filepath.Join(w.dir, walFileName+".tmp")
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	old := w.f
	keep := make([]walRecord, 0)
	if err := w.forEachRecord(func(rec walRecord) error {
		if rec.Seq > w.checkpoint {
			keep = append(keep, rec)
		}
		return nil
	}); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	w.f = tmp
	for _, rec := range keep {
		if err := w.writeRecord(rec); err != nil {
			_ = tmp.Close()
			w.f = old
			_ = os.Remove(tmpPath)
			return err
		}
	}
	if w.syncWrite {
		_ = tmp.Sync()
	}
	_ = tmp.Close()
	_ = old.Close()
	final := filepath.Join(w.dir, walFileName)
	if err := os.Rename(tmpPath, final); err != nil {
		return err
	}
	f, err := os.OpenFile(final, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return err
	}
	w.f = f
	return nil
}
