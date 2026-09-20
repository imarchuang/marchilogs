package storage

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
)

const (
	bloomFileName       = "_msg.bloom"
	bloomHashes         = 6
	bloomBitsPerTrigram = 16
	bloomTrigramLen     = 3
	bloomVersion        = 1
)

// bloomFilter is a classic bitset bloom used to skip blocks on Contains queries.
// We index UTF-8 byte trigrams of _msg so arbitrary substrings of length ≥3
// can be rejected without false negatives (false positives still force a full read).
type bloomFilter struct {
	bits []uint64
}

func newBloomFilter(trigramCount int) *bloomFilter {
	if trigramCount < 1 {
		trigramCount = 1
	}
	nBits := trigramCount * bloomBitsPerTrigram
	nWords := (nBits + 63) / 64
	if nWords < 1 {
		nWords = 1
	}
	return &bloomFilter{bits: make([]uint64, nWords)}
}

func (bf *bloomFilter) nBits() int {
	return len(bf.bits) * 64
}

func (bf *bloomFilter) add(s string) {
	if s == "" {
		return
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	x := h.Sum64()
	n := uint64(bf.nBits())
	for i := 0; i < bloomHashes; i++ {
		bit := (x + uint64(i)*0x9e3779b97f4a7c15) % n
		bf.bits[bit/64] |= 1 << (bit % 64)
	}
}

func (bf *bloomFilter) mightContain(s string) bool {
	if s == "" || len(bf.bits) == 0 {
		return true
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	x := h.Sum64()
	n := uint64(bf.nBits())
	for i := 0; i < bloomHashes; i++ {
		bit := (x + uint64(i)*0x9e3779b97f4a7c15) % n
		if bf.bits[bit/64]&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// mightContainSubstring reports whether needle could appear as a substring.
// Needles shorter than 3 bytes cannot be rejected (return true).
func (bf *bloomFilter) mightContainSubstring(needle string) bool {
	if len(needle) < bloomTrigramLen {
		return true
	}
	for _, tg := range byteTrigrams(needle) {
		if !bf.mightContain(tg) {
			return false
		}
	}
	return true
}

func byteTrigrams(s string) []string {
	if len(s) < bloomTrigramLen {
		return nil
	}
	out := make([]string, 0, len(s)-bloomTrigramLen+1)
	for i := 0; i+bloomTrigramLen <= len(s); i++ {
		out = append(out, s[i:i+bloomTrigramLen])
	}
	return out
}

func buildMsgBloom(msgs []string) *bloomFilter {
	n := 0
	for _, m := range msgs {
		if len(m) >= bloomTrigramLen {
			n += len(m) - bloomTrigramLen + 1
		}
	}
	bf := newBloomFilter(n)
	for _, m := range msgs {
		for _, tg := range byteTrigrams(m) {
			bf.add(tg)
		}
	}
	return bf
}

func (bf *bloomFilter) marshal() []byte {
	dst := make([]byte, 1+4+len(bf.bits)*8)
	dst[0] = bloomVersion
	binary.LittleEndian.PutUint32(dst[1:5], uint32(len(bf.bits)))
	for i, w := range bf.bits {
		binary.LittleEndian.PutUint64(dst[5+i*8:], w)
	}
	return dst
}

func unmarshalBloom(src []byte) (*bloomFilter, error) {
	if len(src) < 5 {
		return nil, fmt.Errorf("bloom too short")
	}
	if src[0] != bloomVersion {
		return nil, fmt.Errorf("bloom version %d", src[0])
	}
	nWords := int(binary.LittleEndian.Uint32(src[1:5]))
	need := 5 + nWords*8
	if nWords < 1 || len(src) < need {
		return nil, fmt.Errorf("bloom truncated: words=%d len=%d", nWords, len(src))
	}
	bits := make([]uint64, nWords)
	for i := 0; i < nWords; i++ {
		bits[i] = binary.LittleEndian.Uint64(src[5+i*8:])
	}
	return &bloomFilter{bits: bits}, nil
}

func writeMsgBloom(path string, msgs []string) error {
	bf := buildMsgBloom(msgs)
	return os.WriteFile(path, bf.marshal(), 0o644)
}

func readMsgBloom(path string) (*bloomFilter, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return unmarshalBloom(b)
}

// blockMightContainMsg loads _msg.bloom when present.
// missing/unreadable bloom → true (safe fallback: do not skip).
func blockMightContainMsg(blockDir, contains string) bool {
	if contains == "" || len(contains) < bloomTrigramLen {
		return true
	}
	bf, err := readMsgBloom(filepath.Join(blockDir, bloomFileName))
	if err != nil {
		return true
	}
	return bf.mightContainSubstring(contains)
}
