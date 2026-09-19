package storage

import (
	"sort"
	"strings"
	"time"
)

// inmemoryPart is a queryable snapshot of buffered memBlocks for one day partition.
// It mirrors on-disk partMeta (stream tags + block time ranges) without touching disk.
type inmemoryPart struct {
	partition string
	meta      partMeta
	blocks    map[string]*memBlock // stream id → columnar block
}

func (b *memBlock) clone() *memBlock {
	if b == nil {
		return nil
	}
	out := &memBlock{
		stream:  b.stream,
		times:   append([]int64(nil), b.times...),
		fields:  make(map[string][]string, len(b.fields)),
		timeMin: b.timeMin,
		timeMax: b.timeMax,
	}
	for k, col := range b.fields {
		out.fields[k] = append([]string(nil), col...)
	}
	return out
}

// snapshotInmemoryLocked groups current buffers into per-day inmemoryParts.
// Caller must hold s.mu. Returned blocks are clones so Append can proceed after unlock.
func (s *Storage) snapshotInmemoryLocked() map[string]*inmemoryPart {
	type acc struct {
		infos  []streamFlushInfo
		blocks map[string]*memBlock
		tMin   int64
		tMax   int64
		n      int
	}
	byPart := make(map[string]*acc)

	for key, b := range s.buffers {
		if b == nil || b.rows() == 0 {
			continue
		}
		part := strings.SplitN(key, "\x00", 2)[0]
		a := byPart[part]
		if a == nil {
			a = &acc{blocks: make(map[string]*memBlock)}
			byPart[part] = a
		}
		cl := b.clone()
		a.blocks[cl.stream] = cl
		a.infos = append(a.infos, streamFlushInfo{
			ID:        cl.stream,
			TimeMinNS: cl.timeMin,
			TimeMaxNS: cl.timeMax,
			Rows:      cl.rows(),
		})
		if a.n == 0 {
			a.tMin, a.tMax = cl.timeMin, cl.timeMax
		} else {
			if cl.timeMin < a.tMin {
				a.tMin = cl.timeMin
			}
			if cl.timeMax > a.tMax {
				a.tMax = cl.timeMax
			}
		}
		a.n++
	}

	out := make(map[string]*inmemoryPart, len(byPart))
	for part, a := range byPart {
		meta := buildPartMeta(a.infos, a.tMin, a.tMax)
		// Paths are unused for in-memory blocks; keep stream id for debugging.
		for i := range meta.Streams {
			if len(meta.Streams[i].Blocks) == 1 {
				meta.Streams[i].Blocks[0].Path = "memory:" + meta.Streams[i].ID
			}
		}
		out[part] = &inmemoryPart{
			partition: part,
			meta:      meta,
			blocks:    a.blocks,
		}
	}
	return out
}

func searchInmemoryPart(ip *inmemoryPart, q Query, out []Entry) []Entry {
	if ip == nil {
		return out
	}
	if !timeRangeOverlaps(q.Start, q.End, ip.meta.TimeMinNS, ip.meta.TimeMaxNS) {
		return out
	}
	candidates := matchStreamIDs(ip.meta, q.StreamEq)
	for _, sm := range candidates {
		b := ip.blocks[sm.ID]
		if b == nil || b.rows() == 0 {
			continue
		}
		refs := sm.Blocks
		if len(refs) == 0 {
			refs = []blockRef{{
				TimeMinNS: b.timeMin,
				TimeMaxNS: b.timeMax,
				Rows:      b.rows(),
			}}
		}
		refs = filterBlockRefs(refs, q.Start, q.End)
		if len(refs) == 0 {
			continue
		}
		for i := 0; i < b.rows(); i++ {
			e := b.row(i)
			if !q.Start.IsZero() && e.Time.Before(q.Start) {
				continue
			}
			if !q.End.IsZero() && e.Time.After(q.End) {
				continue
			}
			if q.Contains != "" && !strings.Contains(e.Msg(), q.Contains) {
				continue
			}
			out = append(out, e)
			if q.Limit > 0 && len(out) >= q.Limit {
				return out
			}
		}
	}
	return out
}

func mergePartitionNames(disk []string, mem map[string]*inmemoryPart, start, end time.Time) []string {
	seen := make(map[string]struct{}, len(disk)+len(mem))
	var names []string
	for _, n := range disk {
		seen[n] = struct{}{}
		names = append(names, n)
	}
	for part := range mem {
		if _, ok := seen[part]; ok {
			continue
		}
		day, err := parsePartitionName(part)
		if err != nil {
			continue
		}
		dayEnd := day.Add(24*time.Hour - time.Nanosecond)
		if !end.IsZero() && day.After(end) {
			continue
		}
		if !start.IsZero() && dayEnd.Before(start) {
			continue
		}
		names = append(names, part)
	}
	sort.Strings(names)
	return names
}
