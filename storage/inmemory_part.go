package storage

import (
	"strings"
	"time"
)

// searchBuffersRLocked scans live memBlocks under s.mu.RLock (no clone).
// Append/Flush take the write lock, so buffers are stable for the duration of the scan.
func (s *Storage) searchBuffersRLocked(q Query, out []Entry) []Entry {
	for key, b := range s.buffers {
		if b == nil || b.rows() == 0 {
			continue
		}
		part := strings.SplitN(key, "\x00", 2)[0]
		if !partitionOverlapsQuery(part, q.Start, q.End) {
			continue
		}
		if len(q.StreamEq) > 0 && !streamMatchesTags(b.stream, q.StreamEq) {
			continue
		}
		if !b.overlaps(q.Start, q.End) {
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

func partitionOverlapsQuery(partName string, start, end time.Time) bool {
	day, err := parsePartitionName(partName)
	if err != nil {
		return false
	}
	dayEnd := day.Add(24*time.Hour - time.Nanosecond)
	if !end.IsZero() && day.After(end) {
		return false
	}
	if !start.IsZero() && dayEnd.Before(start) {
		return false
	}
	return true
}
