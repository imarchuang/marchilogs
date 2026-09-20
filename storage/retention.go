package storage

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func (o *Options) withRetentionDefaults() {
	// RetentionPeriod <= 0 disables retention.
	if o.RetentionCheckInterval == 0 && o.RetentionPeriod > 0 {
		o.RetentionCheckInterval = time.Hour
	}
}

// ApplyRetention deletes day partitions whose entire day is older than RetentionPeriod.
// Returns the number of day directories removed.
func (s *Storage) ApplyRetention() (int, error) {
	return s.applyRetentionAt(time.Now().UTC())
}

func (s *Storage) applyRetentionAt(now time.Time) (int, error) {
	if s.opts.RetentionPeriod <= 0 {
		return 0, nil
	}
	deadline := now.Add(-s.opts.RetentionPeriod)

	s.mu.RLock()
	days := make([]string, 0, len(s.manifests))
	for day := range s.manifests {
		days = append(days, day)
	}
	s.mu.RUnlock()

	// Also discover days that exist on disk but somehow missing from manifests map.
	partsRoot := filepath.Join(s.root, "partitions")
	if entries, err := os.ReadDir(partsRoot); err == nil {
		seen := make(map[string]struct{}, len(days))
		for _, d := range days {
			seen[d] = struct{}{}
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := e.Name()
			if _, err := parsePartitionName(name); err != nil {
				continue
			}
			if _, ok := seen[name]; !ok {
				days = append(days, name)
			}
		}
	}
	sort.Strings(days)

	var toDrop []string
	for _, day := range days {
		dayStart, err := parsePartitionName(day)
		if err != nil {
			continue
		}
		dayEnd := dayStart.Add(24*time.Hour - time.Nanosecond)
		if dayEnd.Before(deadline) {
			toDrop = append(toDrop, day)
		}
	}
	if len(toDrop) == 0 {
		return 0, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	dropped := 0
	for _, day := range toDrop {
		// Re-check under write lock in case of concurrent flush into that day.
		dayStart, err := parsePartitionName(day)
		if err != nil {
			continue
		}
		dayEnd := dayStart.Add(24*time.Hour - time.Nanosecond)
		if !dayEnd.Before(deadline) {
			continue
		}
		if err := s.dropDayLocked(day); err != nil {
			return dropped, err
		}
		dropped++
	}
	return dropped, nil
}

func (s *Storage) dropDayLocked(day string) error {
	prefix := day + "\x00"
	for key := range s.buffers {
		if strings.HasPrefix(key, prefix) {
			delete(s.buffers, key)
		}
	}
	delete(s.manifests, day)
	delete(s.indexdbs, day)
	delete(s.partSeq, day)

	dayDir := filepath.Join(s.root, "partitions", day)
	if err := os.RemoveAll(dayDir); err != nil {
		return err
	}
	return nil
}

func (s *Storage) startPeriodicRetention() {
	if s.opts.RetentionPeriod <= 0 || s.opts.RetentionCheckInterval <= 0 {
		return
	}
	s.retentionStop = make(chan struct{})
	s.retentionDone = make(chan struct{})
	every := s.opts.RetentionCheckInterval
	go func() {
		defer close(s.retentionDone)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				_, _ = s.ApplyRetention()
			case <-s.retentionStop:
				return
			}
		}
	}()
}

func (s *Storage) stopPeriodicRetention() {
	if s.retentionStop == nil {
		return
	}
	close(s.retentionStop)
	<-s.retentionDone
	s.retentionStop = nil
}
