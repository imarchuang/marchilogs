package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const mergingPrefix = ".merging-"

func (o *Options) withMergeDefaults() {
	if o.MergeMinParts <= 0 {
		o.MergeMinParts = 4
	}
	if o.MergeMaxPartsPerJob <= 0 {
		o.MergeMaxPartsPerJob = 8
	}
	if o.MergeCheckInterval == 0 && !o.MergeDisable {
		o.MergeCheckInterval = 10 * time.Second
	}
}

func (s *Storage) startPeriodicMerge() {
	if s.opts.MergeDisable || s.opts.MergeCheckInterval <= 0 {
		return
	}
	s.mergeStop = make(chan struct{})
	s.mergeDone = make(chan struct{})
	every := s.opts.MergeCheckInterval
	go func() {
		defer close(s.mergeDone)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				_ = s.runMergePass()
			case <-s.mergeStop:
				return
			}
		}
	}()
}

func (s *Storage) stopPeriodicMerge() {
	if s.mergeStop == nil {
		return
	}
	close(s.mergeStop)
	<-s.mergeDone
	s.mergeStop = nil
}

// runMergePass tries one merge job across day partitions (global single-flight).
func (s *Storage) runMergePass() error {
	s.mu.RLock()
	days := make([]string, 0, len(s.manifests))
	for day := range s.manifests {
		days = append(days, day)
	}
	s.mu.RUnlock()
	sort.Strings(days)
	for _, day := range days {
		merged, err := s.tryMergeDay(day)
		if err != nil {
			return err
		}
		if merged {
			return nil // one job per pass
		}
	}
	return nil
}

// tryMergeDay merges oldest small parts when count >= MergeMinParts.
// Returns true if a merge ran.
func (s *Storage) tryMergeDay(day string) (bool, error) {
	s.mu.RLock()
	ids := append([]string(nil), s.manifests[day]...)
	s.mu.RUnlock()
	if len(ids) < s.opts.MergeMinParts {
		return false, nil
	}

	small, err := s.listSmallPartIDs(day, ids)
	if err != nil {
		return false, err
	}
	if len(small) < s.opts.MergeMinParts {
		return false, nil
	}
	n := s.opts.MergeMaxPartsPerJob
	if n > len(small) {
		n = len(small)
	}
	inputs := small[:n]
	if err := s.mergeParts(day, inputs); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Storage) listSmallPartIDs(day string, ids []string) ([]string, error) {
	var small []string
	for _, id := range ids {
		tier, err := s.partTier(day, id)
		if err != nil {
			return nil, err
		}
		if tier == partTierBig {
			continue
		}
		small = append(small, id)
	}
	sort.Strings(small) // oldest numeric ids first
	return small, nil
}

func (s *Storage) partTier(day, id string) (string, error) {
	var meta partMeta
	path := filepath.Join(s.root, "partitions", day, "parts", id, "meta.json")
	if err := readJSON(path, &meta); err != nil {
		return "", err
	}
	if meta.Tier == "" {
		return partTierSmall, nil
	}
	return meta.Tier, nil
}

// mergeParts rewrites inputs into one big part, swaps manifest, deletes inputs.
func (s *Storage) mergeParts(day string, inputs []string) error {
	if len(inputs) == 0 {
		return nil
	}
	partsParent := filepath.Join(s.root, "partitions", day, "parts")
	if err := os.MkdirAll(partsParent, 0o755); err != nil {
		return err
	}

	// Allocate output id while holding the lock; verify inputs still active.
	s.mu.Lock()
	for _, id := range inputs {
		if !containsString(s.manifests[day], id) {
			s.mu.Unlock()
			return nil // raced with another merge / flush path; skip
		}
	}
	s.partSeq[day]++
	outID := s.partSeq[day]
	finalName := fmt.Sprintf("%06d", outID)
	finalDir := filepath.Join(partsParent, finalName)
	tmpDir := filepath.Join(partsParent, mergingPrefix+finalName)
	s.mu.Unlock()

	_ = os.RemoveAll(tmpDir)
	rollbackSeq := func() {
		s.mu.Lock()
		if s.partSeq[day] == outID {
			s.partSeq[day]--
		}
		s.mu.Unlock()
		_ = os.RemoveAll(tmpDir)
		_ = os.RemoveAll(finalDir)
	}

	if err := s.writeMergedPart(day, inputs, tmpDir); err != nil {
		rollbackSeq()
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Re-check inputs still in manifest before publish.
	for _, id := range inputs {
		if !containsString(s.manifests[day], id) {
			rollbackSeqUnlocked(s, day, outID, tmpDir, finalDir)
			return nil
		}
	}

	if err := os.Rename(tmpDir, finalDir); err != nil {
		rollbackSeqUnlocked(s, day, outID, tmpDir, finalDir)
		return fmt.Errorf("publish merged part %s: %w", finalName, err)
	}
	if err := s.replaceManifestPartsLocked(day, inputs, []string{finalName}); err != nil {
		rollbackSeqUnlocked(s, day, outID, "", finalDir)
		return err
	}

	// Delete inputs after manifest swap (Search no longer sees them).
	for _, id := range inputs {
		_ = os.RemoveAll(filepath.Join(partsParent, id))
	}
	return nil
}

func rollbackSeqUnlocked(s *Storage, day string, outID int, tmpDir, finalDir string) {
	if s.partSeq[day] == outID {
		s.partSeq[day]--
	}
	if tmpDir != "" {
		_ = os.RemoveAll(tmpDir)
	}
	if finalDir != "" {
		_ = os.RemoveAll(finalDir)
	}
}

func (s *Storage) writeMergedPart(day string, inputs []string, outDir string) error {
	blocksDir := filepath.Join(outDir, "blocks")
	if err := os.MkdirAll(blocksDir, 0o755); err != nil {
		return err
	}

	// stream id → concatenated memBlock (inputs already oldest-first)
	byStream := make(map[string]*memBlock)
	var streamOrder []string

	for _, id := range inputs {
		partDir := filepath.Join(s.root, "partitions", day, "parts", id)
		var meta partMeta
		if err := readJSON(filepath.Join(partDir, "meta.json"), &meta); err != nil {
			return err
		}
		// Stable order within part: use Streams order from meta.
		for _, sm := range meta.Streams {
			dirName := sm.Dir
			if dirName == "" {
				dirName = safeStreamDir(sm.ID)
			}
			refs := sm.Blocks
			if len(refs) == 0 {
				refs = []blockRef{{Path: filepath.Join("blocks", dirName)}}
			}
			dst := byStream[sm.ID]
			if dst == nil {
				dst = newMemBlock(sm.ID)
				byStream[sm.ID] = dst
				streamOrder = append(streamOrder, sm.ID)
			}
			for _, ref := range refs {
				b, err := readBlock(filepath.Join(partDir, ref.Path))
				if err != nil {
					return fmt.Errorf("read %s/%s: %w", id, sm.ID, err)
				}
				for i := 0; i < b.rows(); i++ {
					dst.add(b.row(i))
				}
			}
		}
	}

	infos := make([]streamFlushInfo, 0, len(streamOrder))
	var tMin, tMax int64
	first := true
	for _, sid := range streamOrder {
		b := byStream[sid]
		dir := filepath.Join(blocksDir, safeStreamDir(sid))
		if err := b.writeTo(dir); err != nil {
			return err
		}
		infos = append(infos, streamFlushInfo{
			ID:        sid,
			TimeMinNS: b.timeMin,
			TimeMaxNS: b.timeMax,
			Rows:      b.rows(),
		})
		if first {
			tMin, tMax = b.timeMin, b.timeMax
			first = false
		} else {
			if b.timeMin < tMin {
				tMin = b.timeMin
			}
			if b.timeMax > tMax {
				tMax = b.timeMax
			}
		}
	}

	meta := buildPartMeta(infos, tMin, tMax)
	meta.Tier = partTierBig
	return writeJSON(filepath.Join(outDir, "meta.json"), meta)
}

func containsString(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
