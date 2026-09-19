package storage

import (
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// partMeta is written beside each flushed part for stream discovery / prune.
type partMeta struct {
	TimeMinNS int64               `json:"time_min_ns"`
	TimeMaxNS int64               `json:"time_max_ns"`
	Streams   []streamMeta        `json:"streams"`
	ByTag     map[string][]string `json:"by_tag"` // "service=api" → stream ids
}

// blockRef is enough to prune by time without opening columnar files.
type blockRef struct {
	Path      string `json:"path"` // relative to part dir, e.g. blocks/host=h1,service=api
	TimeMinNS int64  `json:"time_min_ns"`
	TimeMaxNS int64  `json:"time_max_ns"`
	Rows      int    `json:"rows"`
}

type streamMeta struct {
	ID     string            `json:"id"`
	Dir    string            `json:"dir"`
	Tags   map[string]string `json:"tags"`
	Blocks []blockRef        `json:"blocks"`
}

// streamFlushInfo is one stream's block summary at flush time.
type streamFlushInfo struct {
	ID        string
	TimeMinNS int64
	TimeMaxNS int64
	Rows      int
}

// ParseStreamTags turns "host=h1,service=api" into a tag map.
func ParseStreamTags(streamID string) map[string]string {
	if streamID == "" || streamID == "_default" {
		return map[string]string{}
	}
	tags := make(map[string]string)
	for _, part := range strings.Split(streamID, ",") {
		k, v, ok := strings.Cut(part, "=")
		if !ok || k == "" {
			continue
		}
		tags[k] = v
	}
	return tags
}

func tagKey(field, value string) string {
	return field + "=" + value
}

func buildPartMeta(infos []streamFlushInfo, tMin, tMax int64) partMeta {
	meta := partMeta{
		TimeMinNS: tMin,
		TimeMaxNS: tMax,
		Streams:   make([]streamMeta, 0, len(infos)),
		ByTag:     make(map[string][]string),
	}
	for _, info := range infos {
		tags := ParseStreamTags(info.ID)
		dir := safeStreamDir(info.ID)
		sm := streamMeta{
			ID:   info.ID,
			Dir:  dir,
			Tags: tags,
			Blocks: []blockRef{{
				Path:      filepath.Join("blocks", dir),
				TimeMinNS: info.TimeMinNS,
				TimeMaxNS: info.TimeMaxNS,
				Rows:      info.Rows,
			}},
		}
		meta.Streams = append(meta.Streams, sm)
		for k, v := range tags {
			tk := tagKey(k, v)
			meta.ByTag[tk] = append(meta.ByTag[tk], info.ID)
		}
	}
	for k := range meta.ByTag {
		sort.Strings(meta.ByTag[k])
	}
	return meta
}

// buildPartMetaFromIDs is a test helper when block times are irrelevant.
func buildPartMetaFromIDs(ids []string, tMin, tMax int64) partMeta {
	infos := make([]streamFlushInfo, len(ids))
	for i, id := range ids {
		infos[i] = streamFlushInfo{ID: id}
	}
	return buildPartMeta(infos, tMin, tMax)
}

// matchStreamIDs returns stream ids in this part that satisfy StreamEq (AND subset match).
// Empty StreamEq → all streams in the part.
func matchStreamIDs(meta partMeta, streamEq map[string]string) map[string]streamMeta {
	byID := make(map[string]streamMeta, len(meta.Streams))
	for _, s := range meta.Streams {
		byID[s.ID] = s
	}
	if len(streamEq) == 0 {
		return byID
	}

	var candidates map[string]struct{}
	first := true
	for k, v := range streamEq {
		if v == "" {
			continue
		}
		ids := meta.ByTag[tagKey(k, v)]
		set := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			set[id] = struct{}{}
		}
		if first {
			candidates = set
			first = false
			continue
		}
		for id := range candidates {
			if _, ok := set[id]; !ok {
				delete(candidates, id)
			}
		}
	}
	if first {
		// StreamEq had only empty values
		return byID
	}

	out := make(map[string]streamMeta, len(candidates))
	for id := range candidates {
		if sm, ok := byID[id]; ok {
			out[id] = sm
		}
	}
	return out
}

// streamMatchesTags is a fallback when by_tag is missing (legacy parts).
func streamMatchesTags(streamID string, streamEq map[string]string) bool {
	if len(streamEq) == 0 {
		return true
	}
	tags := ParseStreamTags(streamID)
	for k, v := range streamEq {
		if v == "" {
			continue
		}
		if tags[k] != v {
			return false
		}
	}
	return true
}

// timeRangeOverlaps reports whether [minNS, maxNS] intersects [start, end].
// Zero start/end means unbounded on that side.
func timeRangeOverlaps(start, end time.Time, minNS, maxNS int64) bool {
	minT := time.Unix(0, minNS).UTC()
	maxT := time.Unix(0, maxNS).UTC()
	if !end.IsZero() && minT.After(end) {
		return false
	}
	if !start.IsZero() && maxT.Before(start) {
		return false
	}
	return true
}

// filterBlockRefs keeps only blocks whose stored time range overlaps the query window.
func filterBlockRefs(refs []blockRef, start, end time.Time) []blockRef {
	if len(refs) == 0 {
		return nil
	}
	if start.IsZero() && end.IsZero() {
		return refs
	}
	out := make([]blockRef, 0, len(refs))
	for _, r := range refs {
		if timeRangeOverlaps(start, end, r.TimeMinNS, r.TimeMaxNS) {
			out = append(out, r)
		}
	}
	return out
}
