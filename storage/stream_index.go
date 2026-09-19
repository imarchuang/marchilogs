package storage

import (
	"sort"
	"strings"
)

// partMeta is written beside each flushed part for stream discovery / prune.
type partMeta struct {
	TimeMinNS int64               `json:"time_min_ns"`
	TimeMaxNS int64               `json:"time_max_ns"`
	Streams   []streamMeta        `json:"streams"`
	ByTag     map[string][]string `json:"by_tag"` // "service=api" → stream ids
}

type streamMeta struct {
	ID   string            `json:"id"`
	Dir  string            `json:"dir"`
	Tags map[string]string `json:"tags"`
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

func buildPartMeta(streams []string, tMin, tMax int64) partMeta {
	meta := partMeta{
		TimeMinNS: tMin,
		TimeMaxNS: tMax,
		Streams:   make([]streamMeta, 0, len(streams)),
		ByTag:     make(map[string][]string),
	}
	for _, id := range streams {
		tags := ParseStreamTags(id)
		sm := streamMeta{ID: id, Dir: safeStreamDir(id), Tags: tags}
		meta.Streams = append(meta.Streams, sm)
		for k, v := range tags {
			tk := tagKey(k, v)
			meta.ByTag[tk] = append(meta.ByTag[tk], id)
		}
	}
	for k := range meta.ByTag {
		sort.Strings(meta.ByTag[k])
	}
	return meta
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
