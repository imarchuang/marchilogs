package storage

// QueryStats summarizes how much work a Search did (for debugging / tuning).
type QueryStats struct {
	// MemBlocksScanned is in-memory stream buffers examined (after day/stream/time prune).
	MemBlocksScanned int `json:"mem_blocks_scanned"`

	// PartsScanned is published disk parts opened from the manifest.
	PartsScanned int `json:"parts_scanned"`

	// BlocksSeen is disk blocks that passed stream + block-time filters.
	BlocksSeen int `json:"blocks_seen"`

	// BlocksSkippedBloom is BlocksSeen rejected by _msg bloom without reading columns.
	BlocksSkippedBloom int `json:"blocks_skipped_bloom"`

	// BlocksScanned is disk blocks whose columnar files were read.
	BlocksScanned int `json:"blocks_scanned"`

	// RowsScanned is rows iterated in memory + on disk (before Contains/time row filters may drop them).
	RowsScanned int `json:"rows_scanned"`

	// RowsReturned is len of the result set.
	RowsReturned int `json:"rows_returned"`
}
