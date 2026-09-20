package storage

// openTest opens storage with periodic flush/merge/retention disabled so unit tests stay deterministic.
func openTest(dir string, opts Options) (*Storage, error) {
	opts.InmemoryDataFlushInterval = -1
	if opts.MergeCheckInterval == 0 && !opts.MergeDisable {
		opts.MergeCheckInterval = -1
	}
	if opts.RetentionCheckInterval == 0 {
		opts.RetentionCheckInterval = -1
	}
	return Open(dir, opts)
}
