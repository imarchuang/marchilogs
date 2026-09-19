package storage

// openTest opens storage with periodic flush disabled so unit tests stay deterministic.
func openTest(dir string, opts Options) (*Storage, error) {
	opts.InmemoryDataFlushInterval = -1
	return Open(dir, opts)
}
