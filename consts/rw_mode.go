package consts

// RWMode represents the read and write mode.
type RWMode int

const (
	// FileIO represents the read and write mode using standard I/O.
	FileIO RWMode = iota

	// MMap represents the read and write mode using mmap.
	MMap
)

func (mode RWMode) IsFileIO() bool {
	return mode == FileIO
}

func (mode RWMode) IsMMap() bool {
	return mode == MMap
}
