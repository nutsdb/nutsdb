package nutsdb

// RWMode represents the read and write mode.
type RWMode int

const (
	//FileIO represents the read and write mode using standard I/O.
	FileIO RWMode = iota

	//MMap represents the read and write mode using mmap.
	MMap
)

// RWManager represents an interface to a RWManager.
type RWManager interface {
	WriteAt(b []byte, off int64) (n int, err error)
	ReadAt(b []byte, off int64) (n int, err error)
	Sync() (err error)
	Close() (err error)
}
