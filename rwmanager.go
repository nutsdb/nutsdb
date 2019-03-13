package nutsdb

type RWMode int

const (
	FileIO RWMode = iota
	MMap
)

type RWManager interface {
	WriteAt(b []byte, off int64) (n int, err error)
	ReadAt(b []byte, off int64) (n int, err error)
	Sync() (err error)
	Close() (err error)
}
