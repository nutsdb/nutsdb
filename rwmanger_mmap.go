package nutsdb

import (
	"errors"
	"os"

	"github.com/xujiajun/mmap-go"
)

type MMapRWManager struct {
	m mmap.MMap
}

func NewMMapRWManager(path string, capacity int64) (*MMapRWManager, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	err = Truncate(path, capacity, f)
	if err != nil {
		return nil, err
	}

	m, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	return &MMapRWManager{m: m}, nil
}

func (mm *MMapRWManager) WriteAt(b []byte, off int64) (n int, err error) {
	if mm.m == nil {
		return 0, errors.New("")
	} else if off >= int64(len(mm.m)) || off < 0 {
		return 0, errors.New("")
	}

	return copy(mm.m[off:], b), nil
}

func (mm *MMapRWManager) ReadAt(b []byte, off int64) (n int, err error) {
	if mm.m == nil {
		return 0, errors.New("")
	} else if off >= int64(len(mm.m)) || off < 0 {
		return 0, errors.New("")
	}

	return copy(b, mm.m[off:]), nil
}

func (mm *MMapRWManager) Sync() (err error) {
	return mm.m.Flush()
}

func (mm *MMapRWManager) Close() (err error) {
	return mm.m.Unmap()
}
