package nutsdb

import (
	"errors"

	"github.com/nutsdb/nutsdb/internal/fileio"
)

// RWMode represents the read and write mode.
type RWMode int

const (
	// FileIO represents the read and write mode using standard I/O.
	FileIO RWMode = iota

	// MMap represents the read and write mode using mmap.
	MMap
)

// FileManager holds the fd cache and file-related operations go through the manager to obtain the file processing object
type FileManager struct {
	rwMode      RWMode
	fdm         *fileio.FdManager
	segmentSize int64
}

// NewFileManager will create a NewFileManager object
func NewFileManager(rwMode RWMode, maxFdNums int, cleanThreshold float64, segmentSize int64) (fm *FileManager) {
	fm = &FileManager{
		rwMode:      rwMode,
		fdm:         fileio.NewFdm(maxFdNums, cleanThreshold),
		segmentSize: segmentSize,
	}
	return fm
}

// GetFileRWManager will return a FileIORWManager Object
func (fm *FileManager) GetFileRWManager(path string, capacity int64, segmentSize int64, readOnly bool) (*fileio.FileIORWManager, error) {
	fd, err := fm.fdm.GetFd(path)
	if err != nil {
		return nil, err
	}
	err = fileio.Truncate(path, capacity, fd, readOnly)
	if err != nil {
		return nil, err
	}

	return &fileio.FileIORWManager{Fd: fd, Path: path, Fdm: fm.fdm, SegmentSize: segmentSize}, nil
}

// GetMMapRWManager will return a MMapRWManager Object
func (fm *FileManager) GetMMapRWManager(path string, capacity int64, segmentSize int64, readOnly bool) (*fileio.MMapRWManager, error) {
	fd, err := fm.fdm.GetFd(path)
	if err != nil {
		return nil, err
	}

	err = fileio.Truncate(path, capacity, fd, readOnly)
	if err != nil {
		return nil, err
	}

	return fileio.GetMMapRWManager(fd, path, fm.fdm, segmentSize), nil
}

func (fm *FileManager) GetRWManager(
	path string,
	capacity int64,
	segmentSize int64,
	readOnly bool,
) (fileio.RWManager, error) {
	if capacity <= 0 {
		return nil, ErrCapacity
	}
	if fm.rwMode == FileIO {
		return fm.GetFileRWManager(path, capacity, segmentSize, readOnly)
	}
	if fm.rwMode == MMap {
		return fm.GetMMapRWManager(path, capacity, segmentSize, readOnly)
	}
	return nil, errors.New("invalid rw mode")
}

// Close will Close fdm resource
func (fm *FileManager) Close() error {
	err := fm.fdm.Close()
	return err
}
