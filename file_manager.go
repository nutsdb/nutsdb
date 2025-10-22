package nutsdb

import "github.com/nutsdb/nutsdb/internal/fileio"

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

// GetDataFile will return a DataFile Object
func (fm *FileManager) GetDataFile(path string, capacity int64) (datafile *DataFile, err error) {
	return fm.getDataFileWithMode(path, capacity, false)
}

// GetDataFileReadOnly will return a DataFile Object for read-only operations
// This method skips file truncation to improve read performance
func (fm *FileManager) GetDataFileReadOnly(path string, capacity int64) (datafile *DataFile, err error) {
	return fm.getDataFileWithMode(path, capacity, true)
}

// getDataFileWithMode will return a DataFile Object with specified read-only mode
func (fm *FileManager) getDataFileWithMode(path string, capacity int64, readOnly bool) (datafile *DataFile, err error) {
	if capacity <= 0 {
		return nil, ErrCapacity
	}

	var rwManager fileio.RWManager

	if fm.rwMode == FileIO {
		rwManager, err = fm.GetFileRWManager(path, capacity, fm.segmentSize, readOnly)
		if err != nil {
			return nil, err
		}
	}

	if fm.rwMode == MMap {
		rwManager, err = fm.GetMMapRWManager(path, capacity, fm.segmentSize, readOnly)
		if err != nil {
			return nil, err
		}
	}

	return NewDataFile(path, rwManager), nil
}

func (fm *FileManager) GetDataFileByID(dir string, fileID int64, capacity int64) (*DataFile, error) {
	path := getDataPath(fileID, dir)
	return fm.GetDataFile(path, capacity)
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

// Close will Close fdm resource
func (fm *FileManager) Close() error {
	err := fm.fdm.Close()
	return err
}
