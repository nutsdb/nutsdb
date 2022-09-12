package nutsdb

import (
	"github.com/xujiajun/mmap-go"
)

// fileManager holds the fd cache and file-related operations go through the manager to obtain the file processing object
type fileManager struct {
	rwMode RWMode
	fdm    *fdManager
}

// newFileManager will create a newFileManager object
func newFileManager(rwMode RWMode, maxFdNums int, cleanThreshold float64) (fm *fileManager) {
	fm = &fileManager{
		rwMode: rwMode,
		fdm:    newFdm(maxFdNums, cleanThreshold),
	}
	return fm
}

// getDataFile will return a DataFile Object
func (fm *fileManager) getDataFile(path string, capacity int64) (datafile *DataFile, err error) {
	if capacity <= 0 {
		return nil, ErrCapacity
	}

	var rwManager RWManager

	if fm.rwMode == FileIO {
		rwManager, err = fm.getFileRWManager(path, capacity)
		if err != nil {
			return nil, err
		}
	}

	if fm.rwMode == MMap {
		rwManager, err = fm.getMMapRWManager(path, capacity)
		if err != nil {
			return nil, err
		}
	}

	return NewDataFile(path, rwManager), nil
}

// getFileRWManager will return a FileIORWManager Object
func (fm *fileManager) getFileRWManager(path string, capacity int64) (*FileIORWManager, error) {
	fd, err := fm.fdm.getFd(path)
	if err != nil {
		return nil, err
	}
	err = Truncate(path, capacity, fd)
	if err != nil {
		return nil, err
	}

	return &FileIORWManager{fd: fd, path: path, fdm: fm.fdm}, nil
}

// getMMapRWManager will return a MMapRWManager Object
func (fm *fileManager) getMMapRWManager(path string, capacity int64) (*MMapRWManager, error) {
	fd, err := fm.fdm.getFd(path)
	if err != nil {
		return nil, err
	}

	err = Truncate(path, capacity, fd)
	if err != nil {
		return nil, err
	}

	m, err := mmap.Map(fd, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	return &MMapRWManager{m: m, path: path, fdm: fm.fdm}, nil
}

// close will close fdm resource
func (fm *fileManager) close() error {
	err := fm.fdm.close()
	return err
}
