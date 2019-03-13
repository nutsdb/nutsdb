package nutsdb

import "os"

type FileIORWManager struct {
	fd *os.File
}

func NewFileIORWManager(path string, capacity int64) (*FileIORWManager, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	err = Truncate(path, capacity, fd)
	if err != nil {
		return nil, err
	}

	return &FileIORWManager{fd: fd}, nil
}

func (fm *FileIORWManager) WriteAt(b []byte, off int64) (n int, err error) {
	return fm.fd.WriteAt(b, off)
}

func (fm *FileIORWManager) ReadAt(b []byte, off int64) (n int, err error) {
	return fm.fd.ReadAt(b, off)
}

func (fm *FileIORWManager) Sync() (err error) {
	return fm.fd.Sync()
}

func (fm *FileIORWManager) Close() (err error) {
	return fm.fd.Close()
}
