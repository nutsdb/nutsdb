package nutsdb

import (
	"bufio"
	"io"
	"os"
)

// fileRecovery use bufio.Reader to read entry
type fileRecovery struct {
	fd     *os.File
	reader *bufio.Reader
}

func newFileRecovery(path string, bufSize int) (fr *fileRecovery, err error) {
	fd, err := os.OpenFile(path, os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	bufSize = calBufferSize(bufSize)
	return &fileRecovery{
		fd:     fd,
		reader: bufio.NewReaderSize(fd, bufSize),
	}, nil
}

// readEntry will read an Entry from disk.
func (fr *fileRecovery) readEntry() (e *Entry, err error) {
	buf := make([]byte, DataEntryHeaderSize)
	_, err = io.ReadFull(fr.reader, buf)
	if err != nil {
		return nil, err
	}

	e = new(Entry)
	err = e.ParseMeta(buf)
	if err != nil {
		return nil, err
	}

	if e.IsZero() {
		return nil, nil
	}

	meta := e.Meta
	dataSize := meta.PayloadSize()
	dataBuf := make([]byte, dataSize)
	_, err = io.ReadFull(fr.reader, dataBuf)
	if err != nil {
		return nil, err
	}
	err = e.ParsePayload(dataBuf)
	if err != nil {
		return nil, err
	}

	crc := e.GetCrc(buf)
	if crc != e.Meta.Crc {
		return nil, ErrCrc
	}

	return e, nil
}

// calBufferSize calculates the buffer size of bufio.Reader
// if the size < 4 * KB, use 4 * KB as the size of buffer in bufio.Reader
// if the size > 4 * KB, use the nearly blockSize buffer as the size of buffer in bufio.Reader
func calBufferSize(size int) int {
	blockSize := 4 * KB
	if size < blockSize {
		return blockSize
	}
	hasRest := (size%blockSize == 0)
	if hasRest {
		return (size/blockSize + 1) * blockSize
	}
	return size
}

func (fr *fileRecovery) release() error {
	return fr.fd.Close()
}
