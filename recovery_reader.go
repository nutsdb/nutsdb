package nutsdb

import (
	"bufio"
	"encoding/binary"
	"os"
)

// fileRecovery use bufio.Reader to read entry
type fileRecovery struct {
	reader *bufio.Reader
}

func newFileRecovery(path string, bufSize int) (fr *fileRecovery, err error) {
	fd, err := os.OpenFile(path, os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	bufSize = calBufferSize(bufSize)
	return &fileRecovery{
		reader: bufio.NewReaderSize(fd, bufSize),
	}, nil
}

// readEntry will read an Entry from disk.
func (fr *fileRecovery) readEntry() (e *Entry, err error) {
	buf, err := fr.readData(DataEntryHeaderSize)
	if err != nil {
		return nil, err
	}
	meta := readMetaData(buf)

	e = &Entry{
		crc:  binary.LittleEndian.Uint32(buf[0:4]),
		Meta: meta,
	}

	if e.IsZero() {
		return nil, nil
	}

	dataSize := meta.BucketSize + meta.KeySize + meta.ValueSize

	dataBuf, err := fr.readData(dataSize)
	if err != nil {
		return nil, err
	}
	err = e.ParsePayload(dataBuf)
	if err != nil {
		return nil, err
	}

	crc := e.GetCrc(buf)
	if crc != e.crc {
		return nil, ErrCrc
	}

	return e, nil
}

// readData will read a byte array from disk by given size, and if the byte size less than given size in the first time it will read twice for the rest data.
func (fr *fileRecovery) readData(size uint32) (data []byte, err error) {
	data = make([]byte, size)
	if n, err := fr.reader.Read(data); err != nil {
		return nil, err
	} else {
		if uint32(n) < size {
			_, err := fr.reader.Read(data[n:])
			if err != nil {
				return nil, err
			}
		}
	}
	return data, nil
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
