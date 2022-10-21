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

func newFileRecovery(path string) (fr *fileRecovery, err error) {
	fd, err := os.OpenFile(path, os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &fileRecovery{
		reader: bufio.NewReader(fd),
	}, nil
}

// readEntry will read a Entry from disk.
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
