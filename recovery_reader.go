package nutsdb

import (
	"bufio"
	"encoding/binary"
	"io"
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
	buf := make([]byte, DataEntryHeaderSize)
	_, err = io.ReadFull(fr.reader, buf)
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
	if crc != e.crc {
		return nil, ErrCrc
	}

	return e, nil
}
