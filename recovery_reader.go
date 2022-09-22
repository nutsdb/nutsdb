package nutsdb

import (
	"bufio"
	"encoding/binary"
	"os"
)

//fileRecovery use bufio.Reader to read entry
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

func (fr *fileRecovery) readEntry() (e *Entry, err error) {
	buf := make([]byte, DataEntryHeaderSize)

	if _, err := fr.reader.Read(buf); err != nil {
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
	_, err = fr.reader.Read(dataBuf)
	if err != nil {
		return nil, err
	}

	bucketLowBound := 0
	bucketHighBound := meta.BucketSize
	keyLowBound := bucketHighBound
	keyHighBound := meta.BucketSize + meta.KeySize
	valueLowBound := keyHighBound
	valueHighBound := dataSize

	// parse bucket
	e.Meta.Bucket = dataBuf[bucketLowBound:bucketHighBound]
	// parse key
	e.Key = dataBuf[keyLowBound:keyHighBound]
	// parse value
	e.Value = dataBuf[valueLowBound:valueHighBound]

	crc := e.GetCrc(buf)
	if crc != e.crc {
		return nil, ErrCrc
	}

	return
}
