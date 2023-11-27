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
	size   int64
}

func newFileRecovery(path string, bufSize int) (fr *fileRecovery, err error) {
	fd, err := os.OpenFile(path, os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	bufSize = calBufferSize(bufSize)
	fileInfo, err := fd.Stat()
	if err != nil {
		return nil, err
	}
	return &fileRecovery{
		fd:     fd,
		reader: bufio.NewReaderSize(fd, bufSize),
		size:   fileInfo.Size(),
	}, nil
}

// readEntry will read an Entry from disk.
func (fr *fileRecovery) readEntry(off int64) (e *Entry, err error) {
	var size int64 = MaxEntryHeaderSize
	// Since MaxEntryHeaderSize may be larger than the actual Header, it needs to be calculated
	if off+size > fr.size {
		size = fr.size - off
	}

	buf := make([]byte, size)
	_, err = fr.fd.Seek(off, 0)
	if err != nil {
		return nil, err
	}

	_, err = fr.fd.Read(buf)
	if err != nil {
		return nil, err
	}

	e = new(Entry)
	headerSize, err := e.ParseMeta(buf)
	if err != nil {
		return nil, err
	}

	if e.IsZero() {
		return nil, nil
	}

	headerBuf := buf[:headerSize]
	remainingBuf := buf[headerSize:]

	payloadSize := e.Meta.PayloadSize()
	dataBuf := make([]byte, payloadSize)
	excessSize := size - headerSize

	if payloadSize <= excessSize {
		copy(dataBuf, remainingBuf[:payloadSize])
	} else {
		copy(dataBuf, remainingBuf)
		_, err := fr.fd.Read(dataBuf[excessSize:])
		if err != nil {
			return nil, err
		}
	}

	err = e.ParsePayload(dataBuf)
	if err != nil {
		return nil, err
	}

	crc := e.GetCrc(headerBuf)
	if crc != e.Meta.Crc {
		return nil, ErrCrc
	}

	return e, nil
}

func (fr *fileRecovery) readBucket() (b *Bucket, err error) {
	buf := make([]byte, BucketMetaSize)
	_, err = io.ReadFull(fr.reader, buf)
	if err != nil {
		return nil, err
	}
	meta := new(BucketMeta)
	meta.Decode(buf)
	bucket := new(Bucket)
	bucket.Meta = meta
	dataBuf := make([]byte, meta.Size)
	_, err = io.ReadFull(fr.reader, dataBuf)
	if err != nil {
		return nil, err
	}
	err = bucket.Decode(dataBuf)
	if err != nil {
		return nil, err
	}

	if bucket.GetCRC(buf, dataBuf) != bucket.Meta.Crc {
		return nil, ErrBucketCrcInvalid
	}

	return bucket, nil
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
