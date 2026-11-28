package nutsdb

import (
	"bufio"
	"errors"
	"io"
	"os"

	"github.com/nutsdb/nutsdb/internal/core"
)

// fileRecovery use bufio.Reader to read entry
type fileRecovery struct {
	fd     *os.File
	reader *bufio.Reader
	size   int64
	closed bool
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
		closed: false,
	}, nil
}

// readEntry will read an Entry from disk.
func (fr *fileRecovery) readEntry(off int64) (e *core.Entry, err error) {
	e, _, err = fr.readEntryWithSize(off)
	return e, err
}

// readEntryWithSize reads an Entry from disk and also reports the full entry byte
// length (headerSize + payloadSize) when the header decoded cleanly, so callers
// can distinguish a torn tail from mid-file corruption.
func (fr *fileRecovery) readEntryWithSize(off int64) (e *core.Entry, entryLen int64, err error) {
	if off >= fr.size {
		return nil, 0, io.EOF
	}
	var size int64 = core.MaxEntryHeaderSize
	// Since core.MaxEntryHeaderSize may be larger than the actual Header, it needs to be calculated
	if off+size > fr.size {
		size = fr.size - off
	}

	buf := make([]byte, size)
	_, err = fr.fd.Seek(off, 0)
	if err != nil {
		return nil, 0, err
	}

	nRead, err := fr.fd.Read(buf)
	if err != nil {
		return nil, 0, err
	}
	if int64(nRead) < size {
		buf = buf[:nRead]
		size = int64(nRead)
	}

	e = new(core.Entry)
	headerSize, err := e.ParseMeta(buf)
	if err != nil {
		return nil, 0, err
	}

	if e.IsZero() {
		return nil, 0, nil
	}

	payloadSize := e.Meta.PayloadSize()
	// Bound payloadSize against the remaining file bytes before allocating so a
	// corrupted varint in a torn header cannot trigger an OOM panic.
	if payloadSize < 0 || payloadSize > fr.size-off-headerSize {
		return nil, 0, io.ErrUnexpectedEOF
	}
	entryLen = headerSize + payloadSize

	headerBuf := buf[:headerSize]
	remainingBuf := buf[headerSize:]

	dataBuf := make([]byte, payloadSize)
	excessSize := size - headerSize

	if payloadSize <= excessSize {
		copy(dataBuf, remainingBuf[:payloadSize])
	} else {
		copy(dataBuf, remainingBuf)
		if _, err := io.ReadFull(fr.fd, dataBuf[excessSize:]); err != nil {
			return nil, entryLen, err
		}
	}

	err = e.ParsePayload(dataBuf)
	if err != nil {
		return nil, entryLen, err
	}

	crc := e.GetCrc(headerBuf)
	if crc != e.Meta.Crc {
		return nil, entryLen, ErrCrc
	}

	return e, entryLen, nil
}

// isTornTailAfter reports whether no further non-zero record header exists at
// or after nextOff, distinguishing an incomplete active-file tail from mid-file corruption.
func (fr *fileRecovery) isTornTailAfter(nextOff int64) (bool, error) {
	if nextOff >= fr.size {
		return true, nil
	}
	remain := fr.size - nextOff
	if remain < core.MinEntryHeaderSize {
		return true, nil
	}
	size := int64(core.MaxEntryHeaderSize)
	if remain < size {
		size = remain
	}
	buf := make([]byte, size)
	n, err := fr.fd.ReadAt(buf, nextOff)
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	if int64(n) < core.MinEntryHeaderSize {
		return true, nil
	}
	var probe core.Entry
	if _, perr := probe.ParseMeta(buf[:n]); perr != nil {
		return true, nil
	}
	return probe.IsZero(), nil
}

func (fr *fileRecovery) readBucket() (b *core.Bucket, err error) {
	buf := make([]byte, core.BucketMetaSize)
	_, err = io.ReadFull(fr.reader, buf)
	if err != nil {
		return nil, err
	}
	meta := new(core.BucketMeta)
	meta.Decode(buf)
	bucket := new(core.Bucket)
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
		return nil, core.ErrBucketCrcInvalid
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

func (fr *fileRecovery) zeroTail(off int64) error {
	if off < 0 || off >= fr.size {
		return nil
	}
	const chunkSize = 32 * 1024
	buf := make([]byte, chunkSize)
	zeros := make([]byte, chunkSize)
	wrote := false
	// Zero chunks from EOF back to off so a crash mid-zero never clears off
	// while leaving later garbage past chunk 0 untouched on the next Open.
	for end := fr.size; end > off; {
		start := end - chunkSize
		if start < off {
			start = off
		}
		n := int(end - start)
		nr, err := fr.fd.ReadAt(buf[:n], start)
		if nr > 0 {
			for _, b := range buf[:nr] {
				if b != 0 {
					if _, werr := fr.fd.WriteAt(zeros[:nr], start); werr != nil {
						return werr
					}
					wrote = true
					break
				}
			}
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		end = start
	}
	if wrote {
		return fr.fd.Sync()
	}
	return nil
}

func (fr *fileRecovery) release() error {
	if fr.closed {
		return nil
	}
	fr.closed = true
	return fr.fd.Close()
}
