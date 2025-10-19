// Copyright 2019 The nutsdb Author. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nutsdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/nutsdb/nutsdb/internal/utils"
	"github.com/xujiajun/utils/strconv2"
)

const (
	// HintSuffix returns the hint file suffix
	HintSuffix = ".hint"
)

var (
	// ErrHintFileCorrupted is returned when hint file is corrupted
	ErrHintFileCorrupted = errors.New("hint file is corrupted")
	// ErrHintFileEntryInvalid is returned when hint entry is invalid
	ErrHintFileEntryInvalid = errors.New("hint file entry is invalid")
)

// getHintPath returns the hint file path for the given file ID and directory
func getHintPath(fid int64, dir string) string {
	separator := string(filepath.Separator)
	if IsMergeFile(fid) {
		seq := GetMergeSeq(fid)
		return dir + separator + fmt.Sprintf("merge_%d%s", seq, HintSuffix)
	}
	return dir + separator + strconv2.Int64ToStr(fid) + HintSuffix
}

// HintEntry represents an entry in the hint file
type HintEntry struct {
	BucketId  uint64
	KeySize   uint32
	ValueSize uint32
	Timestamp uint64
	TTL       uint32
	Flag      uint16
	Status    uint16
	Ds        uint16
	DataPos   uint64
	FileID    int64
	Key       []byte
}

func newHintEntryFromEntry(entry *Entry, fileID int64, offset uint64) *HintEntry {
	meta := entry.Meta
	return &HintEntry{
		BucketId:  meta.BucketId,
		KeySize:   meta.KeySize,
		ValueSize: meta.ValueSize,
		Timestamp: meta.Timestamp,
		TTL:       meta.TTL,
		Flag:      meta.Flag,
		Status:    meta.Status,
		Ds:        meta.Ds,
		DataPos:   offset,
		FileID:    fileID,
		Key:       append([]byte(nil), entry.Key...),
	}
}

// Size returns the size of the hint entry
func (h *HintEntry) Size() int64 {
	keySize := len(h.Key)

	size := 0
	size += utils.UvarintSize(h.BucketId)
	size += utils.UvarintSize(uint64(keySize))
	size += utils.UvarintSize(uint64(h.ValueSize))
	size += utils.UvarintSize(h.Timestamp)
	size += utils.UvarintSize(uint64(h.TTL))
	size += utils.UvarintSize(uint64(h.Flag))
	size += utils.UvarintSize(uint64(h.Status))
	size += utils.UvarintSize(uint64(h.Ds))
	size += utils.UvarintSize(h.DataPos)
	size += utils.VarintSize(h.FileID)
	size += keySize

	return int64(size)
}

// Encode encodes the hint entry to bytes
func (h *HintEntry) Encode() []byte {
	keySize := len(h.Key)
	h.KeySize = uint32(keySize)

	buf := make([]byte, h.Size())
	index := 0

	index += binary.PutUvarint(buf[index:], h.BucketId)
	index += binary.PutUvarint(buf[index:], uint64(keySize))
	index += binary.PutUvarint(buf[index:], uint64(h.ValueSize))
	index += binary.PutUvarint(buf[index:], h.Timestamp)
	index += binary.PutUvarint(buf[index:], uint64(h.TTL))
	index += binary.PutUvarint(buf[index:], uint64(h.Flag))
	index += binary.PutUvarint(buf[index:], uint64(h.Status))
	index += binary.PutUvarint(buf[index:], uint64(h.Ds))
	index += binary.PutUvarint(buf[index:], h.DataPos)
	index += binary.PutVarint(buf[index:], h.FileID)

	copy(buf[index:], h.Key)
	index += keySize

	return buf[:index]
}

// Decode decodes the hint entry from bytes
func (h *HintEntry) Decode(buf []byte) error {
	if len(buf) == 0 {
		return ErrHintFileEntryInvalid
	}

	index := 0

	bucketId, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return ErrHintFileCorrupted
	}
	index += n
	h.BucketId = bucketId

	keySize, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return ErrHintFileCorrupted
	}
	index += n
	h.KeySize = uint32(keySize)

	valueSize, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return ErrHintFileCorrupted
	}
	index += n
	h.ValueSize = uint32(valueSize)

	timestamp, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return ErrHintFileCorrupted
	}
	index += n
	h.Timestamp = timestamp

	ttl, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return ErrHintFileCorrupted
	}
	index += n
	h.TTL = uint32(ttl)

	flag, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return ErrHintFileCorrupted
	}
	index += n
	h.Flag = uint16(flag)

	status, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return ErrHintFileCorrupted
	}
	index += n
	h.Status = uint16(status)

	ds, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return ErrHintFileCorrupted
	}
	index += n
	h.Ds = uint16(ds)

	dataPos, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return ErrHintFileCorrupted
	}
	index += n
	h.DataPos = dataPos

	fileID, n, err := decodeCompatInt64(buf[index:])
	if err != nil {
		return err
	}
	index += n
	h.FileID = fileID

	if len(buf) < index+int(h.KeySize) {
		return ErrHintFileCorrupted
	}

	h.Key = make([]byte, h.KeySize)
	copy(h.Key, buf[index:index+int(h.KeySize)])

	return nil
}

// HintFileReader is used to read hint files
type HintFileReader struct {
	file   *os.File
	reader *bufio.Reader
}

// Open opens a hint file for reading
func (r *HintFileReader) Open(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}

	r.file = file
	r.reader = bufio.NewReader(file)

	return nil
}

// Read reads a hint entry from the file
func (r *HintFileReader) Read() (*HintEntry, error) {
	if r.reader == nil {
		return nil, ErrHintFileEntryInvalid
	}

	entry := &HintEntry{}
	fieldsRead := 0

	readUvarint := func() (uint64, error) {
		val, err := binary.ReadUvarint(r.reader)
		if err != nil {
			if err == io.EOF {
				if fieldsRead == 0 {
					return 0, io.EOF
				}
				return 0, ErrHintFileCorrupted
			}
			if err == io.ErrUnexpectedEOF {
				return 0, ErrHintFileCorrupted
			}
			return 0, err
		}
		fieldsRead++
		return val, nil
	}

	var err error
	if entry.BucketId, err = readUvarint(); err != nil {
		return nil, err
	}

	keySize, err := readUvarint()
	if err != nil {
		return nil, err
	}
	entry.KeySize = uint32(keySize)

	valueSize, err := readUvarint()
	if err != nil {
		return nil, err
	}
	entry.ValueSize = uint32(valueSize)

	if entry.Timestamp, err = readUvarint(); err != nil {
		return nil, err
	}

	ttl, err := readUvarint()
	if err != nil {
		return nil, err
	}
	entry.TTL = uint32(ttl)

	flag, err := readUvarint()
	if err != nil {
		return nil, err
	}
	entry.Flag = uint16(flag)

	status, err := readUvarint()
	if err != nil {
		return nil, err
	}
	entry.Status = uint16(status)

	ds, err := readUvarint()
	if err != nil {
		return nil, err
	}
	entry.Ds = uint16(ds)

	if entry.DataPos, err = readUvarint(); err != nil {
		return nil, err
	}

	fileID, err := readCompatInt64(r.reader, &fieldsRead)
	if err != nil {
		return nil, err
	}
	entry.FileID = fileID

	if entry.KeySize > 0 {
		entry.Key = make([]byte, entry.KeySize)
		if _, err := io.ReadFull(r.reader, entry.Key); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil, ErrHintFileCorrupted
			}
			return nil, err
		}
	}

	return entry, nil
}

func decodeCompatInt64(buf []byte) (int64, int, error) {
	if len(buf) == 0 {
		return 0, 0, ErrHintFileCorrupted
	}
	fileID, n := binary.Varint(buf)
	if n > 0 {
		var encoded [binary.MaxVarintLen64]byte
		m := binary.PutVarint(encoded[:], fileID)
		if n == m && bytes.Equal(buf[:n], encoded[:m]) {
			return fileID, n, nil
		}
	}

	uval, n2 := binary.Uvarint(buf)
	if n2 > 0 {
		return int64(uval), n2, nil
	}

	return 0, 0, ErrHintFileCorrupted
}

func readCompatInt64(r *bufio.Reader, fieldsRead *int) (int64, error) {
	var scratch [binary.MaxVarintLen64]byte
	var i int

	for {
		b, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				if i == 0 {
					if fieldsRead != nil && *fieldsRead == 0 {
						return 0, io.EOF
					}
					return 0, ErrHintFileCorrupted
				}
				return 0, ErrHintFileCorrupted
			}
			if err == io.ErrUnexpectedEOF {
				return 0, ErrHintFileCorrupted
			}
			return 0, err
		}

		scratch[i] = b
		i++

		if b < 0x80 || i == len(scratch) {
			break
		}
	}

	if i == len(scratch) && scratch[i-1]&0x80 != 0 {
		return 0, ErrHintFileCorrupted
	}

	val, n, err := decodeCompatInt64(scratch[:i])
	if err != nil {
		return 0, err
	}
	if n != i {
		return 0, ErrHintFileCorrupted
	}

	if fieldsRead != nil {
		(*fieldsRead)++
	}

	return val, nil
}

// Close closes the hint file
func (r *HintFileReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// HintFileWriter is used to write hint files
type HintFileWriter struct {
	file   *os.File
	writer *bufio.Writer
}

// Create creates a hint file for writing
func (w *HintFileWriter) Create(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	w.file = file
	w.writer = bufio.NewWriter(file)

	return nil
}

// Write writes a hint entry to the file
func (w *HintFileWriter) Write(entry *HintEntry) error {
	if entry == nil {
		return ErrHintFileEntryInvalid
	}

	data := entry.Encode()
	_, err := w.writer.Write(data)
	return err
}

// Sync flushes the buffer and syncs the file to disk
func (w *HintFileWriter) Sync() error {
	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return err
		}
	}

	if w.file != nil {
		return w.file.Sync()
	}

	return nil
}

// Close closes the hint file
func (w *HintFileWriter) Close() error {
	var err error

	if w.writer != nil {
		if flushErr := w.writer.Flush(); flushErr != nil {
			err = flushErr
		}
	}

	if w.file != nil {
		if closeErr := w.file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}

	return err
}
