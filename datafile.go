// Copyright 2019 The nutsdb Authors. All rights reserved.
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
	"encoding/binary"
	"errors"
	"os"
	"syscall"

	"github.com/grandecola/mmap"
)

var (
	// ErrCrcZero is returned when crc is 0
	ErrCrcZero = errors.New("error crc is 0")

	// ErrCrc is returned when crc is error
	ErrCrc = errors.New(" crc error")
)

const (
	// DataSuffix returns the data suffix
	DataSuffix = ".dat"

	// DataEntryHeaderSize returns the entry header size
	DataEntryHeaderSize = 42
)

// DataFile records about data file information.
type DataFile struct {
	fd         *os.File
	m          mmap.IMmap
	path       string
	fileID     int64
	writeOff   int64
	ActualSize int64
}

// NewDataFile returns a newly initialized DataFile object.
func NewDataFile(path string, capacity int64) (*DataFile, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fileInfo, _ := os.Stat(path)
	if fileInfo.Size() < capacity {
		if err := f.Truncate(capacity); err != nil {
			return nil, err
		}
	}

	m, err := mmap.NewSharedFileMmap(f, 0, int(capacity), syscall.PROT_READ|syscall.PROT_WRITE)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		fd:         f,
		m:          m,
		path:       path,
		writeOff:   0,
		ActualSize: 0,
	}, nil
}

// ReadAt returns entry at the given off(offset).
func (df *DataFile) ReadAt(off int) (e *Entry, err error) {
	buf := make([]byte, DataEntryHeaderSize)
	if _, err := df.m.Read(buf, off); err != nil {
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

	// read bucket
	off += DataEntryHeaderSize
	bucketBuf := make([]byte, meta.bucketSize)
	_, err = df.m.Read(bucketBuf, off)
	if err != nil {
		return nil, err
	}

	e.Meta.bucket = bucketBuf

	// read key
	off += int(meta.bucketSize)
	keyBuf := make([]byte, meta.keySize)

	_, err = df.m.Read(keyBuf, off)
	if err != nil {
		return nil, err
	}
	e.Key = keyBuf

	// read value
	off += int(meta.keySize)
	valBuf := make([]byte, meta.valueSize)
	_, err = df.m.Read(valBuf, off)
	if err != nil {
		return nil, err
	}
	e.Value = valBuf

	crc := e.GetCrc(buf)
	if crc != e.crc {
		return nil, ErrCrc
	}

	return
}

// WriteAt copies data to mapped region from the b slice starting at
// given off and returns number of bytes copied to the mapped region.
func (df *DataFile) WriteAt(b []byte, off int64) (n int, err error) {
	return df.m.Write(b, int(off))
}

// readMetaData returns MetaData at given buf slice.
func readMetaData(buf []byte) *MetaData {
	return &MetaData{
		timestamp:  binary.LittleEndian.Uint64(buf[4:12]),
		keySize:    binary.LittleEndian.Uint32(buf[12:16]),
		valueSize:  binary.LittleEndian.Uint32(buf[16:20]),
		Flag:       binary.LittleEndian.Uint16(buf[20:22]),
		TTL:        binary.LittleEndian.Uint32(buf[22:26]),
		bucketSize: binary.LittleEndian.Uint32(buf[26:30]),
		status:     binary.LittleEndian.Uint16(buf[30:32]),
		ds:         binary.LittleEndian.Uint16(buf[32:34]),
		txID:       binary.LittleEndian.Uint64(buf[34:42]),
	}
}
