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
	"encoding/binary"
	"errors"
)

var (
	// ErrCrcZero is returned when crc is 0
	ErrCrcZero = errors.New("error crc is 0")

	// ErrCrc is returned when crc is error
	ErrCrc = errors.New("crc error")

	// ErrCapacity is returned when capacity is error.
	ErrCapacity = errors.New("capacity error")
)

const (
	// DataSuffix returns the data suffix
	DataSuffix = ".dat"

	// DataEntryHeaderSize returns the entry header size
	DataEntryHeaderSize = 42
)

// DataFile records about data file information.
type DataFile struct {
	path       string
	fileID     int64
	writeOff   int64
	ActualSize int64
	rwManager  RWManager
}

// NewDataFile will return a new DataFile Object.
func NewDataFile(path string, rwManager RWManager) *DataFile {
	dataFile := &DataFile{
		path:      path,
		rwManager: rwManager,
	}
	return dataFile
}

// ReadAt returns entry at the given off(offset).
func (df *DataFile) ReadAt(off int) (e *Entry, err error) {
	buf := make([]byte, DataEntryHeaderSize)

	if _, err := df.rwManager.ReadAt(buf, int64(off)); err != nil {
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

	off += DataEntryHeaderSize
	dataSize := meta.BucketSize + meta.KeySize + meta.ValueSize

	dataBuf := make([]byte, dataSize)
	_, err = df.rwManager.ReadAt(dataBuf, int64(off))
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

	return
}

// WriteAt copies data to mapped region from the b slice starting at
// given off and returns number of bytes copied to the mapped region.
func (df *DataFile) WriteAt(b []byte, off int64) (n int, err error) {
	return df.rwManager.WriteAt(b, off)
}

// Sync commits the current contents of the file to stable storage.
// Typically, this means flushing the file system's in-memory copy
// of recently written data to disk.
func (df *DataFile) Sync() (err error) {
	return df.rwManager.Sync()
}

// Close closes the RWManager.
// If RWManager is FileRWManager represents closes the File,
// rendering it unusable for I/O.
// If RWManager is a MMapRWManager represents Unmap deletes the memory mapped region,
// flushes any remaining changes.
func (df *DataFile) Close() (err error) {
	return df.rwManager.Close()
}

func (df *DataFile) Release() (err error) {
	return df.rwManager.Release()
}

// readMetaData returns MetaData at given buf slice.
func readMetaData(buf []byte) *MetaData {
	return &MetaData{
		Timestamp:  binary.LittleEndian.Uint64(buf[4:12]),
		KeySize:    binary.LittleEndian.Uint32(buf[12:16]),
		ValueSize:  binary.LittleEndian.Uint32(buf[16:20]),
		Flag:       binary.LittleEndian.Uint16(buf[20:22]),
		TTL:        binary.LittleEndian.Uint32(buf[22:26]),
		BucketSize: binary.LittleEndian.Uint32(buf[26:30]),
		Status:     binary.LittleEndian.Uint16(buf[30:32]),
		Ds:         binary.LittleEndian.Uint16(buf[32:34]),
		TxID:       binary.LittleEndian.Uint64(buf[34:42]),
	}
}
