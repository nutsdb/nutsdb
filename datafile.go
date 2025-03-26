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
	"errors"

	"github.com/nutsdb/nutsdb/internal/nutspath"
)

var (
	// ErrCrc is returned when crc is error
	ErrCrc = errors.New("crc error")

	// ErrCapacity is returned when capacity is error.
	ErrCapacity = errors.New("capacity error")

	ErrEntryZero = errors.New("entry is zero ")
)

const (
	// DataSuffix returns the data suffix
	DataSuffix = ".dat"
)

// DataFile records about data file information.
type DataFile struct {
	path       nutspath.Path
	fileID     int64
	writeOff   int64
	ActualSize int64
	rwManager  RWManager
}

// NewDataFile will return a new DataFile Object.
func NewDataFile(path nutspath.Path, rwManager RWManager) *DataFile {
	dataFile := &DataFile{
		path:      path,
		rwManager: rwManager,
	}
	return dataFile
}

// ReadEntry returns entry at the given off(offset).
// payloadSize = bucketSize + keySize + valueSize
func (df *DataFile) ReadEntry(off int, payloadSize int64) (e *Entry, err error) {
	size := MaxEntryHeaderSize + payloadSize
	// Since MaxEntryHeaderSize + payloadSize may be larger than the actual entry size, it needs to be calculated
	if int64(off)+size > df.rwManager.Size() {
		size = df.rwManager.Size() - int64(off)
	}
	buf := make([]byte, size)

	if _, err := df.rwManager.ReadAt(buf, int64(off)); err != nil {
		return nil, err
	}

	e = new(Entry)
	headerSize, err := e.ParseMeta(buf)
	if err != nil {
		return nil, err
	}

	// Remove the content after the Header
	buf = buf[:int(headerSize+payloadSize)]

	if e.IsZero() {
		return nil, ErrEntryZero
	}

	if err := e.checkPayloadSize(payloadSize); err != nil {
		return nil, err
	}

	err = e.ParsePayload(buf[headerSize:])
	if err != nil {
		return nil, err
	}

	crc := e.GetCrc(buf[:headerSize])
	if crc != e.Meta.Crc {
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
