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
	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/fileio"
)

const (
	// DataSuffix returns the data suffix
	DataSuffix = ".dat"
)

// DataFile records about data file information.
type DataFile struct {
	path       string
	fileID     int64
	writeOff   int64
	ActualSize int64
	rwManager  fileio.RWManager
}

// NewDataFile will return a new DataFile Object.
func NewDataFile(path string, rwManager fileio.RWManager) *DataFile {
	dataFile := &DataFile{
		path:      path,
		rwManager: rwManager,
	}
	return dataFile
}

// ReadData returns data at the given off(offset).
// payloadSize = bucketSize + keySize + valueSize
func (df *DataFile) ReadData(off int, payloadSize int64) (buf []byte, err error) {
	size := core.MaxEntryHeaderSize + payloadSize
	// Since core.MaxEntryHeaderSize + payloadSize may be larger than the actual entry size, it needs to be calculated
	if int64(off)+size > df.rwManager.Size() {
		size = df.rwManager.Size() - int64(off)
	}
	buf = make([]byte, size)

	_, err = df.rwManager.ReadAt(buf, int64(off))
	if err != nil {
		return nil, err
	}

	return buf, nil
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

// DataFileManager is an interface that provides methods to manage data files.
type DataFileManager interface {
	// GetDataFile will return a DataFile Object for read-write operations
	GetDataFile(path string, capacity int64) (datafile *DataFile, err error)

	// GetDataFileReadOnly will return a DataFile Object for read-only operations
	// This method skips file truncation to improve read performance
	GetDataFileReadOnly(path string, capacity int64) (datafile *DataFile, err error)

	// GetDataFileByID will return a DataFile Object by file ID
	GetDataFileByID(dir string, fileID int64, capacity int64) (*DataFile, error)

	// CloseByPath closes the data file by path
	CloseByPath(path string) error

	Close() error
}

type dataFileManagerImpl struct {
	fm *FileManager
}

func newDataFileManager(fm *FileManager) DataFileManager {
	return &dataFileManagerImpl{
		fm: fm,
	}
}

func (dfm *dataFileManagerImpl) GetDataFile(path string, capacity int64) (datafile *DataFile, err error) {
	rwmanager, err := dfm.fm.GetRWManager(path, capacity, dfm.fm.segmentSize, false)
	if err != nil {
		return nil, err
	}
	return NewDataFile(path, rwmanager), nil
}

func (dfm *dataFileManagerImpl) GetDataFileReadOnly(path string, capacity int64) (datafile *DataFile, err error) {
	rwmanager, err := dfm.fm.GetRWManager(path, capacity, dfm.fm.segmentSize, true)
	if err != nil {
		return nil, err
	}
	return NewDataFile(path, rwmanager), nil
}

func (dfm *dataFileManagerImpl) GetDataFileByID(dir string, fileID int64, capacity int64) (*DataFile, error) {
	path := getDataPath(fileID, dir)
	rwmanager, err := dfm.fm.GetRWManager(path, capacity, dfm.fm.segmentSize, false)
	if err != nil {
		return nil, err
	}
	return NewDataFile(path, rwmanager), nil
}

func (dfm *dataFileManagerImpl) CloseByPath(path string) error {
	return dfm.fm.fdm.CloseByPath(path)
}

func (dfm *dataFileManagerImpl) Close() error {
	return dfm.fm.Close()
}
