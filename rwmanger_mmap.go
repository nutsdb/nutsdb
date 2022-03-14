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
	"os"
	"path/filepath"

	mmap "github.com/xujiajun/mmap-go"
)

// MMapRWManager represents the RWManager which using mmap.
type MMapRWManager struct {
	m mmap.MMap
}

var (
	// ErrUnmappedMemory is returned when a function is called on unmapped memory
	ErrUnmappedMemory = errors.New("unmapped memory")

	// ErrIndexOutOfBound is returned when given offset out of mapped region
	ErrIndexOutOfBound = errors.New("offset out of mapped region")
)

// NewMMapRWManager returns a newly initialized MMapRWManager.
func NewMMapRWManager(path string, capacity int64) (*MMapRWManager, error) {
	f, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()


	err = Truncate(path, capacity, f)
	if err != nil {
		return nil, err
	}

	m, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	return &MMapRWManager{m: m}, nil
}

// WriteAt copies data to mapped region from the b slice starting at
// given off and returns number of bytes copied to the mapped region.
func (mm *MMapRWManager) WriteAt(b []byte, off int64) (n int, err error) {
	if mm.m == nil {
		return 0, ErrUnmappedMemory
	} else if off >= int64(len(mm.m)) || off < 0 {
		return 0, ErrIndexOutOfBound
	}

	return copy(mm.m[off:], b), nil
}

// ReadAt copies data to b slice from mapped region starting at
// given off and returns number of bytes copied to the b slice.
func (mm *MMapRWManager) ReadAt(b []byte, off int64) (n int, err error) {
	if mm.m == nil {
		return 0, ErrUnmappedMemory
	} else if off >= int64(len(mm.m)) || off < 0 {
		return 0, ErrIndexOutOfBound
	}

	return copy(b, mm.m[off:]), nil
}

// Sync synchronizes the mapping's contents to the file's contents on disk.
func (mm *MMapRWManager) Sync() (err error) {
	return mm.m.Flush()
}

//Close deletes the memory mapped region, flushes any remaining changes
func (mm *MMapRWManager) Close() (err error) {
	return mm.m.Unmap()
}
