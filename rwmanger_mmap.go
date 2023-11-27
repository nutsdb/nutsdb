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

	mmap "github.com/xujiajun/mmap-go"
)

// MMapRWManager represents the RWManager which using mmap.
type MMapRWManager struct {
	path        string
	fdm         *fdManager
	m           mmap.MMap
	segmentSize int64
}

var (
	// ErrUnmappedMemory is returned when a function is called on unmapped memory
	ErrUnmappedMemory = errors.New("unmapped memory")

	// ErrIndexOutOfBound is returned when given offset out of mapped region
	ErrIndexOutOfBound = errors.New("offset out of mapped region")
)

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

// Release deletes the memory mapped region, flushes any remaining changes
func (mm *MMapRWManager) Release() (err error) {
	mm.fdm.reduceUsing(mm.path)
	return mm.m.Unmap()
}

func (mm *MMapRWManager) Size() int64 {
	return mm.segmentSize
}

// Close will remove the cache in the fdm of the specified path, and call the close method of the os of the file
func (mm *MMapRWManager) Close() (err error) {
	return mm.fdm.closeByPath(mm.path)
}
