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

	"github.com/edsrzf/mmap-go"
)

const mmapBlockSize int = 2 * MB

var (
	// ErrUnmappedMemory is returned when a function is called on unmapped memory
	ErrUnmappedMemory = errors.New("unmapped memory")

	// ErrIndexOutOfBound is returned when given offset out of mapped region
	ErrIndexOutOfBound = errors.New("offset out of mapped region")
)

// MMapRWManager represents the RWManager which using mmap.
type MMapRWManager struct {
	fd          *os.File
	path        string
	fdm         *fdManager
	segmentSize int64
}

// WriteAt copies data to mapped region from the b slice starting at
// given off and returns number of bytes copied to the mapped region.
func (mm *MMapRWManager) WriteAt(b []byte, off int64) (n int, err error) {
	lb := len(b)
	startOffset := mm.alignOffset(off)
	diff := off - startOffset
	curmmap, err := mm.getTargetMMap(startOffset, lb+int(diff), mmap.RDWR)
	if err != nil {
		return 0, err
	}
	n = copy(curmmap[diff:], b)
	err = curmmap.Unmap()
	return n, err
}

// ReadAt copies data to b slice from mapped region starting at
// given off and returns number of bytes copied to the b slice.
func (mm *MMapRWManager) ReadAt(b []byte, off int64) (n int, err error) {
	lb := len(b)
	startOffset := mm.alignOffset(off)
	diff := off - startOffset
	curmmap, err := mm.getTargetMMap(startOffset, lb+int(diff), mmap.RDONLY)
	if err != nil {
		return 0, err
	}
	n = copy(b, curmmap[diff:])
	err = curmmap.Unmap()
	return n, err
}

// Sync synchronizes the mapping's contents to the file's contents on disk.
func (mm *MMapRWManager) Sync() (err error) {
	return nil
}

// Release deletes the memory mapped region, flushes any remaining changes
func (mm *MMapRWManager) Release() (err error) {
	mm.fdm.reduceUsing(mm.path)
	return nil
}

func (mm *MMapRWManager) Size() int64 {
	return mm.segmentSize
}

// Close will remove the cache in the fdm of the specified path, and call the close method of the os of the file
func (mm *MMapRWManager) Close() (err error) {
	return mm.fdm.closeByPath(mm.path)
}

func (mm *MMapRWManager) getTargetMMap(offset int64, length int, prot int) (mmap.MMap, error) {
	return mmap.MapRegion(mm.fd, length, prot, 0, offset)
}

func (mm *MMapRWManager) alignOffset(offset int64) int64 {
	return offset / int64(mmapBlockSize) * int64(mmapBlockSize)
}
