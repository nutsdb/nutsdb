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

const (
	mmapBlockSize int64 = 32 * MB
)

var (
	// ErrUnmappedMemory is returned when a function is called on unmapped memory
	ErrUnmappedMemory = errors.New("unmapped memory")

	// ErrIndexOutOfBound is returned when given offset out of mapped region
	ErrIndexOutOfBound = errors.New("offset out of mapped region")
)

func newMMapRWManager(fd *os.File, path string, fdm *fdManager, segmentSize int64) *MMapRWManager {
	m := &MMapRWManager{
		fd:          fd,
		path:        path,
		fdm:         fdm,
		segmentSize: segmentSize,
	}
	m.mmaps = make([]mmap.MMap, segmentSize/mmapBlockSize)
	m.mmapsProt = make([]int, segmentSize/mmapBlockSize)
	for i := 0; i < len(m.mmaps); i++ {
		m.mmaps[i] = nil
		m.mmapsProt[i] = -1
	}
	return m
}

// MMapRWManager represents the RWManager which using mmap.
type MMapRWManager struct {
	fd          *os.File
	path        string
	fdm         *fdManager
	segmentSize int64

	mmaps     []mmap.MMap
	mmapsProt []int
}

// WriteAt copies data to mapped region from the b slice starting at
// given off and returns number of bytes copied to the mapped region.
func (mm *MMapRWManager) WriteAt(b []byte, off int64) (n int, err error) {
	lb := len(b)
	index := mm.selectMMap(off)
	diff := off - int64(index)*mmapBlockSize
	for n < lb {
		err = mm.prepareMMap(mmap.RDWR, index)
		if err != nil {
			break
		}
		curN := copy(mm.mmaps[index][diff:], b)
		b = b[curN:]
		n += curN
		diff = 0
		index++
	}
	return n, err
}

// ReadAt copies data to b slice from mapped region starting at
// given off and returns number of bytes copied to the b slice.
func (mm *MMapRWManager) ReadAt(b []byte, off int64) (n int, err error) {
	lb := len(b)
	index := mm.selectMMap(off)
	diff := off - int64(index)*mmapBlockSize
	for n < lb {
		err = mm.prepareMMap(mmap.COPY, index)
		if err != nil {
			break
		}
		curN := copy(b, mm.mmaps[index][diff:])
		b = b[curN:]
		n += curN
		diff = 0
		index++
	}
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

func (mm *MMapRWManager) selectMMap(offset int64) int {
	index := int64(0)
	for i := 0; i < len(mm.mmaps); i++ {
		if (mmapBlockSize*index) <= offset && offset < (mmapBlockSize*(index+1)) {
			return int(index)
		} else {
			index++
		}
	}
	return -1
}

func (mm *MMapRWManager) prepareMMap(prot int, index int) (err error) {
	if mm.mmaps[index] != nil {
		if mm.mmapsProt[index] != prot {
			mm.mmapsProt[index] = -1
			err = mm.mmaps[index].Unmap()
			if err != nil {
				return
			}
		} else {
			return
		}
	}
	mm.mmaps[index], err = mmap.MapRegion(mm.fd, int(mmapBlockSize), prot, 0, int64(index)*mmapBlockSize)
	if err != nil {
		return
	}
	mm.mmapsProt[index] = prot
	return
}
