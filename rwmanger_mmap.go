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
	"runtime"
	"sync"

	"github.com/edsrzf/mmap-go"
)

var (
	mmapBlockSize              = int64(os.Getpagesize()) * 4
	mmapRWManagerInstancesLock = sync.Mutex{}
	mmapRWManagerInstances     = make(map[string]*MMapRWManager)
	mmapLRUCacheCapacity       = 1024
)

var (
	// ErrUnmappedMemory is returned when a function is called on unmapped memory
	ErrUnmappedMemory = errors.New("unmapped memory")

	// ErrIndexOutOfBound is returned when given offset out of mapped region
	ErrIndexOutOfBound = errors.New("offset out of mapped region")
)

func getMMapRWManager(fd *os.File, path string, fdm *fdManager, segmentSize int64) *MMapRWManager {
	mmapRWManagerInstancesLock.Lock()
	defer mmapRWManagerInstancesLock.Unlock()
	mm, ok := mmapRWManagerInstances[path]
	if ok {
		return mm
	}
	mm = &MMapRWManager{
		fd:          fd,
		path:        path,
		fdm:         fdm,
		segmentSize: segmentSize,
		readCache:   NewLruCache(mmapLRUCacheCapacity),
		writeCache:  NewLruCache(mmapLRUCacheCapacity),
	}

	mmapRWManagerInstances[path] = mm
	return mm

}

// MMapRWManager represents the RWManager which using mmap.
// different with FileIORWManager, we can only allow one MMapRWManager
// for one datafile, so we need to do some concurrency safety control.
type MMapRWManager struct {
	fd          *os.File
	path        string
	fdm         *fdManager
	segmentSize int64

	readCache  *LRUCache
	writeCache *LRUCache
}

// WriteAt copies data to mapped region from the b slice starting at
// given off and returns number of bytes copied to the mapped region.
func (mm *MMapRWManager) WriteAt(b []byte, off int64) (n int, err error) {
	if off >= int64(mm.segmentSize) || off < 0 {
		return 0, ErrIndexOutOfBound
	}
	lb := len(b)
	curOffset := mm.alignedOffset(off)
	diff := off - curOffset
	for ; n < lb && curOffset < mm.segmentSize; curOffset += mmapBlockSize {
		data, err := mm.accessMMap(mm.writeCache, curOffset, mmap.RDWR)
		if err != nil {
			return n, err
		}
		data.mut.Lock()
		defer data.mut.Unlock()
		n += copy(data.data[diff:mmapBlockSize], b[n:])
		diff = 0
	}
	return n, err
}

// ReadAt copies data to b slice from mapped region starting at
// given off and returns number of bytes copied to the b slice.
func (mm *MMapRWManager) ReadAt(b []byte, off int64) (n int, err error) {
	if off >= int64(mm.segmentSize) || off < 0 {
		return 0, ErrIndexOutOfBound
	}
	lb := len(b)
	curOffset := mm.alignedOffset(off)
	diff := off - curOffset
	for ; n < lb && curOffset < mm.segmentSize; curOffset += mmapBlockSize {
		data, err := mm.accessMMap(mm.readCache, curOffset, mmap.RDONLY)
		if err != nil {
			return n, err
		}
		data.mut.RLock()
		defer data.mut.RUnlock()
		n += copy(b[n:], data.data[diff:mmapBlockSize])
		diff = 0
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
	return
}

func (mm *MMapRWManager) Size() int64 {
	return mm.segmentSize
}

// Close will remove the cache in the fdm of the specified path, and call the close method of the os of the file
func (mm *MMapRWManager) Close() (err error) {
	return mm.fdm.closeByPath(mm.path)
}

func (mm *MMapRWManager) alignedOffset(offset int64) int64 {
	return offset - (offset & (mmapBlockSize - 1))
}

// accessMMap access the MMap data. If for this block the mmap data is not mmapped, will add
// it into cache.
func (mm *MMapRWManager) accessMMap(cache *LRUCache, offset int64, prot int) (data *mmapData, err error) {
	item := cache.Get(offset)
	if item == nil {
		newItem, err := newMMapData(mm.fd, offset, prot)
		if err != nil {
			return nil, err
		}
		cache.Add(offset, newItem)
		item = newItem
	}
	data = item.(*mmapData)
	return
}

// mmapData is a struct to control the lifetime and access level of mmap.MMap
type mmapData struct {
	data   mmap.MMap
	offset int64
	mut    sync.RWMutex
}

func newMMapData(fd *os.File, offset int64, prot int) (md *mmapData, err error) {
	md = &mmapData{
		offset: offset,
	}
	md.data, err = mmap.MapRegion(fd, int(mmapBlockSize), prot, 0, offset)
	if err != nil {
		return nil, err
	}
	runtime.SetFinalizer(md, (*mmapData).Close)
	return md, nil
}

func (md *mmapData) Close() (err error) {
	return md.data.Unmap()
}
