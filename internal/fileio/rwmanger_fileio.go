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

package fileio

import (
	"os"
)

// FileIORWManager represents the RWManager which using standard I/O.
type FileIORWManager struct {
	Fd          *os.File
	Path        string
	Fdm         *FdManager
	SegmentSize int64
}

// WriteAt writes len(b) bytes to the File starting at byte offset off.
// `WriteAt` is a wrapper of the *File.WriteAt.
func (fm *FileIORWManager) WriteAt(b []byte, off int64) (n int, err error) {
	return fm.Fd.WriteAt(b, off)
}

// ReadAt reads len(b) bytes from the File starting at byte offset off.
// `ReadAt` is a wrapper of the *File.ReadAt.
func (fm *FileIORWManager) ReadAt(b []byte, off int64) (n int, err error) {
	return fm.Fd.ReadAt(b, off)
}

// Sync commits the current contents of the file to stable storage.
// Typically, this means flushing the file system's in-memory copy
// of recently written data to disk.
// `Sync` is a wrapper of the *File.Sync.
func (fm *FileIORWManager) Sync() (err error) {
	return fm.Fd.Sync()
}

// Release is a wrapper around the reduceUsing method
func (fm *FileIORWManager) Release() (err error) {
	fm.Fdm.ReduceUsing(fm.Path)
	return nil
}

func (fm *FileIORWManager) Size() int64 {
	return fm.SegmentSize
}

// Close will remove the cache in the fdm of the specified path, and call the close method of the os of the file
func (fm *FileIORWManager) Close() (err error) {
	return fm.Fdm.CloseByPath(fm.Path)
}
