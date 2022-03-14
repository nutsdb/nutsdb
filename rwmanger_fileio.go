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
	"os"
	"path/filepath"
)

// FileIORWManager represents the RWManager which using standard I/O.
type FileIORWManager struct {
	fd *os.File
}

// NewFileIORWManager returns a newly initialized FileIORWManager.
func NewFileIORWManager(path string, capacity int64) (*FileIORWManager, error) {
	fd, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	err = Truncate(path, capacity, fd)
	if err != nil {
		return nil, err
	}

	return &FileIORWManager{fd: fd}, nil
}

// WriteAt writes len(b) bytes to the File starting at byte offset off.
// `WriteAt` is a wrapper of the *File.WriteAt.
func (fm *FileIORWManager) WriteAt(b []byte, off int64) (n int, err error) {
	return fm.fd.WriteAt(b, off)
}

// ReadAt reads len(b) bytes from the File starting at byte offset off.
// `ReadAt` is a wrapper of the *File.ReadAt.
func (fm *FileIORWManager) ReadAt(b []byte, off int64) (n int, err error) {
	return fm.fd.ReadAt(b, off)
}

// Sync commits the current contents of the file to stable storage.
// Typically, this means flushing the file system's in-memory copy
// of recently written data to disk.
// `Sync` is a wrapper of the *File.Sync.
func (fm *FileIORWManager) Sync() (err error) {
	return fm.fd.Sync()
}

// Close closes the File, rendering it unusable for I/O.
// On files that support SetDeadline, any pending I/O operations will
// be canceled and return immediately with an error.
// `Close` is a wrapper of the *File.Close.
func (fm *FileIORWManager) Close() (err error) {
	return fm.fd.Close()
}
