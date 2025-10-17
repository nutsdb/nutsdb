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
	"path"
	"testing"

	"github.com/nutsdb/nutsdb/internal/fileio"
	"github.com/stretchr/testify/require"

	"github.com/edsrzf/mmap-go"
)

func TestRWManager_MMap_Release(t *testing.T) {
	filePath := "/tmp/foo_rw_MMap"
	fdm := newFileManager(MMap, 8*MB, 0.5, 8*MB)
	rwmanager, err := fdm.getMMapRWManager(filePath, 8*MB, 8*MB)
	if err != nil {
		t.Error("err TestRWManager_MMap_Release getMMapRWManager")
	}

	b := []byte("hello")
	off := int64(0)
	_, err = rwmanager.WriteAt(b, off)
	if err != nil {
		t.Error("err TestRWManager_MMap_Release WriteAt")
	}

	if !rwmanager.IsActive() {
		t.Error("err TestRWManager_MMap_Release FdInfo:using not correct")
	}

	err = rwmanager.Release()
	if err != nil {
		t.Error("err TestRWManager_MMap_Release Release")
	}

	if rwmanager.IsActive() {
		t.Error("err TestRWManager_MMap_Release Release Failed")
	}
}

func (rwmanager *MMapRWManager) IsActive() bool {
	return rwmanager.fdm.Cache[rwmanager.path].Using() != 0
}

func TestRWManager_MMap_WriteAt(t *testing.T) {
	filePath := "/tmp/foo_rw_filemmap"
	maxFdNums := 1024
	cleanThreshold := 0.5
	var fdm = fileio.NewFdm(maxFdNums, cleanThreshold)

	fd, err := fdm.GetFd(filePath)
	if err != nil {
		require.NoError(t, err)
	}
	defer os.Remove(fd.Name())

	err = Truncate(filePath, 8*MB, fd)
	if err != nil {
		require.NoError(t, err)

	}

	mmManager := getMMapRWManager(fd, filePath, fdm, 8*MB)
	b := []byte("test write at")
	off := int64(3)
	n, err := mmManager.WriteAt(b, off)
	if err != nil {
		require.NoError(t, err)
	}
	require.Equal(t, len(b), n)
	m, err := mmap.Map(fd, mmap.RDWR, 0)
	if err != nil {
		require.NoError(t, err)
	}
	require.Equal(t, append([]byte{0, 0, 0}, b...), []byte(m[:off+int64(len(b))]))
}

func TestRWManager_MMap_WriteAt_NotEnoughData(t *testing.T) {
	filePath := path.Join(t.TempDir(), "rw_mmap")
	maxFdNums := 1024
	cleanThreshold := 0.5
	var fdm = fileio.NewFdm(maxFdNums, cleanThreshold)

	fd, err := fdm.GetFd(filePath)
	require.NoError(t, err)

	defer os.Remove(fd.Name())

	err = Truncate(filePath, 8*MB, fd)
	require.NoError(t, err)

	m, err := mmap.Map(fd, mmap.RDWR, 0)
	require.NoError(t, err)

	b := []byte("test-data-message")
	off := int64(8*MB - 4)
	copy(m[off:], b)

	mmManager := getMMapRWManager(fd, filePath, fdm, 8*MB)

	n, err := mmManager.WriteAt(b, off)
	require.NoError(t, err)
	require.Equal(t, 4, n)

	data := make([]byte, n)
	copy(data, m[off:])
	require.Equal(t, b[:n], data)
}

func TestRWManager_MMap_ReadAt_CrossBlock(t *testing.T) {
	filePath := path.Join(t.TempDir(), "rw_mmap")
	maxFdNums := 1024
	cleanThreshold := 0.5
	var fdm = fileio.NewFdm(maxFdNums, cleanThreshold)

	fd, err := fdm.GetFd(filePath)
	require.NoError(t, err)

	defer os.Remove(fd.Name())

	err = Truncate(filePath, 8*MB, fd)
	require.NoError(t, err)

	m, err := mmap.Map(fd, mmap.RDWR, 0)
	require.NoError(t, err)

	b := []byte("test-data-message")
	off := int64(mmapBlockSize - 5)
	copy(m[off:], b)

	mmManager := getMMapRWManager(fd, filePath, fdm, 8*MB)

	data := make([]byte, len(b))
	n, err := mmManager.ReadAt(data, off)
	require.NoError(t, err)
	require.Equal(t, len(b), n)
	require.Equal(t, b, data)
}

func TestRWManager_MMap_ReadAt_NotEnoughBytes(t *testing.T) {
	filePath := path.Join(t.TempDir(), "rw_mmap")
	maxFdNums := 1024
	cleanThreshold := 0.5
	var fdm = fileio.NewFdm(maxFdNums, cleanThreshold)

	fd, err := fdm.GetFd(filePath)
	require.NoError(t, err)

	defer os.Remove(fd.Name())

	err = Truncate(filePath, 8*MB, fd)
	require.NoError(t, err)

	m, err := mmap.Map(fd, mmap.RDWR, 0)
	require.NoError(t, err)

	b := []byte("test")
	off := int64(8*MB - 4)
	copy(m[off:], b)

	mmManager := getMMapRWManager(fd, filePath, fdm, 8*MB)

	data := make([]byte, 10)
	n, err := mmManager.ReadAt(data, off)
	require.NoError(t, err)
	require.Equal(t, len(b), n)
	require.Equal(t, b, data[0:4])
}

func TestRWManager_MMap_ReadAt_ErrIndexOutOfBound(t *testing.T) {
	filePath := path.Join(t.TempDir(), "rw_mmap")
	maxFdNums := 1024
	cleanThreshold := 0.5
	var fdm = fileio.NewFdm(maxFdNums, cleanThreshold)

	fd, err := fdm.GetFd(filePath)
	require.NoError(t, err)

	defer os.Remove(fd.Name())

	err = Truncate(filePath, 8*MB, fd)
	require.NoError(t, err)

	b := make([]byte, 16)
	mmManager := getMMapRWManager(fd, filePath, fdm, 8*MB)
	_, err = mmManager.ReadAt(b, 9*MB)
	require.True(t, errors.Is(err, ErrIndexOutOfBound))
	_, err = mmManager.ReadAt(b, -1)
	require.True(t, errors.Is(err, ErrIndexOutOfBound))
}

func TestRWManager_MMap_Sync(t *testing.T) {
	filePath := path.Join(t.TempDir(), t.Name())
	maxFdNums := 1024
	cleanThreshold := 0.5
	var fdm = fileio.NewFdm(maxFdNums, cleanThreshold)

	fd, err := fdm.GetFd(filePath)
	if err != nil {
		require.NoError(t, err)
	}
	defer os.Remove(fd.Name())

	err = Truncate(filePath, 8*MB, fd)
	if err != nil {
		require.NoError(t, err)

	}

	mmManager := getMMapRWManager(fd, filePath, fdm, 8*MB)
	m, err := mmap.Map(fd, mmap.RDWR, 0)
	if err != nil {
		require.NoError(t, err)
	}
	m[1] = 'z'
	err = mmManager.Sync()
	require.NoError(t, err)
	fileContents, err := os.ReadFile(filePath)
	require.NoError(t, err)
	require.Equal(t, fileContents, []byte(m[:]))
}

func TestRWManager_MMap_Close(t *testing.T) {
	filePath := path.Join(t.TempDir(), t.Name())
	maxFdNums := 1024
	cleanThreshold := 0.5
	var fdm = fileio.NewFdm(maxFdNums, cleanThreshold)

	fd, err := fdm.GetFd(filePath)
	if err != nil {
		require.NoError(t, err)
	}
	defer os.Remove(fd.Name())

	err = Truncate(filePath, 8*MB, fd)
	if err != nil {
		require.NoError(t, err)

	}

	mmManager := getMMapRWManager(fd, filePath, fdm, 8*MB)
	err = mmManager.Close()
	err = isFileDescriptorClosed(fd.Fd())
	if err == nil {
		t.Error("expected file descriptor to be closed, but it's still open")
	}
}

func isFileDescriptorClosed(fd uintptr) error {
	file := os.NewFile(fd, "")
	defer file.Close()
	_, err := file.Stat()

	return err
}
