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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/edsrzf/mmap-go"
)

func TestRWManager_MMap_Release(t *testing.T) {
	filePath := "/tmp/foo_rw_MMap"
	fdm := newFileManager(MMap, 1024, 0.5, 256*MB)
	rwmanager, err := fdm.getMMapRWManager(filePath, 1024, 256*MB)
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
	return rwmanager.fdm.cache[rwmanager.path].using != 0
}

func TestRWManager_MMap_WriteAt(t *testing.T) {
	filePath := "/tmp/foo_rw_filemmap"
	maxFdNums := 1024
	cleanThreshold := 0.5
	var fdm = newFdm(maxFdNums, cleanThreshold)

	fd, err := fdm.getFd(filePath)
	if err != nil {
		require.NoError(t, err)
	}
	defer os.Remove(fd.Name())

	err = Truncate(filePath, 1024, fd)
	if err != nil {
		require.NoError(t, err)

	}

	mmManager := newMMapRWManager(fd, filePath, fdm, 256*MB)
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

func TestRWManager_MMap_Sync(t *testing.T) {
	filePath := "/tmp/foo_rw_filemmap"
	maxFdNums := 1024
	cleanThreshold := 0.5
	var fdm = newFdm(maxFdNums, cleanThreshold)

	fd, err := fdm.getFd(filePath)
	if err != nil {
		require.NoError(t, err)
	}
	defer os.Remove(fd.Name())

	err = Truncate(filePath, 1024, fd)
	if err != nil {
		require.NoError(t, err)

	}

	mmManager := newMMapRWManager(fd, filePath, fdm, 256*MB)
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
	filePath := "/tmp/foo_rw_filemmap"
	maxFdNums := 1024
	cleanThreshold := 0.5
	var fdm = newFdm(maxFdNums, cleanThreshold)

	fd, err := fdm.getFd(filePath)
	if err != nil {
		require.NoError(t, err)
	}
	defer os.Remove(fd.Name())

	err = Truncate(filePath, 1024, fd)
	if err != nil {
		require.NoError(t, err)

	}

	mmManager := newMMapRWManager(fd, filePath, fdm, 256*MB)
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
