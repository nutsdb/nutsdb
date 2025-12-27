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

package fileio_test

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/nutsdb/nutsdb"
	"github.com/nutsdb/nutsdb/internal/fileio"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/edsrzf/mmap-go"
)

type rwMgrMMapTestSuite struct {
	suite.Suite
}

func (s *rwMgrMMapTestSuite) isActive(rwmanager *fileio.MMapRWManager) bool {
	return rwmanager.Fdm.Cache[rwmanager.Path].Using() != 0
}

func (s *rwMgrMMapTestSuite) isFileDescriptorClosed(fd uintptr) error {
	file := os.NewFile(fd, "")
	defer func() { _ = file.Close() }()
	_, err := file.Stat()

	return err
}

func (s *rwMgrMMapTestSuite) TestRWManager_MMap_Release() {
	t := s.T()
	filePath := filepath.Join(t.TempDir(), "foo_rw_MMap")
	fdm := nutsdb.NewFileManager(nutsdb.MMap, 8*nutsdb.MB, 0.5, 8*nutsdb.MB)
	rwmanager, err := fdm.GetMMapRWManager(filePath, 8*nutsdb.MB, 8*nutsdb.MB, false)
	if err != nil {
		t.Error("err TestRWManager_MMap_Release GetMMapRWManager")
	}

	b := []byte("hello")
	off := int64(0)
	_, err = rwmanager.WriteAt(b, off)
	if err != nil {
		t.Error("err TestRWManager_MMap_Release WriteAt")
	}

	if !s.isActive(rwmanager) {
		t.Error("err TestRWManager_MMap_Release FdInfo:using not correct")
	}

	err = rwmanager.Release()
	if err != nil {
		t.Error("err TestRWManager_MMap_Release Release")
	}

	if s.isActive(rwmanager) {
		t.Error("err TestRWManager_MMap_Release Release Failed")
	}
}

func (s *rwMgrMMapTestSuite) TestRWManager_MMap_WriteAt() {
	t := s.T()
	filePath := filepath.Join(t.TempDir(), "foo_rw_filemmap")
	maxFdNums := 1024
	cleanThreshold := 0.5
	var fdm = fileio.NewFdm(maxFdNums, cleanThreshold)

	fd, err := fdm.GetFd(filePath)
	if err != nil {
		require.NoError(t, err)
	}
	defer func() { _ = os.Remove(fd.Name()) }()

	err = fileio.Truncate(filePath, 8*nutsdb.MB, fd, false)
	if err != nil {
		require.NoError(t, err)

	}

	mmManager := fileio.GetMMapRWManager(fd, filePath, fdm, 8*fileio.MB)
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

func (s *rwMgrMMapTestSuite) TestRWManager_MMap_WriteAt_NotEnoughData() {
	t := s.T()
	filePath := filepath.Join(t.TempDir(), "rw_mmap")
	maxFdNums := 1024
	cleanThreshold := 0.5
	var fdm = fileio.NewFdm(maxFdNums, cleanThreshold)

	fd, err := fdm.GetFd(filePath)
	require.NoError(t, err)

	defer func() { _ = os.Remove(fd.Name()) }()

	err = fileio.Truncate(filePath, 8*nutsdb.MB, fd, false)
	require.NoError(t, err)

	m, err := mmap.Map(fd, mmap.RDWR, 0)
	require.NoError(t, err)

	b := []byte("test-data-message")
	off := int64(8*fileio.MB - 4)
	copy(m[off:], b)

	mmManager := fileio.GetMMapRWManager(fd, filePath, fdm, 8*fileio.MB)

	n, err := mmManager.WriteAt(b, off)
	require.NoError(t, err)
	require.Equal(t, 4, n)

	data := make([]byte, n)
	copy(data, m[off:])
	require.Equal(t, b[:n], data)
}

func (s *rwMgrMMapTestSuite) TestRWManager_MMap_ReadAt_CrossBlock() {
	t := s.T()
	filePath := filepath.Join(t.TempDir(), "rw_mmap")
	maxFdNums := 1024
	cleanThreshold := 0.5
	var fdm = fileio.NewFdm(maxFdNums, cleanThreshold)

	fd, err := fdm.GetFd(filePath)
	require.NoError(t, err)

	defer func() { _ = os.Remove(fd.Name()) }()

	err = fileio.Truncate(filePath, 8*nutsdb.MB, fd, false)
	require.NoError(t, err)

	m, err := mmap.Map(fd, mmap.RDWR, 0)
	require.NoError(t, err)

	b := []byte("test-data-message")
	off := int64(fileio.MmapBlockSize - 5)
	copy(m[off:], b)

	mmManager := fileio.GetMMapRWManager(fd, filePath, fdm, 8*fileio.MB)

	data := make([]byte, len(b))
	n, err := mmManager.ReadAt(data, off)
	require.NoError(t, err)
	require.Equal(t, len(b), n)
	require.Equal(t, b, data)
}

func (s *rwMgrMMapTestSuite) TestRWManager_MMap_ReadAt_NotEnoughBytes() {
	t := s.T()
	filePath := filepath.Join(t.TempDir(), "rw_mmap")
	maxFdNums := 1024
	cleanThreshold := 0.5
	var fdm = fileio.NewFdm(maxFdNums, cleanThreshold)

	fd, err := fdm.GetFd(filePath)
	require.NoError(t, err)

	defer func() { _ = os.Remove(fd.Name()) }()

	err = fileio.Truncate(filePath, 8*nutsdb.MB, fd, false)
	require.NoError(t, err)

	m, err := mmap.Map(fd, mmap.RDWR, 0)
	require.NoError(t, err)

	b := []byte("test")
	off := int64(8*fileio.MB - 4)
	copy(m[off:], b)

	mmManager := fileio.GetMMapRWManager(fd, filePath, fdm, 8*fileio.MB)

	data := make([]byte, 10)
	n, err := mmManager.ReadAt(data, off)
	require.NoError(t, err)
	require.Equal(t, len(b), n)
	require.Equal(t, b, data[0:4])
}

func (s *rwMgrMMapTestSuite) TestRWManager_MMap_ReadAt_ErrIndexOutOfBound() {
	t := s.T()
	filePath := filepath.Join(t.TempDir(), "rw_mmap")
	maxFdNums := 1024
	cleanThreshold := 0.5
	var fdm = fileio.NewFdm(maxFdNums, cleanThreshold)

	fd, err := fdm.GetFd(filePath)
	require.NoError(t, err)

	defer func() { _ = os.Remove(fd.Name()) }()

	err = fileio.Truncate(filePath, 8*nutsdb.MB, fd, false)
	require.NoError(t, err)

	b := make([]byte, 16)
	mmManager := fileio.GetMMapRWManager(fd, filePath, fdm, 8*fileio.MB)
	_, err = mmManager.ReadAt(b, 9*fileio.MB)
	require.True(t, errors.Is(err, fileio.ErrIndexOutOfBound))
	_, err = mmManager.ReadAt(b, -1)
	require.True(t, errors.Is(err, fileio.ErrIndexOutOfBound))
}

func (s *rwMgrMMapTestSuite) TestRWManager_MMap_Sync() {
	t := s.T()
	r := require.New(t)
	filePath := filepath.Join(t.TempDir(), t.Name())
	err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
	r.NoError(err)
	maxFdNums := 1024
	cleanThreshold := 0.5
	var fdm = fileio.NewFdm(maxFdNums, cleanThreshold)

	fd, err := fdm.GetFd(filePath)
	if err != nil {
		require.NoError(t, err)
	}
	defer func() { _ = os.Remove(fd.Name()) }()

	err = fileio.Truncate(filePath, 8*nutsdb.MB, fd, false)
	if err != nil {
		require.NoError(t, err)

	}

	mmManager := fileio.GetMMapRWManager(fd, filePath, fdm, 8*fileio.MB)
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

func (s *rwMgrMMapTestSuite) TestRWManager_MMap_Close() {
	t := s.T()
	r := require.New(t)
	filePath := filepath.Join(t.TempDir(), t.Name())
	err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
	r.NoError(err)
	maxFdNums := 1024
	cleanThreshold := 0.5
	var fdm = fileio.NewFdm(maxFdNums, cleanThreshold)

	fd, err := fdm.GetFd(filePath)
	if err != nil {
		require.NoError(t, err)
	}
	defer func() { _ = os.Remove(fd.Name()) }()

	err = fileio.Truncate(filePath, 8*fileio.MB, fd, false)
	if err != nil {
		require.NoError(t, err)

	}

	mmManager := fileio.GetMMapRWManager(fd, filePath, fdm, 8*fileio.MB)
	require.NoError(t, mmManager.Close())
	err = s.isFileDescriptorClosed(fd.Fd())
	if err == nil {
		t.Error("expected file descriptor to be closed, but it's still open")
	}
}

func TestRWMgrMMapMain(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	suite.Run(t, new(rwMgrMMapTestSuite))
}
