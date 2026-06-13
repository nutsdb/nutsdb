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
	"runtime"
	"testing"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/stretchr/testify/assert"
)

var (
	filePath string
	entry    core.Entry
)

func init() {
	filePath = filepath.Join(os.TempDir(), "foo")
	entry = core.Entry{
		Key:   []byte("key_0001"),
		Value: []byte("val_0001"),
		Meta: core.NewMetaData().WithKeySize(uint32(len("key_0001"))).
			WithValueSize(uint32(len("val_0001"))).WithTimeStamp(1547707905).
			WithTTL(Persistent).WithFlag(DataSetFlag).WithBucketId(1),
	}
}

func TestDataFile_Err(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	dfm := newDataFileManager(NewFileManager(MMap, 1024, 0.5, 256*MB))
	defer func() { _ = dfm.Close() }()
	_, err := dfm.GetDataFile(filePath, -1)
	defer func() {
		_ = dfm.Close()
		_ = os.Remove(filePath)
	}()

	assert.NotNil(t, err)
}

func TestDataFile1(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	dfm := newDataFileManager(NewFileManager(MMap, 1024, 0.5, 256*MB))
	defer func() { _ = dfm.Close() }()
	df, err := dfm.GetDataFile(filePath, 1024)
	defer func() { _ = os.Remove(filePath) }()
	if err != nil {
		t.Fatal(err)
	}

	n, err := df.WriteAt(entry.Encode(), 0)
	if err != nil {
		t.Error("err TestDataFile_All WriteAt")
	}

	payloadSize := entry.Meta.PayloadSize()
	buf, err := df.ReadData(n, payloadSize)
	if err != nil {
		t.Errorf("err TestDataFile_All ReadData: %v", err)
	}
	e, err := core.DecodeEntryWithError(buf, payloadSize, err)
	assert.Nil(t, e)
	assert.Error(t, err, ErrEntryZero)

	buf, err = df.ReadData(0, payloadSize)
	if err != nil {
		t.Errorf("err TestDataFile_All ReadData: %v", err)
	}
	e, err = core.DecodeEntryWithError(buf, payloadSize, err)
	if err != nil || string(e.Key) != "key_0001" || string(e.Value) != "val_0001" || e.Meta.Timestamp != 1547707905 {
		t.Error("err TestDataFile_All ReadAt")
	}

	buf, err = df.ReadData(1, payloadSize)
	if err != nil {
		t.Errorf("err TestDataFile_All ReadData: %v", err)
	}
	e, err = core.DecodeEntryWithError(buf, payloadSize, err)
	if err == nil || e != nil {
		t.Error("err TestDataFile_All ReadAt")
	}
}

func TestDataFile2(t *testing.T) {
	dfm := newDataFileManager(NewFileManager(FileIO, 1024, 0.5, 256*MB))
	tmpdir := t.TempDir()
	filePath2 := filepath.Join(tmpdir, "foo2")
	df, err := dfm.GetDataFile(filePath2, 64)
	assert.Nil(t, err)
	defer func() { _ = os.Remove(filePath2) }()
	headerSize := entry.Meta.Size()
	content := entry.Encode()[0 : headerSize-1]
	_, err = df.WriteAt(content, 0)
	if err != nil {
		t.Error("err TestDataFile_All WriteAt")
	}

	payloadSize := entry.Meta.PayloadSize()
	buf, err := df.ReadData(0, payloadSize)
	if err == nil {
		t.Error("err TestDataFile_All ReadData not EOF")
	}
	e, err := core.DecodeEntryWithError(buf, payloadSize, err)
	if err == nil || e != nil {
		t.Error("err TestDataFile_All ReadAt")
	}

	filePath3 := filepath.Join(tmpdir, "foo3")

	df2, err := dfm.GetDataFile(filePath3, 64)
	defer func() { _ = os.Remove(filePath3) }()
	assert.Nil(t, err)

	headerSize = entry.Meta.Size()
	content = entry.Encode()[0 : headerSize+1]
	_, err = df2.WriteAt(content, 0)
	assert.Nil(t, err)

	buf, err = df2.ReadData(0, payloadSize)
	if err == nil {
		t.Error("err TestDataFile_All ReadData not EOF")
	}
	e, err = core.DecodeEntryWithError(buf, payloadSize, err)
	if err == nil || e != nil {
		t.Error("err TestDataFile_All ReadAt")
	}

	err = df.Release()
	assert.Nil(t, err)
	err = df2.Release()
	assert.Nil(t, err)
	err = dfm.Close()
	assert.Nil(t, err)
}

func TestDataFile_ReadRecord(t *testing.T) {
	dfm := newDataFileManager(NewFileManager(FileIO, 1024, 0.5, 256*MB))
	tmpdir := t.TempDir()
	filePath4 := filepath.Join(tmpdir, "foo4")
	df, err := dfm.GetDataFile(filePath4, 1024)
	defer func() {
		err = df.Release()
		assert.Nil(t, err)
		err = dfm.Close()
		assert.Nil(t, err)
	}()
	assert.Nil(t, err)
	if err != nil {
		t.Fatal(err)
	}

	payloadSize := entry.Meta.PayloadSize()
	buf, err := df.ReadData(0, payloadSize)
	if err != nil {
		t.Errorf("err TestDataFile_All ReadData: %v", err)
	}
	e, err := core.DecodeEntryWithError(buf, payloadSize, err)
	if err != nil && e != nil {
		t.Error("err ReadAt")
	}

	buf, err = df.ReadData(1025, payloadSize)
	if err == nil {
		t.Error("err TestDataFile_All ReadData not EOF")
	}
	e, err = core.DecodeEntryWithError(buf, payloadSize, err)
	if err == nil && e != nil {
		t.Error("err ReadAt")
	}
}

func TestDataFile_Err_Path(t *testing.T) {
	dfm := newDataFileManager(NewFileManager(FileIO, 1024, 0.5, 256*MB))
	defer func() { _ = dfm.Close() }()
	filePath5 := ":/tmp/foo5"
	df, err := dfm.GetDataFile(filePath5, entry.Size())
	if err == nil && df != nil {
		t.Error("err TestDataFile_All open")
	}
}

func TestDataFile_Crc_Err(t *testing.T) {
	dfm := newDataFileManager(NewFileManager(FileIO, 1024, 0.5, 256*MB))
	filePath6 := filepath.Join(t.TempDir(), "foo6")

	df, err := dfm.GetDataFile(filePath6, entry.Size())
	assert.Nil(t, err)
	assert.NotNil(t, df)
	defer func() {
		err = df.Release()
		assert.Nil(t, err)
		err = dfm.Close()
		assert.Nil(t, err)
		err = os.Remove(filePath6)
		assert.Nil(t, err)
	}()

	var errContent []byte
	errContent = append(errContent, entry.Encode()[0:4]...)
	errContent = append(errContent, entry.Encode()[4:entry.Size()-1]...)
	errContent = append(errContent, 0)
	_, err = df.WriteAt(errContent, 0)
	assert.Nil(t, err)

	payloadSize := entry.Meta.PayloadSize()
	buf, err := df.ReadData(0, payloadSize)
	if err == nil {
		t.Error("err TestDataFile_All ReadData not EOF")
	}
	e, err := core.DecodeEntryWithError(buf, payloadSize, err)
	if err == nil || e != nil {
		t.Error("err TestDataFile_All ReadAt")
	}
}

func TestFileManager1(t *testing.T) {
	dfm := newDataFileManager(NewFileManager(FileIO, 1024, 0.5, 256*MB))
	filePath6 := filepath.Join(t.TempDir(), "foo2")
	df, err := dfm.GetDataFile(filePath6, entry.Size())
	assert.Nil(t, err)
	defer func() {
		err = df.Release()
		assert.Nil(t, err)
		err = dfm.Close()
		assert.Nil(t, err)
		_ = os.Remove(filePath)
	}()
}
