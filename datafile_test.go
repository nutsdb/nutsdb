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

	"github.com/stretchr/testify/assert"
)

var (
	filePath string
	entryObj entry
)

func init() {
	filePath = "/tmp/foo"
	entryObj = entry{
		Key:   []byte("key_0001"),
		Value: []byte("val_0001"),
		Meta: newMetaData().withKeySize(uint32(len("key_0001"))).
			withValueSize(uint32(len("val_0001"))).withTimeStamp(1547707905).
			withTTL(Persistent).withFlag(DataSetFlag).withBucketId(1),
	}
}

func TestDataFile_Err(t *testing.T) {
	fm := newFileManager(MMap, 1024, 0.5, 256*mb)
	defer fm.close()
	_, err := fm.getDataFile(filePath, -1)
	defer func() {
		os.Remove(filePath)
	}()

	assert.NotNil(t, err)
}

func TestDataFile1(t *testing.T) {
	fm := newFileManager(MMap, 1024, 0.5, 256*mb)
	defer fm.close()
	df, err := fm.getDataFile(filePath, 1024)
	defer os.Remove(filePath)
	if err != nil {
		t.Fatal(err)
	}

	n, err := df.WriteAt(entryObj.encode(), 0)
	if err != nil {
		t.Error("err TestDataFile_All WriteAt")
	}

	payloadSize := entryObj.Meta.payloadSize()
	e, err := df.ReadEntry(n, payloadSize)
	assert.Nil(t, e)
	assert.Error(t, err, ErrEntryZero)

	e, err = df.ReadEntry(0, payloadSize)
	if err != nil || string(e.Key) != "key_0001" || string(e.Value) != "val_0001" || e.Meta.Timestamp != 1547707905 {
		t.Error("err TestDataFile_All ReadAt")
	}

	e, err = df.ReadEntry(1, payloadSize)
	if err == nil || e != nil {
		t.Error("err TestDataFile_All ReadAt")
	}
}

func TestDataFile2(t *testing.T) {
	fm := newFileManager(FileIO, 1024, 0.5, 256*mb)

	filePath2 := "/tmp/foo2"
	df, err := fm.getDataFile(filePath2, 64)
	assert.Nil(t, err)
	defer os.Remove(filePath2)
	headerSize := entryObj.Meta.size()
	content := entryObj.encode()[0 : headerSize-1]
	_, err = df.WriteAt(content, 0)
	if err != nil {
		t.Error("err TestDataFile_All WriteAt")
	}

	payloadSize := entryObj.Meta.payloadSize()
	e, err := df.ReadEntry(0, payloadSize)
	if err == nil || e != nil {
		t.Error("err TestDataFile_All ReadAt")
	}

	filePath3 := "/tmp/foo3"

	df2, err := fm.getDataFile(filePath3, 64)
	defer os.Remove(filePath3)
	assert.Nil(t, err)

	headerSize = entryObj.Meta.size()
	content = entryObj.encode()[0 : headerSize+1]
	_, err = df2.WriteAt(content, 0)
	assert.Nil(t, err)

	e, err = df2.ReadEntry(0, payloadSize)
	if err == nil || e != nil {
		t.Error("err TestDataFile_All ReadAt")
	}

	err = df.Release()
	assert.Nil(t, err)
	err = df2.Release()
	assert.Nil(t, err)
	err = fm.close()
	assert.Nil(t, err)
}

func TestDataFile_ReadRecord(t *testing.T) {
	fm := newFileManager(FileIO, 1024, 0.5, 256*mb)
	filePath4 := "/tmp/foo4"
	df, err := fm.getDataFile(filePath4, 1024)
	defer func() {
		err = df.Release()
		assert.Nil(t, err)
		err = fm.close()
		assert.Nil(t, err)
	}()
	assert.Nil(t, err)
	if err != nil {
		t.Fatal(err)
	}

	payloadSize := entryObj.Meta.payloadSize()
	e, err := df.ReadEntry(0, payloadSize)
	if err != nil && e != nil {
		t.Error("err ReadAt")
	}

	e, err = df.ReadEntry(1025, payloadSize)
	if err == nil && e != nil {
		t.Error("err ReadAt")
	}
}

func TestDataFile_Err_Path(t *testing.T) {
	fm := newFileManager(FileIO, 1024, 0.5, 256*mb)
	defer fm.close()
	filePath5 := ":/tmp/foo5"
	df, err := fm.getDataFile(filePath5, entryObj.size())
	if err == nil && df != nil {
		t.Error("err TestDataFile_All open")
	}
}

func TestDataFile_Crc_Err(t *testing.T) {
	fm := newFileManager(FileIO, 1024, 0.5, 256*mb)
	filePath4 := "/tmp/foo6"

	df, err := fm.getDataFile(filePath4, entryObj.size())
	assert.Nil(t, err)
	assert.NotNil(t, df)
	defer func() {
		err = df.Release()
		assert.Nil(t, err)
		err = fm.close()
		assert.Nil(t, err)
		err = os.Remove(filePath4)
		assert.Nil(t, err)
	}()

	var errContent []byte
	errContent = append(errContent, entryObj.encode()[0:4]...)
	errContent = append(errContent, entryObj.encode()[4:entryObj.size()-1]...)
	errContent = append(errContent, 0)
	_, err = df.WriteAt(errContent, 0)
	assert.Nil(t, err)

	payloadSize := entryObj.Meta.payloadSize()
	e, err := df.ReadEntry(0, payloadSize)
	if err == nil || e != nil {
		t.Error("err TestDataFile_All ReadAt")
	}
}

func TestFileManager1(t *testing.T) {
	fm := newFileManager(FileIO, 1024, 0.5, 256*mb)
	filePath4 := "/tmp/foo6"
	df, err := fm.getDataFile(filePath4, entryObj.size())
	assert.Nil(t, err)
	defer func() {
		err = df.Release()
		assert.Nil(t, err)
		err = fm.close()
		assert.Nil(t, err)
		os.Remove(filePath)
	}()
}
