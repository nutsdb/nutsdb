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
)

var (
	filepath string
	entry    Entry
)

func init() {
	filepath = "/tmp/foo"
	entry = Entry{
		Key:   []byte("key_0001"),
		Value: []byte("val_0001"),
		Meta: &MetaData{
			keySize:    uint32(len("key_0001")),
			valueSize:  uint32(len("val_0001")),
			timestamp:  1547707905,
			TTL:        Persistent,
			bucket:     []byte("test_DataFile"),
			bucketSize: uint32(len("test_datafile")),
			Flag:       DataSetFlag,
		},
		position: 0,
	}
}
func TestDataFile_Err(t *testing.T) {
	_, err := NewDataFile(filepath, -1, FileIO)
	defer os.Remove(filepath)

	if err == nil {
		t.Error("err invalid argument")
	}

}

func TestDataFile1(t *testing.T) {
	df, err := NewDataFile(filepath, 1024, MMap)
	defer df.rwManager.Close()
	if err != nil {
		t.Fatal(err)
	}

	n, err := df.WriteAt(entry.Encode(), 0)
	if err != nil {
		t.Error("err TestDataFile_All WriteAt")
	}

	e, err := df.ReadAt(n)
	if e != nil || err != nil {
		t.Error("err TestDataFile_All ReadAt")
	}

	e, err = df.ReadAt(0)
	if err != nil || string(e.Key) != "key_0001" || string(e.Value) != "val_0001" || e.Meta.timestamp != 1547707905 {
		t.Error("err TestDataFile_All ReadAt")
	}

	e, err = df.ReadAt(1)
	if err == nil || e != nil {
		t.Error("err TestDataFile_All ReadAt")
	}
}

func TestDataFile2(t *testing.T) {
	filepath2 := "/tmp/foo2"
	df, err := NewDataFile(filepath2, 39, FileIO)
	defer df.rwManager.Close()
	defer os.Remove(filepath2)
	if err != nil {
		t.Fatal(err)
	}
	content := entry.Encode()[0 : DataEntryHeaderSize-1]
	_, err = df.WriteAt(content, 0)
	if err != nil {
		t.Error("err TestDataFile_All WriteAt")
	}

	e, err := df.ReadAt(0)
	if err == nil || e != nil {
		t.Error("err TestDataFile_All ReadAt")
	}

	filepath3 := "/tmp/foo3"
	df, err = NewDataFile(filepath3, 41, FileIO)
	defer os.Remove(filepath3)
	if err != nil {
		t.Fatal(err)
	}
	content = entry.Encode()[0 : DataEntryHeaderSize+1]
	_, err = df.WriteAt(content, 0)
	if err != nil {
		t.Error("err TestDataFile_All WriteAt")
	}

	e, err = df.ReadAt(0)
	if err == nil || e != nil {
		t.Error("err TestDataFile_All ReadAt")
	}
}

func TestDataFile_ReadAt(t *testing.T) {
	df, err := NewDataFile(filepath, 1024, FileIO)
	if err != nil {
		t.Fatal(err)
	}

	e, err := df.ReadAt(0)
	if err != nil && e != nil {
		t.Error("err ReadAt")
	}

	e, err = df.ReadAt(1025)
	if err == nil && e != nil {
		t.Error("err ReadAt")
	}
}

func TestDataFile_Err_Path(t *testing.T) {
	filepath5 := ":/tmp/foo5"
	df, err := NewDataFile(filepath5, entry.Size(), FileIO)
	if err == nil && df != nil {
		t.Error("err TestDataFile_All open")
	}
}

func TestDataFile_Crc_Err(t *testing.T) {
	filepath4 := "/tmp/foo4"

	df, err := NewDataFile(filepath4, entry.Size(), FileIO)
	defer os.Remove(filepath4)
	if err != nil {
		t.Fatal(err)
	}
	var errContent []byte
	errContent = append(errContent, entry.Encode()[0:4]...)
	errContent = append(errContent, entry.Encode()[4:entry.Size()-1]...)
	errContent = append(errContent, 0)
	_, err = df.WriteAt(errContent, 0)
	if err != nil {
		t.Error("err TestDataFile_All WriteAt")
	}

	e, err := df.ReadAt(0)
	if err == nil || e != nil {
		t.Error("err TestDataFile_All ReadAt")
	}
}
