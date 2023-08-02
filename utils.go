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
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/xujiajun/utils/filesystem"
	"github.com/xujiajun/utils/strconv2"
)

// Truncate changes the size of the file.
func Truncate(path string, capacity int64, f *os.File) error {
	fileInfo, _ := os.Stat(path)
	if fileInfo.Size() < capacity {
		if err := f.Truncate(capacity); err != nil {
			return err
		}
	}
	return nil
}

func MarshalInts(ints []int) ([]byte, error) {
	buffer := bytes.NewBuffer([]byte{})
	for _, x := range ints {
		if err := binary.Write(buffer, binary.LittleEndian, int64(x)); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func UnmarshalInts(data []byte) ([]int, error) {
	var ints []int
	buffer := bytes.NewBuffer(data)
	for {
		var i int64
		err := binary.Read(buffer, binary.LittleEndian, &i)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}
		ints = append(ints, int(i))
	}
	return ints, nil
}

func MatchForRange(pattern, key string, f func(key string) bool) (end bool, err error) {
	match, err := filepath.Match(pattern, key)
	if err != nil {
		return true, err
	}
	if match && !f(key) {
		return true, nil
	}
	return false, nil
}

// getDataPath returns the data path for the given file ID.
func getDataPath(fID int64, dir string) string {
	separator := string(filepath.Separator)
	return dir + separator + strconv2.Int64ToStr(fID) + DataSuffix
}

// getMetaPath returns the path for the meta file in the specified directory.
func getMetaPath(dir string) string {
	separator := string(filepath.Separator)
	return dir + separator + "meta"
}

// getBucketMetaPath returns the path for the bucket meta file in the specified directory.
func getBucketMetaPath(dir string) string {
	separator := string(filepath.Separator)
	return getMetaPath(dir) + separator + "bucket"
}

// getBucketMetaFilePath returns the path for the bucket meta file with the given name.
func getBucketMetaFilePath(name, dir string) string {
	separator := string(filepath.Separator)
	return getBucketMetaPath(dir) + separator + name + BucketMetaSuffix
}

// getBPTDir returns the BPT directory path in the specified directory.
func getBPTDir(dir string) string {
	separator := string(filepath.Separator)
	return dir + separator + bptDir
}

// getBPTPath returns the BPT index path for the given file ID.
func getBPTPath(fID int64, dir string) string {
	separator := string(filepath.Separator)
	return getBPTDir(dir) + separator + strconv2.Int64ToStr(fID) + BPTIndexSuffix
}

// getBPTRootPath returns the BPT root index path for the given file ID.
func getBPTRootPath(fID int64, dir string) string {
	separator := string(filepath.Separator)
	return getBPTDir(dir) + separator + "root" + separator + strconv2.Int64ToStr(fID) + BPTRootIndexSuffix
}

// getBPTTxIDPath returns the BPT transaction ID index path for the given file ID.
func getBPTTxIDPath(fID int64, dir string) string {
	separator := string(filepath.Separator)
	return getBPTDir(dir) + separator + "txid" + separator + strconv2.Int64ToStr(fID) + BPTTxIDIndexSuffix
}

// getBPTRootTxIDPath returns the BPT root transaction ID index path for the given file ID.
func getBPTRootTxIDPath(fID int64, dir string) string {
	separator := string(filepath.Separator)
	return getBPTDir(dir) + separator + "txid" + separator + strconv2.Int64ToStr(fID) + BPTRootTxIDIndexSuffix
}

func OneOfUint16Array(value uint16, array []uint16) bool {
	for _, v := range array {
		if v == value {
			return true
		}
	}
	return false
}

func createDirIfNotExist(dir string) error {
	if ok := filesystem.PathIsExist(dir); !ok {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return err
		}
	}
	return nil
}
