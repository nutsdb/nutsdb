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
	"sort"
)

// SortedEntryKeys returns sorted entries.
func SortedEntryKeys(m map[string]*Entry) (keys []string, es map[string]*Entry) {
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys, m
}

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
