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
	"strings"

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

func ConvertBigEndianBytesToUint64(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}

func ConvertUint64ToBigEndianBytes(value uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, value)
	return b
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

func OneOfUint16Array(value uint16, array []uint16) bool {
	for _, v := range array {
		if v == value {
			return true
		}
	}
	return false
}

func splitIntStringStr(str, separator string) (int, string) {
	strList := strings.Split(str, separator)
	firstItem, _ := strconv2.StrToInt(strList[0])
	secondItem := strList[1]
	return firstItem, secondItem
}

func splitStringIntStr(str, separator string) (string, int) {
	strList := strings.Split(str, separator)
	firstItem := strList[0]
	secondItem, _ := strconv2.StrToInt(strList[1])
	return firstItem, secondItem
}

func splitIntIntStr(str, separator string) (int, int) {
	strList := strings.Split(str, separator)
	firstItem, _ := strconv2.StrToInt(strList[0])
	secondItem, _ := strconv2.StrToInt(strList[1])
	return firstItem, secondItem
}

func encodeListKey(key []byte, seq uint64) []byte {
	buf := make([]byte, len(key)+8)
	binary.LittleEndian.PutUint64(buf[:8], seq)
	copy(buf[8:], key[:])
	return buf
}

func decodeListKey(buf []byte) ([]byte, uint64) {
	seq := binary.LittleEndian.Uint64(buf[:8])
	key := make([]byte, len(buf[8:]))
	copy(key[:], buf[8:])
	return key, seq
}

func splitStringFloat64Str(str, separator string) (string, float64) {
	strList := strings.Split(str, separator)
	firstItem := strList[0]
	secondItem, _ := strconv2.StrToFloat64(strList[1])
	return firstItem, secondItem
}

func getFnv32(value []byte) (uint32, error) {
	_, err := fnvHash.Write(value)
	if err != nil {
		return 0, err
	}
	hash := fnvHash.Sum32()
	fnvHash.Reset()
	return hash, nil
}

func generateSeq(seq *HeadTailSeq, isLeft bool) uint64 {
	var res uint64
	if isLeft {
		res = seq.Head
		seq.Head--
	} else {
		res = seq.Tail
		seq.Tail++
	}

	return res
}

func createNewBufferWithSize(size int) *bytes.Buffer {
	buf := new(bytes.Buffer)
	buf.Grow(int(size))
	return buf
}
