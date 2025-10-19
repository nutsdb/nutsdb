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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/xujiajun/utils/strconv2"
)

// Truncate changes the size of the file.
// If readOnly is true, it skips the file stat check and truncate operation,
// which significantly improves read performance by avoiding unnecessary syscalls.
func Truncate(path string, capacity int64, f *os.File, readOnly bool) error {
	// Skip truncation for read-only operations to avoid expensive os.Stat syscall
	if readOnly {
		return nil
	}

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

func MatchForRange(pattern, bucket string, f func(bucket string) bool) (end bool, err error) {
	match, err := filepath.Match(pattern, bucket)
	if err != nil {
		return true, err
	}
	if match && !f(bucket) {
		return true, nil
	}
	return false, nil
}

// getDataPath returns the data path for the given file ID.
func getDataPath(fID int64, dir string) string {
	separator := string(filepath.Separator)
	if IsMergeFile(fID) {
		seq := GetMergeSeq(fID)
		return dir + separator + fmt.Sprintf("merge_%d%s", seq, DataSuffix)
	}
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

func UvarintSize(x uint64) int {
	i := 0
	for x >= 0x80 {
		x >>= 7
		i++
	}
	return i + 1
}

func VarintSize(x int64) int {
	ux := uint64(x<<1) ^ uint64(x>>63)
	return UvarintSize(ux)
}

// compareAndReturn use bytes.Compare(other, target), if return value is
// comVal, return other, else return target.
func compareAndReturn(target []byte, other []byte, cmpVal int) []byte {
	if target == nil {
		return other
	}
	if bytes.Compare(other, target) == cmpVal {
		return other
	}
	return target
}

func expireTime(timestamp uint64, ttl uint32) time.Duration {
	now := time.UnixMilli(time.Now().UnixMilli())
	expireTime := time.UnixMilli(int64(timestamp))
	expireTime = expireTime.Add(time.Duration(int64(ttl)) * time.Second)
	return expireTime.Sub(now)
}

func mergeKeyValues(
	k0, v0 [][]byte,
	k1, v1 [][]byte,
) (keys, values [][]byte) {
	l0 := len(k0)
	l1 := len(k1)
	// Pre-allocate capacity with estimated maximum size to reduce slice re-growth
	estimatedSize := l0 + l1
	keys = make([][]byte, 0, estimatedSize)
	values = make([][]byte, 0, estimatedSize)
	cur0 := 0
	cur1 := 0
	for cur0 < l0 && cur1 < l1 {
		if bytes.Compare(k0[cur0], k1[cur1]) <= 0 {
			keys = append(keys, k0[cur0])
			values = append(values, v0[cur0])
			cur0++
			if bytes.Equal(k0[cur0], k1[cur1]) {
				// skip k1 item if k0 == k1
				cur1++
			}
		} else {
			keys = append(keys, k1[cur1])
			values = append(values, v1[cur1])
			cur1++
		}
	}
	for cur0 < l0 {
		keys = append(keys, k0[cur0])
		values = append(values, v0[cur0])
		cur0++
	}
	for cur1 < l1 {
		keys = append(keys, k1[cur1])
		values = append(values, v1[cur1])
		cur1++
	}
	return
}

// This Type is for sort a pair of (k, v).
type sortkv struct {
	k, v [][]byte
}

func (skv *sortkv) Len() int {
	return len(skv.k)
}

func (skv *sortkv) Swap(i, j int) {
	skv.k[i], skv.v[i], skv.k[j], skv.v[j] = skv.k[j], skv.v[j], skv.k[i], skv.v[i]
}

func (skv *sortkv) Less(i, j int) bool {
	return bytes.Compare(skv.k[i], skv.k[j]) < 0
}
