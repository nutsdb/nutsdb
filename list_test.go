// Copyright 2023 The PromiseDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file expect in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nutsdb

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func ListPush(t *testing.T, list *List, key string, r *Record, isLeft bool, expectError error) {
	var e error
	if isLeft {
		e = list.LPush(key, r)
	} else {
		e = list.RPush(key, r)
	}
	assertErr(t, e, expectError)
}

func ListPop(t *testing.T, list *List, key string, isLeft bool, expectVal *Record, expectError error) {
	var (
		e error
		r *Record
	)

	if isLeft {
		r, e = list.LPop(key)
	} else {
		r, e = list.RPop(key)
	}
	if expectError != nil {
		require.Equal(t, expectError, e)
	} else {
		require.NoError(t, e)
		require.Equal(t, expectVal, r)
	}
}

func ListCmp(t *testing.T, list *List, key string, expectRecords []*Record, isReverse bool) {
	records, err := list.LRange(key, 0, -1)
	require.NoError(t, err)

	if isReverse {
		for i := len(expectRecords) - 1; i >= 0; i-- {
			require.Equal(t, expectRecords[i], records[len(expectRecords)-1-i])
		}
	} else {
		for i := 0; i < len(expectRecords); i++ {
			require.Equal(t, expectRecords[i], records[i])
		}
	}
}

func TestList_LPush(t *testing.T) {
	list := NewList()
	expectRecords := generateRecords(5)
	key := string(GetTestBytes(0))

	for i := 0; i < len(expectRecords); i++ {
		ListPush(t, list, key, expectRecords[i], true, nil)
	}

	ListCmp(t, list, key, expectRecords, true)
}

func TestList_RPush(t *testing.T) {
	list := NewList()
	expectRecords := generateRecords(5)
	key := string(GetTestBytes(0))

	for i := 0; i < len(expectRecords); i++ {
		ListPush(t, list, key, expectRecords[i], false, nil)
	}

	ListCmp(t, list, key, expectRecords, false)
}

func TestList_Pop(t *testing.T) {
	list := NewList()
	expectRecords := generateRecords(5)
	key := string(GetTestBytes(0))

	ListPop(t, list, key, true, nil, ErrListNotFound)

	for i := 0; i < len(expectRecords); i++ {
		ListPush(t, list, key, expectRecords[i], false, nil)
	}

	ListPop(t, list, key, true, expectRecords[0], nil)
	expectRecords = expectRecords[1:]

	ListPop(t, list, key, false, expectRecords[len(expectRecords)-1], nil)
	expectRecords = expectRecords[:len(expectRecords)-1]

	ListCmp(t, list, key, expectRecords, false)
}

func TestList_LRem(t *testing.T) {
	list := NewList()
	records := generateRecords(2)
	key := string(GetTestBytes(0))

	for i := 0; i < 3; i++ {
		ListPush(t, list, key, records[0], false, nil)
	}

	ListPush(t, list, key, records[1], false, nil)
	ListPush(t, list, key, records[0], false, nil)
	ListPush(t, list, key, records[1], false, nil)

	// r1 r1 r1 r2 r1 r2
	expectRecords := []*Record{records[0], records[0], records[0], records[1], records[0], records[1]}

	cmp := func(r *Record) bool {
		return bytes.Equal(r.E.Value, records[0].E.Value)
	}

	// r1 r1 r1 r2 r2
	err := list.LRem(key, -1, cmp)
	require.NoError(t, err)
	expectRecords = append(expectRecords[0:4], expectRecords[5:]...)
	ListCmp(t, list, key, expectRecords, false)

	// r1 r2 r2
	err = list.LRem(key, 2, cmp)
	require.NoError(t, err)
	expectRecords = expectRecords[2:]
	ListCmp(t, list, key, expectRecords, false)

	cmp = func(r *Record) bool {
		return bytes.Equal(r.E.Value, records[1].E.Value)
	}

	// r1
	err = list.LRem(key, 0, cmp)
	require.NoError(t, err)
	expectRecords = expectRecords[0:1]
	ListCmp(t, list, key, expectRecords, false)
}

func TestList_LSet(t *testing.T) {
	list := NewList()
	expectRecords := generateRecords(5)
	key := string(GetTestBytes(0))

	for i := 0; i < len(expectRecords); i++ {
		ListPush(t, list, key, expectRecords[i], false, nil)
	}

	err := list.LSet(key, 3, expectRecords[4])
	require.NoError(t, err)

	expectRecords[3] = expectRecords[4]
	ListCmp(t, list, key, expectRecords, false)
}

func TestList_LTrim(t *testing.T) {
	list := NewList()
	expectRecords := generateRecords(5)
	key := string(GetTestBytes(0))

	for i := 0; i < len(expectRecords); i++ {
		ListPush(t, list, key, expectRecords[i], false, nil)
	}

	err := list.LTrim(key, 1, 3)
	require.NoError(t, err)
	expectRecords = expectRecords[1 : len(expectRecords)-1]
	ListCmp(t, list, key, expectRecords, false)
}

func TestList_LRemByIndex(t *testing.T) {
	list := NewList()
	expectRecords := generateRecords(8)
	key := string(GetTestBytes(0))

	// r1 r2 r3 r4 r5 r6 r7 r8
	for i := 0; i < 8; i++ {
		ListPush(t, list, key, expectRecords[i], false, nil)
	}

	// r1 r2 r4 r5 r6 r7 r8
	err := list.LRemByIndex(key, []int{2})
	require.NoError(t, err)
	expectRecords = append(expectRecords[0:2], expectRecords[3:]...)
	ListCmp(t, list, key, expectRecords, false)

	// r2 r6 r7 r8
	err = list.LRemByIndex(key, []int{0, 2, 3})
	require.NoError(t, err)
	expectRecords = expectRecords[1:]
	expectRecords = append(expectRecords[0:1], expectRecords[3:]...)
	ListCmp(t, list, key, expectRecords, false)

	err = list.LRemByIndex(key, []int{0, 0, 0})
	require.NoError(t, err)
	expectRecords = expectRecords[1:]
	ListCmp(t, list, key, expectRecords, false)
}

func generateRecords(count int) []*Record {
	bucket := []byte("bucket")
	rand.Seed(time.Now().UnixNano())
	records := make([]*Record, count)
	for i := 0; i < count; i++ {
		key := GetTestBytes(i)
		val := GetRandomBytes(24)

		metaData := &MetaData{
			KeySize:    uint32(len(key)),
			ValueSize:  uint32(len(val)),
			Timestamp:  uint64(time.Now().Unix()),
			TTL:        uint32(rand.Intn(3600)),
			Flag:       uint16(rand.Intn(2)),
			BucketSize: uint32(len(bucket)),
			TxID:       uint64(rand.Intn(1000)),
			Status:     uint16(rand.Intn(2)),
			Ds:         uint16(rand.Intn(3)),
			Crc:        rand.Uint32(),
		}

		record := &Record{
			H: &Hint{
				Key:     key,
				FileID:  int64(i),
				Meta:    metaData,
				DataPos: uint64(rand.Uint32()),
			},
			E: &Entry{
				Key:    key,
				Value:  val,
				Bucket: bucket,
				Meta:   metaData,
			},
			Bucket: string(bucket),
		}
		records[i] = record
	}
	return records
}
