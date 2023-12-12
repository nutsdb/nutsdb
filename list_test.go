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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func ListPush(t *testing.T, list *list, key string, r *record, isLeft bool, expectError error) {
	var e error
	if isLeft {
		e = list.lPush(key, r)
	} else {
		e = list.rPush(key, r)
	}
	assertErr(t, e, expectError)
}

func ListPop(t *testing.T, list *list, key string, isLeft bool, expectVal *record, expectError error) {
	var (
		e error
		r *record
	)

	if isLeft {
		r, e = list.lPop(key)
	} else {
		r, e = list.rPop(key)
	}
	if expectError != nil {
		require.Equal(t, expectError, e)
	} else {
		require.NoError(t, e)
		require.Equal(t, expectVal, r)
	}
}

func ListCmp(t *testing.T, list *list, key string, expectRecords []*record, isReverse bool) {
	records, err := list.lRange(key, 0, -1)
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
	list := newList()
	// 测试 lPush
	key := string(getTestBytes(0))
	expectRecords := generateRecords(5)
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	for i := 0; i < len(expectRecords); i++ {
		seq := generateSeq(&seqInfo, true)
		newKey := encodeListKey([]byte(key), seq)
		expectRecords[i].Key = newKey
		ListPush(t, list, string(newKey), expectRecords[i], true, nil)
	}

	ListCmp(t, list, key, expectRecords, true)
}

func TestList_RPush(t *testing.T) {
	list := newList()
	expectRecords := generateRecords(5)
	key := string(getTestBytes(0))
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	for i := 0; i < len(expectRecords); i++ {
		seq := generateSeq(&seqInfo, false)
		newKey := encodeListKey([]byte(key), seq)
		expectRecords[i].Key = newKey
		ListPush(t, list, string(newKey), expectRecords[i], false, nil)
	}

	ListCmp(t, list, key, expectRecords, false)
}

func TestList_Pop(t *testing.T) {
	list := newList()
	expectRecords := generateRecords(5)
	key := string(getTestBytes(0))

	ListPop(t, list, key, true, nil, ErrListNotFound)
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	for i := 0; i < len(expectRecords); i++ {
		seq := generateSeq(&seqInfo, false)
		newKey := encodeListKey([]byte(key), seq)
		expectRecords[i].Key = newKey
		ListPush(t, list, string(newKey), expectRecords[i], false, nil)
	}

	ListPop(t, list, key, true, expectRecords[0], nil)
	expectRecords = expectRecords[1:]

	ListPop(t, list, key, false, expectRecords[len(expectRecords)-1], nil)
	expectRecords = expectRecords[:len(expectRecords)-1]

	ListCmp(t, list, key, expectRecords, false)
}

func TestList_LRem(t *testing.T) {
	list := newList()
	records := generateRecords(2)
	key := string(getTestBytes(0))
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	for i := 0; i < 3; i++ {
		seq := generateSeq(&seqInfo, false)
		newKey := encodeListKey([]byte(key), seq)
		records[0].Key = newKey
		ListPush(t, list, string(newKey), records[0], false, nil)
	}

	seq := generateSeq(&seqInfo, false)
	newKey := encodeListKey([]byte(key), seq)
	records[1].Key = newKey
	ListPush(t, list, string(newKey), records[1], false, nil)

	seq = generateSeq(&seqInfo, false)
	newKey = encodeListKey([]byte(key), seq)
	records[0].Key = newKey
	ListPush(t, list, string(newKey), records[0], false, nil)

	seq = generateSeq(&seqInfo, false)
	newKey = encodeListKey([]byte(key), seq)
	records[1].Key = newKey
	ListPush(t, list, string(newKey), records[1], false, nil)

	// r1 r1 r1 r2 r1 r2
	expectRecords := []*record{records[0], records[0], records[0], records[1], records[0], records[1]}

	cmp := func(r *record) (bool, error) {
		return bytes.Equal(r.Value, records[0].Value), nil
	}

	// r1 r1 r1 r2 r2
	err := list.lRem(key, -1, cmp)
	require.NoError(t, err)
	expectRecords = append(expectRecords[0:4], expectRecords[5:]...)
	ListCmp(t, list, key, expectRecords, false)

	// r1 r2 r2
	err = list.lRem(key, 2, cmp)
	require.NoError(t, err)
	expectRecords = expectRecords[2:]
	ListCmp(t, list, key, expectRecords, false)

	cmp = func(r *record) (bool, error) {
		return bytes.Equal(r.Value, records[1].Value), nil
	}

	// r1
	err = list.lRem(key, 0, cmp)
	require.NoError(t, err)
	expectRecords = expectRecords[0:1]
	ListCmp(t, list, key, expectRecords, false)
}

func TestList_LTrim(t *testing.T) {
	list := newList()
	expectRecords := generateRecords(5)
	key := string(getTestBytes(0))
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	for i := 0; i < len(expectRecords); i++ {
		seq := generateSeq(&seqInfo, false)
		newKey := encodeListKey([]byte(key), seq)
		expectRecords[i].Key = newKey
		ListPush(t, list, string(newKey), expectRecords[i], false, nil)
	}

	err := list.lTrim(key, 1, 3)
	require.NoError(t, err)
	expectRecords = expectRecords[1 : len(expectRecords)-1]
	ListCmp(t, list, key, expectRecords, false)
}

func TestList_LRemByIndex(t *testing.T) {
	list := newList()
	expectRecords := generateRecords(8)
	key := string(getTestBytes(0))
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	// r1 r2 r3 r4 r5 r6 r7 r8
	for i := 0; i < 8; i++ {
		seq := generateSeq(&seqInfo, false)
		newKey := encodeListKey([]byte(key), seq)
		expectRecords[i].Key = newKey
		ListPush(t, list, string(newKey), expectRecords[i], false, nil)
	}

	// r1 r2 r4 r5 r6 r7 r8
	err := list.lRemByIndex(key, []int{2})
	require.NoError(t, err)
	expectRecords = append(expectRecords[0:2], expectRecords[3:]...)
	ListCmp(t, list, key, expectRecords, false)

	// r2 r6 r7 r8
	err = list.lRemByIndex(key, []int{0, 2, 3})
	require.NoError(t, err)
	expectRecords = expectRecords[1:]
	expectRecords = append(expectRecords[0:1], expectRecords[3:]...)
	ListCmp(t, list, key, expectRecords, false)

	err = list.lRemByIndex(key, []int{0, 0, 0})
	require.NoError(t, err)
	expectRecords = expectRecords[1:]
	ListCmp(t, list, key, expectRecords, false)
}

func generateRecords(count int) []*record {
	rand.Seed(time.Now().UnixNano())
	records := make([]*record, count)
	for i := 0; i < count; i++ {
		key := getTestBytes(i)
		val := getRandomBytes(24)

		record := &record{
			Key:       key,
			Value:     val,
			FileID:    int64(i),
			DataPos:   uint64(rand.Uint32()),
			ValueSize: uint32(len(val)),
			Timestamp: uint64(time.Now().Unix()),
			TTL:       uint32(rand.Intn(3600)),
			TxID:      uint64(rand.Intn(1000)),
		}
		records[i] = record
	}
	return records
}
