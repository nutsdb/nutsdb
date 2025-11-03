// Copyright 2023 The nutsdb Author. All rights reserved.
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

package data_test

import (
	"bytes"
	"testing"

	"github.com/nutsdb/nutsdb/internal/data"
	"github.com/nutsdb/nutsdb/internal/testutils"
	"github.com/nutsdb/nutsdb/internal/utils"
	"github.com/stretchr/testify/require"
)

func ListPush(t *testing.T, list *data.List, key string, r *data.Record, isLeft bool, expectError error) {
	var e error
	if isLeft {
		e = list.LPush(key, r)
	} else {
		e = list.RPush(key, r)
	}
	testutils.AssertErr(t, e, expectError)
}

func ListPop(t *testing.T, list *data.List, key string, isLeft bool, expectVal *data.Record, expectError error) {
	var (
		e error
		r *data.Record
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

func ListCmp(t *testing.T, list *data.List, key string, expectRecords []*data.Record, isReverse bool) {
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
	for _, listImpl := range []data.ListImplementationType{data.ListImplDoublyLinkedList, data.ListImplBTree} {
		list := data.NewList(listImpl)
		// 测试 LPush
		key := string(testutils.GetTestBytes(0))
		expectRecords := data.GenerateRecords(5)
		seqInfo := data.HeadTailSeq{Head: data.InitialListSeq, Tail: data.InitialListSeq + 1}

		for i := 0; i < len(expectRecords); i++ {
			seq := seqInfo.GenerateSeq(true)
			newKey := utils.EncodeListKey([]byte(key), seq)
			expectRecords[i].Key = newKey
			ListPush(t, list, string(newKey), expectRecords[i], true, nil)
		}

		ListCmp(t, list, key, expectRecords, true)
	}
}

func TestList_RPush(t *testing.T) {
	for _, listImpl := range []data.ListImplementationType{data.ListImplDoublyLinkedList, data.ListImplBTree} {
		list := data.NewList(listImpl)
		// 测试 RPush
		key := string(testutils.GetTestBytes(0))
		expectRecords := data.GenerateRecords(5)
		seqInfo := data.HeadTailSeq{Head: data.InitialListSeq, Tail: data.InitialListSeq + 1}

		for i := 0; i < len(expectRecords); i++ {
			seq := seqInfo.GenerateSeq(false)
			newKey := utils.EncodeListKey([]byte(key), seq)
			expectRecords[i].Key = newKey
			ListPush(t, list, string(newKey), expectRecords[i], false, nil)
		}

		ListCmp(t, list, key, expectRecords, false)
	}
}

func TestList_Pop(t *testing.T) {
	for _, listImpl := range []data.ListImplementationType{data.ListImplDoublyLinkedList, data.ListImplBTree} {
		list := data.NewList(listImpl)
		expectRecords := data.GenerateRecords(5)
		key := string(testutils.GetTestBytes(0))

		ListPop(t, list, key, true, nil, data.ErrListNotFound)
		seqInfo := data.HeadTailSeq{Head: data.InitialListSeq, Tail: data.InitialListSeq + 1}

		for i := 0; i < len(expectRecords); i++ {
			seq := seqInfo.GenerateSeq(false)
			newKey := utils.EncodeListKey([]byte(key), seq)
			expectRecords[i].Key = newKey
			ListPush(t, list, string(newKey), expectRecords[i], false, nil)
		}

		ListPop(t, list, key, true, expectRecords[0], nil)
		expectRecords = expectRecords[1:]

		ListPop(t, list, key, false, expectRecords[len(expectRecords)-1], nil)
		expectRecords = expectRecords[:len(expectRecords)-1]

		ListCmp(t, list, key, expectRecords, false)
	}
}

func TestList_LRem(t *testing.T) {
	for _, listImpl := range []data.ListImplementationType{data.ListImplDoublyLinkedList, data.ListImplBTree} {
		list := data.NewList(listImpl)
		records := data.GenerateRecords(2)
		key := string(testutils.GetTestBytes(0))
		seqInfo := data.HeadTailSeq{Head: data.InitialListSeq, Tail: data.InitialListSeq + 1}

		for i := 0; i < 3; i++ {
			seq := seqInfo.GenerateSeq(false)
			newKey := utils.EncodeListKey([]byte(key), seq)
			records[0].Key = newKey
			ListPush(t, list, string(newKey), records[0], false, nil)
		}

		seq := seqInfo.GenerateSeq(false)
		newKey := utils.EncodeListKey([]byte(key), seq)
		records[1].Key = newKey
		ListPush(t, list, string(newKey), records[1], false, nil)

		seq = seqInfo.GenerateSeq(false)
		newKey = utils.EncodeListKey([]byte(key), seq)
		records[0].Key = newKey
		ListPush(t, list, string(newKey), records[0], false, nil)

		seq = seqInfo.GenerateSeq(false)
		newKey = utils.EncodeListKey([]byte(key), seq)
		records[1].Key = newKey
		ListPush(t, list, string(newKey), records[1], false, nil)

		// r1 r1 r1 r2 r1 r2
		expectRecords := []*data.Record{records[0], records[0], records[0], records[1], records[0], records[1]}

		cmp := func(r *data.Record) (bool, error) {
			return bytes.Equal(r.Value, records[0].Value), nil
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

		cmp = func(r *data.Record) (bool, error) {
			return bytes.Equal(r.Value, records[1].Value), nil
		}

		// r1
		err = list.LRem(key, 0, cmp)
		require.NoError(t, err)
		expectRecords = expectRecords[0:1]
		ListCmp(t, list, key, expectRecords, false)
	}
}

func TestList_LTrim(t *testing.T) {
	for _, listImpl := range []data.ListImplementationType{data.ListImplDoublyLinkedList, data.ListImplBTree} {
		list := data.NewList(listImpl)
		expectRecords := data.GenerateRecords(5)
		key := string(testutils.GetTestBytes(0))
		seqInfo := data.HeadTailSeq{Head: data.InitialListSeq, Tail: data.InitialListSeq + 1}

		for i := 0; i < len(expectRecords); i++ {
			seq := seqInfo.GenerateSeq(false)
			newKey := utils.EncodeListKey([]byte(key), seq)
			expectRecords[i].Key = newKey
			ListPush(t, list, string(newKey), expectRecords[i], false, nil)
		}

		err := list.LTrim(key, 1, 3)
		require.NoError(t, err)
		expectRecords = expectRecords[1 : len(expectRecords)-1]
		ListCmp(t, list, key, expectRecords, false)
	}
}

func TestList_LRemByIndex(t *testing.T) {
	for _, listImpl := range []data.ListImplementationType{data.ListImplDoublyLinkedList, data.ListImplBTree} {
		list := data.NewList(listImpl)
		expectRecords := data.GenerateRecords(8)
		key := string(testutils.GetTestBytes(0))
		seqInfo := data.HeadTailSeq{Head: data.InitialListSeq, Tail: data.InitialListSeq + 1}

		// r1 r2 r3 r4 r5 r6 r7 r8
		for i := 0; i < 8; i++ {
			seq := seqInfo.GenerateSeq(false)
			newKey := utils.EncodeListKey([]byte(key), seq)
			expectRecords[i].Key = newKey
			ListPush(t, list, string(newKey), expectRecords[i], false, nil)
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
}

// TestList_SequenceConsistency tests sequence number consistency
func TestList_SequenceConsistency(t *testing.T) {
	for _, listImpl := range []data.ListImplementationType{data.ListImplDoublyLinkedList, data.ListImplBTree} {
		list := data.NewList(listImpl)
		key := string(testutils.GetTestBytes(0))
		seqInfo := data.HeadTailSeq{Head: data.InitialListSeq, Tail: data.InitialListSeq + 1}

		// Push 5 elements
		for i := 0; i < 5; i++ {
			seq := seqInfo.GenerateSeq(false)
			newKey := utils.EncodeListKey([]byte(key), seq)
			record := &data.Record{Key: newKey, Value: testutils.GetTestBytes(i)}
			ListPush(t, list, string(newKey), record, false, nil)
		}

		// Verify Head and Tail
		require.Equal(t, uint64(data.InitialListSeq), seqInfo.Head, "Head should not change for RPush")
		require.Equal(t, uint64(data.InitialListSeq+6), seqInfo.Tail, "Tail should increment")

		// Pop from left
		ListPop(t, list, key, true, &data.Record{
			Key:   utils.EncodeListKey([]byte(key), data.InitialListSeq+1),
			Value: testutils.GetTestBytes(0),
		}, nil)

		// Push again - should not reuse old sequence
		seq := seqInfo.GenerateSeq(false)
		newKey := utils.EncodeListKey([]byte(key), seq)
		record := &data.Record{Key: newKey, Value: testutils.GetTestBytes(99)}
		ListPush(t, list, string(newKey), record, false, nil)

		// Verify final state
		size, err := list.Size(key)
		require.NoError(t, err)
		require.Equal(t, 5, size)
	}
}

// TestList_MixedPushPop tests mixed LPush/RPush/LPop/RPop operations
func TestList_MixedPushPop(t *testing.T) {
	for _, listImpl := range []data.ListImplementationType{data.ListImplDoublyLinkedList, data.ListImplBTree} {
		list := data.NewList(listImpl)
		key := string(testutils.GetTestBytes(0))
		seqInfo := data.HeadTailSeq{Head: data.InitialListSeq, Tail: data.InitialListSeq + 1}

		operations := []struct {
			op    string
			value int
		}{
			{"RPush", 1},
			{"RPush", 2},
			{"LPush", 0},
			{"RPush", 3},
			{"LPop", -1}, // Should remove 0
			{"LPush", -1},
			{"RPop", -1}, // Should remove 3
			{"RPush", 4},
		}

		for _, operation := range operations {
			switch operation.op {
			case "RPush":
				seq := seqInfo.GenerateSeq(false)
				newKey := utils.EncodeListKey([]byte(key), seq)
				record := &data.Record{Key: newKey, Value: testutils.GetTestBytes(operation.value)}
				ListPush(t, list, string(newKey), record, false, nil)
			case "LPush":
				seq := seqInfo.GenerateSeq(true)
				newKey := utils.EncodeListKey([]byte(key), seq)
				record := &data.Record{Key: newKey, Value: testutils.GetTestBytes(operation.value)}
				ListPush(t, list, string(newKey), record, true, nil)
			case "LPop":
				_, err := list.LPop(key)
				require.NoError(t, err)
			case "RPop":
				_, err := list.RPop(key)
				require.NoError(t, err)
			}
		}

		// Expected: [-1, 1, 2, 4]
		records, err := list.LRange(key, 0, -1)
		require.NoError(t, err)
		require.Equal(t, 4, len(records))

		expected := []int{-1, 1, 2, 4}
		for i, exp := range expected {
			require.Equal(t, testutils.GetTestBytes(exp), records[i].Value)
		}
	}
}

// TestList_HeadTailBoundary tests Head and Tail boundary updates
func TestList_HeadTailBoundary(t *testing.T) {
	for _, listImpl := range []data.ListImplementationType{data.ListImplDoublyLinkedList, data.ListImplBTree} {
		list := data.NewList(listImpl)
		key := string(testutils.GetTestBytes(0))
		seqInfo := data.HeadTailSeq{Head: data.InitialListSeq, Tail: data.InitialListSeq + 1}

		// Only RPush
		for i := 0; i < 3; i++ {
			seq := seqInfo.GenerateSeq(false)
			newKey := utils.EncodeListKey([]byte(key), seq)
			record := &data.Record{Key: newKey, Value: testutils.GetTestBytes(i)}
			ListPush(t, list, string(newKey), record, false, nil)
		}

		// Verify Tail moved, Head didn't
		require.Equal(t, uint64(data.InitialListSeq), seqInfo.Head)
		require.Equal(t, uint64(data.InitialListSeq+4), seqInfo.Tail)

		// Now LPush
		for i := 0; i < 3; i++ {
			seq := seqInfo.GenerateSeq(true)
			newKey := utils.EncodeListKey([]byte(key), seq)
			record := &data.Record{Key: newKey, Value: testutils.GetTestBytes(100 + i)}
			ListPush(t, list, string(newKey), record, true, nil)
		}

		// Verify Head moved, Tail didn't
		require.Equal(t, uint64(data.InitialListSeq-3), seqInfo.Head)
		require.Equal(t, uint64(data.InitialListSeq+4), seqInfo.Tail)
	}
}
