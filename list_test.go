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
	"os"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/data"
	"github.com/nutsdb/nutsdb/internal/testutils"
	"github.com/stretchr/testify/require"
)

func ListPush(t *testing.T, list *List, key string, r *data.Record, isLeft bool, expectError error) {
	var e error
	if isLeft {
		e = list.LPush(key, r)
	} else {
		e = list.RPush(key, r)
	}
	assertErr(t, e, expectError)
}

func ListPop(t *testing.T, list *List, key string, isLeft bool, expectVal *data.Record, expectError error) {
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

func ListCmp(t *testing.T, list *List, key string, expectRecords []*data.Record, isReverse bool) {
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
	list := NewList(DefaultOptions)
	// 测试 LPush
	key := string(testutils.GetTestBytes(0))
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
	list := NewList(DefaultOptions)
	// 测试 RPush
	key := string(testutils.GetTestBytes(0))
	expectRecords := generateRecords(5)
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
	list := NewList(DefaultOptions)
	expectRecords := generateRecords(5)
	key := string(testutils.GetTestBytes(0))

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
	list := NewList(DefaultOptions)
	records := generateRecords(2)
	key := string(testutils.GetTestBytes(0))
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

func TestList_LTrim(t *testing.T) {
	list := NewList(DefaultOptions)
	expectRecords := generateRecords(5)
	key := string(testutils.GetTestBytes(0))
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	for i := 0; i < len(expectRecords); i++ {
		seq := generateSeq(&seqInfo, false)
		newKey := encodeListKey([]byte(key), seq)
		expectRecords[i].Key = newKey
		ListPush(t, list, string(newKey), expectRecords[i], false, nil)
	}

	err := list.LTrim(key, 1, 3)
	require.NoError(t, err)
	expectRecords = expectRecords[1 : len(expectRecords)-1]
	ListCmp(t, list, key, expectRecords, false)
}

func TestList_LRemByIndex(t *testing.T) {
	list := NewList(DefaultOptions)
	expectRecords := generateRecords(8)
	key := string(testutils.GetTestBytes(0))
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	// r1 r2 r3 r4 r5 r6 r7 r8
	for i := 0; i < 8; i++ {
		seq := generateSeq(&seqInfo, false)
		newKey := encodeListKey([]byte(key), seq)
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

func generateRecords(count int) []*data.Record {
	rand.Seed(time.Now().UnixNano())
	records := make([]*data.Record, count)
	for i := 0; i < count; i++ {
		key := testutils.GetTestBytes(i)
		val := testutils.GetRandomBytes(24)

		record := &data.Record{
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

// TestList_SequenceConsistency tests sequence number consistency
func TestList_SequenceConsistency(t *testing.T) {
	list := NewList(DefaultOptions)
	key := string(testutils.GetTestBytes(0))
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	// Push 5 elements
	for i := 0; i < 5; i++ {
		seq := generateSeq(&seqInfo, false)
		newKey := encodeListKey([]byte(key), seq)
		record := &data.Record{Key: newKey, Value: testutils.GetTestBytes(i)}
		ListPush(t, list, string(newKey), record, false, nil)
	}

	// Verify Head and Tail
	require.Equal(t, uint64(initialListSeq), seqInfo.Head, "Head should not change for RPush")
	require.Equal(t, uint64(initialListSeq+6), seqInfo.Tail, "Tail should increment")

	// Pop from left
	ListPop(t, list, key, true, &data.Record{
		Key:   encodeListKey([]byte(key), initialListSeq+1),
		Value: testutils.GetTestBytes(0),
	}, nil)

	// Push again - should not reuse old sequence
	seq := generateSeq(&seqInfo, false)
	newKey := encodeListKey([]byte(key), seq)
	record := &data.Record{Key: newKey, Value: testutils.GetTestBytes(99)}
	ListPush(t, list, string(newKey), record, false, nil)

	// Verify final state
	size, err := list.Size(key)
	require.NoError(t, err)
	require.Equal(t, 5, size)
}

// TestTx_PushPopPushSequence tests Push->Pop->Push sequence numbers
func TestTx_PushPopPushSequence(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)

		// Push 3 elements
		txPush(t, db, bucket, key, testutils.GetTestBytes(0), false, nil, nil)
		txPush(t, db, bucket, key, testutils.GetTestBytes(1), false, nil, nil)
		txPush(t, db, bucket, key, testutils.GetTestBytes(2), false, nil, nil)

		// Pop one from left
		txPop(t, db, bucket, key, testutils.GetTestBytes(0), nil, true)

		// Push another to right
		txPush(t, db, bucket, key, testutils.GetTestBytes(3), false, nil, nil)

		// Pop from right
		txPop(t, db, bucket, key, testutils.GetTestBytes(3), nil, false)

		// Push to left
		txPush(t, db, bucket, key, testutils.GetTestBytes(99), true, nil, nil)

		// Verify final order: [99, 1, 2]
		txLRange(t, db, bucket, key, 0, -1, 3, [][]byte{
			testutils.GetTestBytes(99), testutils.GetTestBytes(1), testutils.GetTestBytes(2),
		}, nil)
	})
}

// TestList_MixedPushPop tests mixed LPush/RPush/LPop/RPop operations
func TestList_MixedPushPop(t *testing.T) {
	list := NewList(DefaultOptions)
	key := string(testutils.GetTestBytes(0))
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

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
			seq := generateSeq(&seqInfo, false)
			newKey := encodeListKey([]byte(key), seq)
			record := &data.Record{Key: newKey, Value: testutils.GetTestBytes(operation.value)}
			ListPush(t, list, string(newKey), record, false, nil)
		case "LPush":
			seq := generateSeq(&seqInfo, true)
			newKey := encodeListKey([]byte(key), seq)
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

// TestList_HeadTailBoundary tests Head and Tail boundary updates
func TestList_HeadTailBoundary(t *testing.T) {
	list := NewList(DefaultOptions)
	key := string(testutils.GetTestBytes(0))
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	// Only RPush
	for i := 0; i < 3; i++ {
		seq := generateSeq(&seqInfo, false)
		newKey := encodeListKey([]byte(key), seq)
		record := &data.Record{Key: newKey, Value: testutils.GetTestBytes(i)}
		ListPush(t, list, string(newKey), record, false, nil)
	}

	// Verify Tail moved, Head didn't
	require.Equal(t, uint64(initialListSeq), seqInfo.Head)
	require.Equal(t, uint64(initialListSeq+4), seqInfo.Tail)

	// Now LPush
	for i := 0; i < 3; i++ {
		seq := generateSeq(&seqInfo, true)
		newKey := encodeListKey([]byte(key), seq)
		record := &data.Record{Key: newKey, Value: testutils.GetTestBytes(100 + i)}
		ListPush(t, list, string(newKey), record, true, nil)
	}

	// Verify Head moved, Tail didn't
	require.Equal(t, uint64(initialListSeq-3), seqInfo.Head)
	require.Equal(t, uint64(initialListSeq+4), seqInfo.Tail)
}

// TestTx_ListRecoveryAfterRestart tests that list data is correctly recovered after DB restart
func TestTx_ListRecoveryAfterRestart(t *testing.T) {
	bucket := "list_bucket"
	key := testutils.GetTestBytes(0)

	dir := "/tmp/test_nutsdb_list_recovery"
	defer os.RemoveAll(dir)

	// Step 1: Create DB and insert data
	opts := DefaultOptions
	opts.Dir = dir
	db, err := Open(opts)
	require.NoError(t, err)

	// Create bucket and insert data
	txCreateBucket(t, db, DataStructureList, bucket, nil)

	// Insert 10 elements using RPush
	for i := 0; i < 10; i++ {
		txPush(t, db, bucket, key, testutils.GetTestBytes(i), false, nil, nil)
	}

	// Verify data before closing
	txLSize(t, db, bucket, key, 10, nil)
	txLRange(t, db, bucket, key, 0, -1, 10, [][]byte{
		testutils.GetTestBytes(0), testutils.GetTestBytes(1), testutils.GetTestBytes(2), testutils.GetTestBytes(3), testutils.GetTestBytes(4),
		testutils.GetTestBytes(5), testutils.GetTestBytes(6), testutils.GetTestBytes(7), testutils.GetTestBytes(8), testutils.GetTestBytes(9),
	}, nil)

	// Step 2: Close DB
	err = db.Close()
	require.NoError(t, err)

	// Step 3: Reopen DB
	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()

	// Step 4: Verify data after recovery
	txLSize(t, db, bucket, key, 10, nil)
	txLRange(t, db, bucket, key, 0, -1, 10, [][]byte{
		testutils.GetTestBytes(0), testutils.GetTestBytes(1), testutils.GetTestBytes(2), testutils.GetTestBytes(3), testutils.GetTestBytes(4),
		testutils.GetTestBytes(5), testutils.GetTestBytes(6), testutils.GetTestBytes(7), testutils.GetTestBytes(8), testutils.GetTestBytes(9),
	}, nil)

	// Step 5: Test continued operations after recovery
	txPush(t, db, bucket, key, testutils.GetTestBytes(10), false, nil, nil)
	txPush(t, db, bucket, key, testutils.GetTestBytes(99), true, nil, nil)

	// Verify final state
	txLSize(t, db, bucket, key, 12, nil)
	txLRange(t, db, bucket, key, 0, 0, 1, [][]byte{testutils.GetTestBytes(99)}, nil)
	txLRange(t, db, bucket, key, 11, 11, 1, [][]byte{testutils.GetTestBytes(10)}, nil)
}

// TestTx_ListRecoveryWithMixedOperations tests recovery after complex operations
func TestTx_ListRecoveryWithMixedOperations(t *testing.T) {
	bucket := "list_bucket"
	key := testutils.GetTestBytes(0)

	dir := "/tmp/test_nutsdb_list_recovery_mixed"
	defer os.RemoveAll(dir)

	opts := DefaultOptions
	opts.Dir = dir
	db, err := Open(opts)
	require.NoError(t, err)

	// Create bucket
	txCreateBucket(t, db, DataStructureList, bucket, nil)

	// RPush 5 elements: [0,1,2,3,4]
	for i := 0; i < 5; i++ {
		txPush(t, db, bucket, key, testutils.GetTestBytes(i), false, nil, nil)
	}

	// LPush 2 elements: [98,99,0,1,2,3,4]
	txPush(t, db, bucket, key, testutils.GetTestBytes(99), true, nil, nil)
	txPush(t, db, bucket, key, testutils.GetTestBytes(98), true, nil, nil)

	// Close and reopen
	err = db.Close()
	require.NoError(t, err)

	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()

	// Verify recovered state - should have 7 elements
	txLSize(t, db, bucket, key, 7, nil)
	txLRange(t, db, bucket, key, 0, -1, 7, [][]byte{
		testutils.GetTestBytes(98), testutils.GetTestBytes(99), testutils.GetTestBytes(0), testutils.GetTestBytes(1),
		testutils.GetTestBytes(2), testutils.GetTestBytes(3), testutils.GetTestBytes(4),
	}, nil)
}

// TestTx_ListRecoveryMultipleLists tests recovery of multiple lists
func TestTx_ListRecoveryMultipleLists(t *testing.T) {
	bucket := "list_bucket"

	dir := "/tmp/test_nutsdb_list_recovery_multiple"
	defer os.RemoveAll(dir)

	opts := DefaultOptions
	opts.Dir = dir
	db, err := Open(opts)
	require.NoError(t, err)

	// Create bucket
	txCreateBucket(t, db, DataStructureList, bucket, nil)

	// Create multiple lists
	for listIdx := 0; listIdx < 3; listIdx++ {
		key := testutils.GetTestBytes(listIdx)
		for i := 0; i < 5; i++ {
			txPush(t, db, bucket, key, testutils.GetTestBytes(listIdx*100+i), false, nil, nil)
		}
	}

	// Close and reopen
	err = db.Close()
	require.NoError(t, err)

	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()

	// Verify all lists recovered correctly
	for listIdx := 0; listIdx < 3; listIdx++ {
		key := testutils.GetTestBytes(listIdx)
		txLSize(t, db, bucket, key, 5, nil)

		expected := make([][]byte, 5)
		for i := 0; i < 5; i++ {
			expected[i] = testutils.GetTestBytes(listIdx*100 + i)
		}
		txLRange(t, db, bucket, key, 0, -1, 5, expected, nil)
	}
}
