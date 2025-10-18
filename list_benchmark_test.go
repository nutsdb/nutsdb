// Copyright 2025 The nutsdb Author. All rights reserved.
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
	"strconv"
	"testing"
)

// Benchmark comparison between BTree and DoublyLinkedList implementations

func benchmarkListPush(b *testing.B, impl ListImplementationType, isLeft bool) {
	opts := DefaultOptions
	opts.ListImpl = impl
	list := NewList(opts)

	key := []byte("benchmark_key")
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	// Pre-generate test data to avoid overhead in the benchmark loop
	testData := make([]struct {
		seq    uint64
		newKey []byte
		record *Record
	}, b.N)

	for i := 0; i < b.N; i++ {
		seq := generateSeq(&seqInfo, isLeft)
		newKey := encodeListKey(key, seq)
		testData[i].seq = seq
		testData[i].newKey = newKey
		testData[i].record = &Record{Key: newKey, Value: GetTestBytes(i)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := list.push(string(testData[i].newKey), testData[i].record, isLeft); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkList_LPush_DoublyLinkedList(b *testing.B) {
	benchmarkListPush(b, ListImplDoublyLinkedList, true)
}

func BenchmarkList_LPush_BTree(b *testing.B) {
	benchmarkListPush(b, ListImplBTree, true)
}

func BenchmarkList_RPush_DoublyLinkedList(b *testing.B) {
	benchmarkListPush(b, ListImplDoublyLinkedList, false)
}

func BenchmarkList_RPush_BTree(b *testing.B) {
	benchmarkListPush(b, ListImplBTree, false)
}

func benchmarkListPop(b *testing.B, impl ListImplementationType, isLeft bool) {
	opts := DefaultOptions
	opts.ListImpl = impl
	list := NewList(opts)

	key := []byte("benchmark_key")
	keyStr := string(key)
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	// Pre-generate test data
	testData := make([]struct {
		newKey []byte
		record *Record
	}, b.N)

	for i := 0; i < b.N; i++ {
		seq := generateSeq(&seqInfo, false)
		newKey := encodeListKey(key, seq)
		testData[i].newKey = newKey
		testData[i].record = &Record{Key: newKey, Value: GetTestBytes(i)}
	}

	// Pre-populate with pre-generated data
	for i := 0; i < b.N; i++ {
		if err := list.push(string(testData[i].newKey), testData[i].record, false); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if isLeft {
			if _, err := list.LPop(keyStr); err != nil {
				b.Fatal(err)
			}
		} else {
			if _, err := list.RPop(keyStr); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkList_LPop_DoublyLinkedList(b *testing.B) {
	benchmarkListPop(b, ListImplDoublyLinkedList, true)
}

func BenchmarkList_LPop_BTree(b *testing.B) {
	benchmarkListPop(b, ListImplBTree, true)
}

func BenchmarkList_RPop_DoublyLinkedList(b *testing.B) {
	benchmarkListPop(b, ListImplDoublyLinkedList, false)
}

func BenchmarkList_RPop_BTree(b *testing.B) {
	benchmarkListPop(b, ListImplBTree, false)
}

func benchmarkListRange(b *testing.B, impl ListImplementationType, size int) {
	opts := DefaultOptions
	opts.ListImpl = impl
	list := NewList(opts)

	key := []byte("benchmark_key")
	keyStr := string(key)
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	// Pre-generate test data
	testData := make([]struct {
		newKey []byte
		record *Record
	}, size)

	for i := 0; i < size; i++ {
		seq := generateSeq(&seqInfo, false)
		newKey := encodeListKey(key, seq)
		testData[i].newKey = newKey
		testData[i].record = &Record{Key: newKey, Value: GetTestBytes(i)}
	}

	// Pre-populate with pre-generated data
	for i := 0; i < size; i++ {
		if err := list.push(string(testData[i].newKey), testData[i].record, false); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := list.LRange(keyStr, 0, size-1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkList_LRange_DoublyLinkedList_1000(b *testing.B) {
	benchmarkListRange(b, ListImplDoublyLinkedList, 1000)
}

func BenchmarkList_LRange_BTree_1000(b *testing.B) {
	benchmarkListRange(b, ListImplBTree, 1000)
}

func BenchmarkList_LRange_DoublyLinkedList_10000(b *testing.B) {
	benchmarkListRange(b, ListImplDoublyLinkedList, 10000)
}

func BenchmarkList_LRange_BTree_10000(b *testing.B) {
	benchmarkListRange(b, ListImplBTree, 10000)
}

func benchmarkListPeek(b *testing.B, impl ListImplementationType, isLeft bool) {
	opts := DefaultOptions
	opts.ListImpl = impl
	list := NewList(opts)

	key := []byte("benchmark_key")
	keyStr := string(key)
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	// Pre-generate test data
	testData := make([]struct {
		newKey []byte
		record *Record
	}, b.N)

	for i := 0; i < b.N; i++ {
		seq := generateSeq(&seqInfo, false)
		newKey := encodeListKey(key, seq)
		testData[i].newKey = newKey
		testData[i].record = &Record{Key: newKey, Value: GetTestBytes(i)}
	}

	// Pre-populate with pre-generated data
	for i := 0; i < b.N; i++ {
		if err := list.push(string(testData[i].newKey), testData[i].record, false); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if isLeft {
			if _, err := list.LPeek(keyStr); err != nil {
				b.Fatal(err)
			}
		} else {
			if _, err := list.RPeek(keyStr); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkList_LPeek_DoublyLinkedList(b *testing.B) {
	benchmarkListPeek(b, ListImplDoublyLinkedList, true)
}

func BenchmarkList_LPeek_BTree(b *testing.B) {
	benchmarkListPeek(b, ListImplBTree, true)
}

func BenchmarkList_RPeek_DoublyLinkedList(b *testing.B) {
	benchmarkListPeek(b, ListImplDoublyLinkedList, false)
}

func BenchmarkList_RPeek_BTree(b *testing.B) {
	benchmarkListPeek(b, ListImplBTree, false)
}

func benchmarkListTrim(b *testing.B, impl ListImplementationType, size int, keepStart, keepEnd int) {
	opts := DefaultOptions
	opts.ListImpl = impl
	list := NewList(opts)

	key := []byte("benchmark_key")
	keyStr := string(key)
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	// Pre-generate test data
	testData := make([]struct {
		newKey []byte
		record *Record
	}, size)

	for i := 0; i < size; i++ {
		seq := generateSeq(&seqInfo, false)
		newKey := encodeListKey(key, seq)
		testData[i].newKey = newKey
		testData[i].record = &Record{Key: newKey, Value: GetTestBytes(i)}
	}

	// Pre-populate with pre-generated data
	for i := 0; i < size; i++ {
		if err := list.push(string(testData[i].newKey), testData[i].record, false); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create a fresh list for each iteration
		freshList := NewList(opts)
		freshList.Items[keyStr] = list.createListStructure()
		freshSeq := &HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}
		freshList.Seq[keyStr] = freshSeq

		// Populate fresh list
		for j := 0; j < size; j++ {
			if err := freshList.push(string(testData[j].newKey), testData[j].record, false); err != nil {
				b.Fatal(err)
			}
		}

		// Perform trim operation
		if err := freshList.LTrim(keyStr, keepStart, keepEnd); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkList_LTrim_DoublyLinkedList_1000(b *testing.B) {
	benchmarkListTrim(b, ListImplDoublyLinkedList, 1000, 100, 900)
}

func BenchmarkList_LTrim_BTree_1000(b *testing.B) {
	benchmarkListTrim(b, ListImplBTree, 1000, 100, 900)
}

func BenchmarkList_LTrim_DoublyLinkedList_10000(b *testing.B) {
	benchmarkListTrim(b, ListImplDoublyLinkedList, 10000, 1000, 9000)
}

func BenchmarkList_LTrim_BTree_10000(b *testing.B) {
	benchmarkListTrim(b, ListImplBTree, 10000, 1000, 9000)
}

func benchmarkListRem(b *testing.B, impl ListImplementationType, size int, count int) {
	opts := DefaultOptions
	opts.ListImpl = impl
	list := NewList(opts)

	key := []byte("benchmark_key")
	keyStr := string(key)
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	// Pre-generate test data
	testData := make([]struct {
		newKey []byte
		record *Record
	}, size)

	for i := 0; i < size; i++ {
		seq := generateSeq(&seqInfo, false)
		newKey := encodeListKey(key, seq)
		testData[i].newKey = newKey
		testData[i].record = &Record{Key: newKey, Value: GetTestBytes(i)}
	}

	// Pre-populate with pre-generated data
	for i := 0; i < size; i++ {
		if err := list.push(string(testData[i].newKey), testData[i].record, false); err != nil {
			b.Fatal(err)
		}
	}

	// Create a comparison function that matches every nth element
	targetIndex := size / 4 // Remove elements at 1/4 position
	cmpFunc := func(r *Record) (bool, error) {
		// Simple comparison based on value pattern
		return bytes.Contains(r.Value, []byte(strconv.Itoa(targetIndex))), nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create a fresh list for each iteration
		freshList := NewList(opts)
		freshList.Items[keyStr] = list.createListStructure()
		freshSeq := &HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}
		freshList.Seq[keyStr] = freshSeq

		// Populate fresh list
		for j := 0; j < size; j++ {
			if err := freshList.push(string(testData[j].newKey), testData[j].record, false); err != nil {
				b.Fatal(err)
			}
		}

		// Perform remove operation
		if err := freshList.LRem(keyStr, count, cmpFunc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkList_LRem_DoublyLinkedList_1000(b *testing.B) {
	benchmarkListRem(b, ListImplDoublyLinkedList, 1000, 10)
}

func BenchmarkList_LRem_BTree_1000(b *testing.B) {
	benchmarkListRem(b, ListImplBTree, 1000, 10)
}

func BenchmarkList_LRem_DoublyLinkedList_10000(b *testing.B) {
	benchmarkListRem(b, ListImplDoublyLinkedList, 10000, 100)
}

func BenchmarkList_LRem_BTree_10000(b *testing.B) {
	benchmarkListRem(b, ListImplBTree, 10000, 100)
}

func benchmarkListRemByIndex(b *testing.B, impl ListImplementationType, size int, numIndexes int) {
	opts := DefaultOptions
	opts.ListImpl = impl
	list := NewList(opts)

	key := []byte("benchmark_key")
	keyStr := string(key)
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	// Pre-generate test data
	testData := make([]struct {
		newKey []byte
		record *Record
	}, size)

	for i := 0; i < size; i++ {
		seq := generateSeq(&seqInfo, false)
		newKey := encodeListKey(key, seq)
		testData[i].newKey = newKey
		testData[i].record = &Record{Key: newKey, Value: GetTestBytes(i)}
	}

	// Pre-populate with pre-generated data
	for i := 0; i < size; i++ {
		if err := list.push(string(testData[i].newKey), testData[i].record, false); err != nil {
			b.Fatal(err)
		}
	}

	// Generate indexes to remove (spread across the list)
	indexes := make([]int, numIndexes)
	step := size / (numIndexes + 1)
	for i := 0; i < numIndexes; i++ {
		indexes[i] = (i + 1) * step
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create a fresh list for each iteration
		freshList := NewList(opts)
		freshList.Items[keyStr] = list.createListStructure()
		freshSeq := &HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}
		freshList.Seq[keyStr] = freshSeq

		// Populate fresh list
		for j := 0; j < size; j++ {
			if err := freshList.push(string(testData[j].newKey), testData[j].record, false); err != nil {
				b.Fatal(err)
			}
		}

		// Perform remove by index operation
		if err := freshList.LRemByIndex(keyStr, indexes); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkList_LRemByIndex_DoublyLinkedList_1000(b *testing.B) {
	benchmarkListRemByIndex(b, ListImplDoublyLinkedList, 1000, 10)
}

func BenchmarkList_LRemByIndex_BTree_1000(b *testing.B) {
	benchmarkListRemByIndex(b, ListImplBTree, 1000, 10)
}

func BenchmarkList_LRemByIndex_DoublyLinkedList_10000(b *testing.B) {
	benchmarkListRemByIndex(b, ListImplDoublyLinkedList, 10000, 100)
}

func BenchmarkList_LRemByIndex_BTree_10000(b *testing.B) {
	benchmarkListRemByIndex(b, ListImplBTree, 10000, 100)
}

func benchmarkListSize(b *testing.B, impl ListImplementationType, size int) {
	opts := DefaultOptions
	opts.ListImpl = impl
	list := NewList(opts)

	key := []byte("benchmark_key")
	keyStr := string(key)
	seqInfo := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}

	// Pre-generate test data
	testData := make([]struct {
		newKey []byte
		record *Record
	}, size)

	for i := 0; i < size; i++ {
		seq := generateSeq(&seqInfo, false)
		newKey := encodeListKey(key, seq)
		testData[i].newKey = newKey
		testData[i].record = &Record{Key: newKey, Value: GetTestBytes(i)}
	}

	// Pre-populate with pre-generated data
	for i := 0; i < size; i++ {
		if err := list.push(string(testData[i].newKey), testData[i].record, false); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := list.Size(keyStr); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkList_Size_DoublyLinkedList_1000(b *testing.B) {
	benchmarkListSize(b, ListImplDoublyLinkedList, 1000)
}

func BenchmarkList_Size_BTree_1000(b *testing.B) {
	benchmarkListSize(b, ListImplBTree, 1000)
}

func BenchmarkList_Size_DoublyLinkedList_10000(b *testing.B) {
	benchmarkListSize(b, ListImplDoublyLinkedList, 10000)
}

func BenchmarkList_Size_BTree_10000(b *testing.B) {
	benchmarkListSize(b, ListImplBTree, 10000)
}
