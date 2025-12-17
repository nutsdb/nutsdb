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
	"testing"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/data"
	"github.com/nutsdb/nutsdb/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDoublyLinkedList_InsertAndFind(t *testing.T) {
	dll := data.NewDoublyLinkedList()

	// Insert records with sequence numbers
	r1 := &core.Record{Key: []byte("key1"), Value: []byte("value1")}
	r2 := &core.Record{Key: []byte("key2"), Value: []byte("value2")}
	r3 := &core.Record{Key: []byte("key3"), Value: []byte("value3")}

	seq1 := utils.ConvertUint64ToBigEndianBytes(100)
	seq2 := utils.ConvertUint64ToBigEndianBytes(200)
	seq3 := utils.ConvertUint64ToBigEndianBytes(150)

	dll.InsertRecord(seq1, r1)
	dll.InsertRecord(seq2, r2)
	dll.InsertRecord(seq3, r3) // Insert in middle

	// Verify find
	found, ok := dll.Find(seq1)
	require.True(t, ok)
	assert.Equal(t, r1, found)

	found, ok = dll.Find(seq3)
	require.True(t, ok)
	assert.Equal(t, r3, found)

	// Verify count
	assert.Equal(t, 3, dll.Count())
}

func TestDoublyLinkedList_OrderedInsertion(t *testing.T) {
	dll := data.NewDoublyLinkedList()

	// Insert in non-sorted order
	sequences := []uint64{150, 100, 200, 50, 175}
	records := make([]*core.Record, len(sequences))

	for i, seq := range sequences {
		records[i] = &core.Record{
			Key:   []byte("key"),
			Value: []byte{byte(seq)},
		}
		dll.InsertRecord(utils.ConvertUint64ToBigEndianBytes(seq), records[i])
	}

	// Verify they are stored in sorted order
	items := dll.AllItems()
	assert.Equal(t, 5, len(items))

	// Should be: 50, 100, 150, 175, 200
	assert.Equal(t, uint64(50), utils.ConvertBigEndianBytesToUint64(items[0].Key))
	assert.Equal(t, uint64(100), utils.ConvertBigEndianBytesToUint64(items[1].Key))
	assert.Equal(t, uint64(150), utils.ConvertBigEndianBytesToUint64(items[2].Key))
	assert.Equal(t, uint64(175), utils.ConvertBigEndianBytesToUint64(items[3].Key))
	assert.Equal(t, uint64(200), utils.ConvertBigEndianBytesToUint64(items[4].Key))
}

func TestDoublyLinkedList_MinMax(t *testing.T) {
	dll := data.NewDoublyLinkedList()

	// Empty list
	_, ok := dll.Min()
	assert.False(t, ok)
	_, ok = dll.Max()
	assert.False(t, ok)

	// Add elements
	r1 := &core.Record{Value: []byte("value1")}
	r2 := &core.Record{Value: []byte("value2")}
	r3 := &core.Record{Value: []byte("value3")}

	dll.InsertRecord(utils.ConvertUint64ToBigEndianBytes(100), r1)
	dll.InsertRecord(utils.ConvertUint64ToBigEndianBytes(200), r2)
	dll.InsertRecord(utils.ConvertUint64ToBigEndianBytes(150), r3)

	// Check min
	minItem, ok := dll.Min()
	require.True(t, ok)
	assert.Equal(t, r1, minItem.Record)
	assert.Equal(t, uint64(100), utils.ConvertBigEndianBytesToUint64(minItem.Key))

	// Check max
	maxItem, ok := dll.Max()
	require.True(t, ok)
	assert.Equal(t, r2, maxItem.Record)
	assert.Equal(t, uint64(200), utils.ConvertBigEndianBytesToUint64(maxItem.Key))
}

func TestDoublyLinkedList_Delete(t *testing.T) {
	dll := data.NewDoublyLinkedList()

	r1 := &core.Record{Value: []byte("value1")}
	r2 := &core.Record{Value: []byte("value2")}
	r3 := &core.Record{Value: []byte("value3")}

	seq1 := utils.ConvertUint64ToBigEndianBytes(100)
	seq2 := utils.ConvertUint64ToBigEndianBytes(200)
	seq3 := utils.ConvertUint64ToBigEndianBytes(150)

	dll.InsertRecord(seq1, r1)
	dll.InsertRecord(seq2, r2)
	dll.InsertRecord(seq3, r3)

	assert.Equal(t, 3, dll.Count())

	// Delete middle element
	deleted := dll.Delete(seq3)
	assert.True(t, deleted)
	assert.Equal(t, 2, dll.Count())

	// Verify order is maintained
	items := dll.AllItems()
	assert.Equal(t, 2, len(items))
	assert.Equal(t, uint64(100), utils.ConvertBigEndianBytesToUint64(items[0].Key))
	assert.Equal(t, uint64(200), utils.ConvertBigEndianBytesToUint64(items[1].Key))

	// Delete head
	deleted = dll.Delete(seq1)
	assert.True(t, deleted)
	assert.Equal(t, 1, dll.Count())

	minItem, ok := dll.Min()
	require.True(t, ok)
	assert.Equal(t, r2, minItem.Record)

	// Delete tail
	deleted = dll.Delete(seq2)
	assert.True(t, deleted)
	assert.Equal(t, 0, dll.Count())

	_, ok = dll.Min()
	assert.False(t, ok)
	_, ok = dll.Max()
	assert.False(t, ok)
}

func TestDoublyLinkedList_PopMinMax(t *testing.T) {
	dll := data.NewDoublyLinkedList()

	r1 := &core.Record{Value: []byte("value1")}
	r2 := &core.Record{Value: []byte("value2")}
	r3 := &core.Record{Value: []byte("value3")}

	dll.InsertRecord(utils.ConvertUint64ToBigEndianBytes(100), r1)
	dll.InsertRecord(utils.ConvertUint64ToBigEndianBytes(200), r2)
	dll.InsertRecord(utils.ConvertUint64ToBigEndianBytes(150), r3)

	// Pop min
	minItem, ok := dll.PopMin()
	require.True(t, ok)
	assert.Equal(t, r1, minItem.Record)
	assert.Equal(t, 2, dll.Count())

	// Pop max
	maxItem, ok := dll.PopMax()
	require.True(t, ok)
	assert.Equal(t, r2, maxItem.Record)
	assert.Equal(t, 1, dll.Count())

	// Only one element left
	lastItem, ok := dll.PopMin()
	require.True(t, ok)
	assert.Equal(t, r3, lastItem.Record)
	assert.Equal(t, 0, dll.Count())

	// Empty now
	_, ok = dll.PopMin()
	assert.False(t, ok)
	_, ok = dll.PopMax()
	assert.False(t, ok)
}

func TestDoublyLinkedList_All(t *testing.T) {
	dll := data.NewDoublyLinkedList()

	records := []*core.Record{
		{Value: []byte("value1")},
		{Value: []byte("value2")},
		{Value: []byte("value3")},
	}

	dll.InsertRecord(utils.ConvertUint64ToBigEndianBytes(150), records[1])
	dll.InsertRecord(utils.ConvertUint64ToBigEndianBytes(100), records[0])
	dll.InsertRecord(utils.ConvertUint64ToBigEndianBytes(200), records[2])

	all := dll.All()
	require.Equal(t, 3, len(all))

	// Should be in sorted order: 100, 150, 200
	assert.Equal(t, records[0], all[0])
	assert.Equal(t, records[1], all[1])
	assert.Equal(t, records[2], all[2])
}

func TestDoublyLinkedList_Range(t *testing.T) {
	dll := data.NewDoublyLinkedList()

	for i := uint64(0); i < 10; i++ {
		r := &core.Record{Value: []byte{byte(i * 10)}}
		dll.InsertRecord(utils.ConvertUint64ToBigEndianBytes(i*10), r)
	}

	// Range query
	start := utils.ConvertUint64ToBigEndianBytes(20)
	end := utils.ConvertUint64ToBigEndianBytes(60)

	records := dll.Range(start, end)
	require.Equal(t, 5, len(records)) // 20, 30, 40, 50, 60

	for i, r := range records {
		assert.Equal(t, byte((i+2)*10), r.Value[0])
	}
}

func BenchmarkDoublyLinkedList_InsertHead(b *testing.B) {
	dll := data.NewDoublyLinkedList()
	r := &core.Record{Key: []byte("key"), Value: []byte("value")}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq := data.InitialListSeq - uint64(i)
		dll.InsertRecord(utils.ConvertUint64ToBigEndianBytes(seq), r)
	}
}

func BenchmarkDoublyLinkedList_InsertTail(b *testing.B) {
	dll := data.NewDoublyLinkedList()
	r := &core.Record{Key: []byte("key"), Value: []byte("value")}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq := data.InitialListSeq + uint64(i)
		dll.InsertRecord(utils.ConvertUint64ToBigEndianBytes(seq), r)
	}
}

func BenchmarkDoublyLinkedList_PopMin(b *testing.B) {
	dll := data.NewDoublyLinkedList()
	r := &core.Record{Key: []byte("key"), Value: []byte("value")}

	// Pre-populate
	for i := 0; i < b.N; i++ {
		dll.InsertRecord(utils.ConvertUint64ToBigEndianBytes(uint64(i)), r)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dll.PopMin()
	}
}

func BenchmarkDoublyLinkedList_PopMax(b *testing.B) {
	dll := data.NewDoublyLinkedList()
	r := &core.Record{Key: []byte("key"), Value: []byte("value")}

	// Pre-populate
	for i := 0; i < b.N; i++ {
		dll.InsertRecord(utils.ConvertUint64ToBigEndianBytes(uint64(i)), r)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dll.PopMax()
	}
}
