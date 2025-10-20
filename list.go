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

package nutsdb

import (
	"errors"
	"time"

	"github.com/nutsdb/nutsdb/internal/data"
	"github.com/nutsdb/nutsdb/internal/utils"
)

var (
	// ErrListNotFound is returned when the list not found.
	ErrListNotFound = errors.New("the list not found")

	// ErrCount is returned when count is error.
	ErrCount = errors.New("err count")

	// ErrEmptyList is returned when the list is empty.
	ErrEmptyList = errors.New("the list is empty")

	// ErrStartOrEnd is returned when start > end
	ErrStartOrEnd = errors.New("start or end error")
)

const (
	initialListSeq = data.InitialListSeq
)

// ListStructure defines the interface for List storage implementations.
// It supports multiple implementations: BTree, DoublyLinkedList, SkipList, etc.
// This interface enables users to choose the most suitable implementation based on their use case:
// - DoublyLinkedList: O(1) head/tail operations, optimal for LPush/RPush/LPop/RPop
// - BTree: O(log n) operations, better for range queries and random access
type ListStructure interface {
	// InsertRecord inserts a record with the given key (sequence number in big-endian format).
	// Returns true if an existing record was replaced, false if a new record was inserted.
	InsertRecord(key []byte, record *data.Record) bool

	// Delete removes the record with the given key.
	// Returns true if the record was found and deleted, false otherwise.
	Delete(key []byte) bool

	// Find retrieves the record with the given key.
	// Returns the record and true if found, nil and false otherwise.
	Find(key []byte) (*data.Record, bool)

	// Min returns the item with the smallest key (head of the list).
	// Returns the item and true if the list is not empty, nil and false otherwise.
	Min() (*data.Item[data.Record], bool)

	// Max returns the item with the largest key (tail of the list).
	// Returns the item and true if the list is not empty, nil and false otherwise.
	Max() (*data.Item[data.Record], bool)

	// All returns all records in ascending key order.
	All() []*data.Record

	// AllItems returns all items (key + record pairs) in ascending key order.
	AllItems() []*data.Item[data.Record]

	// Count returns the number of elements in the list.
	Count() int

	// Range returns records with keys in the range [start, end] (inclusive).
	Range(start, end []byte) []*data.Record

	// PrefixScan scans records with keys matching the given prefix.
	// offset: number of matching records to skip
	// limitNum: maximum number of records to return
	PrefixScan(prefix []byte, offset, limitNum int) []*data.Record

	// PrefixSearchScan scans records with keys matching the given prefix and regex pattern.
	// The regex is applied to the portion of the key after removing the prefix.
	// offset: number of matching records to skip
	// limitNum: maximum number of records to return
	PrefixSearchScan(prefix []byte, reg string, offset, limitNum int) []*data.Record

	// PopMin removes and returns the item with the smallest key.
	// Returns the item and true if the list is not empty, nil and false otherwise.
	PopMin() (*data.Item[data.Record], bool)

	// PopMax removes and returns the item with the largest key.
	// Returns the item and true if the list is not empty, nil and false otherwise.
	PopMax() (*data.Item[data.Record], bool)
}

// Compile-time interface implementation checks
var (
	_ ListStructure = (*data.BTree)(nil)
	_ ListStructure = (*data.DoublyLinkedList)(nil)
)

// BTree represents the btree.

// HeadTailSeq list head and tail seq num
type HeadTailSeq struct {
	Head uint64
	Tail uint64
}

// List represents the list.
type List struct {
	Items     map[string]ListStructure
	TTL       map[string]uint32
	TimeStamp map[string]uint64
	Seq       map[string]*HeadTailSeq
	opts      Options // Configuration options
}

func NewList(opts Options) *List {
	return &List{
		Items:     make(map[string]ListStructure),
		TTL:       make(map[string]uint32),
		TimeStamp: make(map[string]uint64),
		Seq:       make(map[string]*HeadTailSeq),
		opts:      opts,
	}
}

// createListStructure creates a new list storage structure based on configuration.
func (l *List) createListStructure() ListStructure {
	switch l.opts.ListImpl {
	case ListImplBTree:
		return data.NewBTree()
	case ListImplDoublyLinkedList:
		return data.NewDoublyLinkedList()
	default:
		// Default to DoublyLinkedList for safety
		return data.NewDoublyLinkedList()
	}
}

func (l *List) LPush(key string, r *data.Record) error {
	return l.push(key, r, true)
}

func (l *List) RPush(key string, r *data.Record) error {
	return l.push(key, r, false)
}

func (l *List) push(key string, r *data.Record, isLeft bool) error {
	// key is seq + user_key
	userKey, curSeq := decodeListKey([]byte(key))
	userKeyStr := string(userKey)
	if l.IsExpire(userKeyStr) {
		return ErrListNotFound
	}

	list, ok := l.Items[userKeyStr]
	if !ok {
		l.Items[userKeyStr] = l.createListStructure()
		list = l.Items[userKeyStr]
	}

	// Initialize seq if not exists
	if _, ok := l.Seq[userKeyStr]; !ok {
		l.Seq[userKeyStr] = &HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}
	}

	list.InsertRecord(utils.ConvertUint64ToBigEndianBytes(curSeq), r)

	// Update seq boundaries to track the next insertion positions
	// This is important for recovery scenarios where we rebuild the index
	// Head and Tail should always represent the next available positions for insertion
	seq := l.Seq[userKeyStr]
	if isLeft {
		// LPush: Head should be the next available position on the left
		// If current seq is the actual head, set Head to current seq - 1
		if curSeq <= seq.Head {
			seq.Head = curSeq - 1
		}
	} else {
		// RPush: Tail should be the next available position on the right
		// If current seq is at or beyond current tail, update Tail accordingly
		if curSeq >= seq.Tail {
			seq.Tail = curSeq + 1
		}
	}

	return nil
}

func (l *List) LPop(key string) (*data.Record, error) {
	if l.IsExpire(key) {
		return nil, ErrListNotFound
	}

	list, ok := l.Items[key]
	if !ok {
		return nil, ErrListNotFound
	}

	// Use PopMin for efficient O(1) head removal
	item, ok := list.PopMin()
	if !ok {
		return nil, ErrEmptyList
	}

	// After LPop, Head should point to the next element's position
	// Note: We don't update Head here because it represents "next push position"
	// The popped element's sequence is already consumed
	return item.Record, nil
}

// RPop removes and returns the last element of the list stored at key.
func (l *List) RPop(key string) (*data.Record, error) {
	if l.IsExpire(key) {
		return nil, ErrListNotFound
	}

	list, ok := l.Items[key]
	if !ok {
		return nil, ErrListNotFound
	}

	// Use PopMax for efficient O(1) tail removal
	item, ok := list.PopMax()
	if !ok {
		return nil, ErrEmptyList
	}

	// After RPop, Tail should point to the next element's position
	// Note: We don't update Tail here because it represents "next push position"
	// The popped element's sequence is already consumed
	return item.Record, nil
}

func (l *List) LPeek(key string) (*data.Item[data.Record], error) {
	return l.peek(key, true)
}

func (l *List) RPeek(key string) (*data.Item[data.Record], error) {
	return l.peek(key, false)
}

func (l *List) peek(key string, isLeft bool) (*data.Item[data.Record], error) {
	if l.IsExpire(key) {
		return nil, ErrListNotFound
	}
	list, ok := l.Items[key]
	if !ok {
		return nil, ErrListNotFound
	}

	if isLeft {
		item, ok := list.Min()
		if ok {
			return item, nil
		}
	} else {
		item, ok := list.Max()
		if ok {
			return item, nil
		}
	}

	return nil, ErrEmptyList
}

// LRange returns the specified elements of the list stored at key [start,end]
func (l *List) LRange(key string, start, end int) ([]*data.Record, error) {
	size, err := l.Size(key)
	if err != nil || size == 0 {
		return nil, err
	}

	start, end, err = checkBounds(start, end, size)
	if err != nil {
		return nil, err
	}

	var res []*data.Record
	allRecords := l.Items[key].All()
	for i, item := range allRecords {
		if i >= start && i <= end {
			res = append(res, item)
		}
	}

	return res, nil
}

// getRemoveIndexes returns a slice of indices to be removed from the list based on the count
func (l *List) getRemoveIndexes(key string, count int, cmp func(r *data.Record) (bool, error)) ([][]byte, error) {
	if l.IsExpire(key) {
		return nil, ErrListNotFound
	}

	list, ok := l.Items[key]

	if !ok {
		return nil, ErrListNotFound
	}

	var res [][]byte
	var allItems []*data.Item[data.Record]
	if count == 0 {
		count = list.Count()
	}

	allItems = l.Items[key].AllItems()
	if count > 0 {
		for _, item := range allItems {
			if count <= 0 {
				break
			}
			r := item.Record
			ok, err := cmp(r)
			if err != nil {
				return nil, err
			}
			if ok {
				res = append(res, item.Key)
				count--
			}
		}
	} else {
		for i := len(allItems) - 1; i >= 0; i-- {
			if count >= 0 {
				break
			}
			r := allItems[i].Record
			ok, err := cmp(r)
			if err != nil {
				return nil, err
			}
			if ok {
				res = append(res, allItems[i].Key)
				count++
			}
		}
	}

	return res, nil
}

// LRem removes the first count occurrences of elements equal to value from the list stored at key.
// The count argument influences the operation in the following ways:
// count > 0: Remove elements equal to value moving from head to tail.
// count < 0: Remove elements equal to value moving from tail to head.
// count = 0: Remove all elements equal to value.
func (l *List) LRem(key string, count int, cmp func(r *data.Record) (bool, error)) error {
	removeIndexes, err := l.getRemoveIndexes(key, count, cmp)
	if err != nil {
		return err
	}

	list := l.Items[key]
	for _, idx := range removeIndexes {
		list.Delete(idx)
	}

	return nil
}

// LTrim trim an existing list so that it will contain only the specified range of elements specified.
func (l *List) LTrim(key string, start, end int) error {
	if l.IsExpire(key) {
		return ErrListNotFound
	}
	if _, ok := l.Items[key]; !ok {
		return ErrListNotFound
	}

	list := l.Items[key]
	allItems := list.AllItems()
	for i, item := range allItems {
		if i < start || i > end {
			list.Delete(item.Key)
		}
	}

	return nil
}

// LRemByIndex remove the list element at specified index
func (l *List) LRemByIndex(key string, indexes []int) error {
	if l.IsExpire(key) {
		return ErrListNotFound
	}

	idxes := l.getValidIndexes(key, indexes)
	if len(idxes) == 0 {
		return nil
	}

	list := l.Items[key]
	allItems := list.AllItems()
	for i, item := range allItems {
		if _, ok := idxes[i]; ok {
			list.Delete(item.Key)
		}
	}

	return nil
}

func (l *List) getValidIndexes(key string, indexes []int) map[int]struct{} {
	idxes := make(map[int]struct{})
	listLen, err := l.Size(key)
	if err != nil || listLen == 0 {
		return idxes
	}

	for _, idx := range indexes {
		if idx < 0 || idx >= listLen {
			continue
		}
		idxes[idx] = struct{}{}
	}

	return idxes
}

func (l *List) IsExpire(key string) bool {
	if l == nil {
		return false
	}

	_, ok := l.TTL[key]
	if !ok {
		return false
	}

	now := time.Now().Unix()
	timestamp := l.TimeStamp[key]
	if l.TTL[key] > 0 && uint64(l.TTL[key])+timestamp > uint64(now) || l.TTL[key] == uint32(0) {
		return false
	}

	delete(l.Items, key)
	delete(l.TTL, key)
	delete(l.TimeStamp, key)
	delete(l.Seq, key)

	return true
}

func (l *List) Size(key string) (int, error) {
	if l.IsExpire(key) {
		return 0, ErrListNotFound
	}
	if _, ok := l.Items[key]; !ok {
		return 0, ErrListNotFound
	}

	return l.Items[key].Count(), nil
}

func (l *List) IsEmpty(key string) (bool, error) {
	size, err := l.Size(key)
	if err != nil || size > 0 {
		return false, err
	}
	return true, nil
}

func (l *List) GetListTTL(key string) (uint32, error) {
	if l.IsExpire(key) {
		return 0, ErrListNotFound
	}

	ttl := l.TTL[key]
	timestamp := l.TimeStamp[key]
	if ttl == 0 || timestamp == 0 {
		return 0, nil
	}

	now := time.Now().Unix()
	remain := timestamp + uint64(ttl) - uint64(now)

	return uint32(remain), nil
}

func checkBounds(start, end int, size int) (int, int, error) {
	if start >= 0 && end < 0 {
		end = size + end
	}

	if start < 0 && end > 0 {
		start = size + start
	}

	if start < 0 && end < 0 {
		start, end = size+start, size+end
	}

	if end >= size {
		end = size - 1
	}

	if start > end {
		return 0, 0, ErrStartOrEnd
	}

	return start, end, nil
}
