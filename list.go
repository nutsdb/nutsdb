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
	"math"
	"time"
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
	initialListSeq = math.MaxUint64 / 2
)

// BTree represents the btree.

// HeadTailSeq list head and tail seq num
type HeadTailSeq struct {
	Head uint64
	Tail uint64
}

// List represents the list.
type List struct {
	Items     map[string]*BTree
	TTL       map[string]uint32
	TimeStamp map[string]uint64
	Seq       map[string]*HeadTailSeq
}

func NewList() *List {
	return &List{
		Items:     make(map[string]*BTree),
		TTL:       make(map[string]uint32),
		TimeStamp: make(map[string]uint64),
		Seq:       make(map[string]*HeadTailSeq),
	}
}

func (l *List) LPush(key string, r *Record) error {
	return l.push(key, r, true)
}

func (l *List) RPush(key string, r *Record) error {
	return l.push(key, r, false)
}

func (l *List) push(key string, r *Record, isLeft bool) error {
	// key is seq + user_key
	userKey, curSeq := decodeListKey([]byte(key))
	userKeyStr := string(userKey)
	if l.IsExpire(userKeyStr) {
		return ErrListNotFound
	}

	list, ok := l.Items[userKeyStr]
	if !ok {
		l.Items[userKeyStr] = NewBTree()
		list = l.Items[userKeyStr]
	}

	seq, ok := l.Seq[userKeyStr]
	if !ok {
		l.Seq[userKeyStr] = &HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}
		seq = l.Seq[userKeyStr]
	}

	list.InsertRecord(ConvertUint64ToBigEndianBytes(curSeq), r)
	if isLeft {
		if seq.Head > curSeq-1 {
			seq.Head = curSeq - 1
		}
	} else {
		if seq.Tail < curSeq+1 {
			seq.Tail = curSeq + 1
		}
	}

	return nil
}

func (l *List) LPop(key string) (*Record, error) {
	item, err := l.LPeek(key)
	if err != nil {
		return nil, err
	}

	l.Items[key].Delete(item.key)
	l.Seq[key].Head = ConvertBigEndianBytesToUint64(item.key)
	return item.r, nil
}

// RPop removes and returns the last element of the list stored at key.
func (l *List) RPop(key string) (*Record, error) {
	item, err := l.RPeek(key)
	if err != nil {
		return nil, err
	}

	l.Items[key].Delete(item.key)
	l.Seq[key].Tail = ConvertBigEndianBytesToUint64(item.key)
	return item.r, nil
}

func (l *List) LPeek(key string) (*Item, error) {
	return l.peek(key, true)
}

func (l *List) RPeek(key string) (*Item, error) {
	return l.peek(key, false)
}

func (l *List) peek(key string, isLeft bool) (*Item, error) {
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
func (l *List) LRange(key string, start, end int) ([]*Record, error) {
	size, err := l.Size(key)
	if err != nil || size == 0 {
		return nil, err
	}

	start, end, err = checkBounds(start, end, size)
	if err != nil {
		return nil, err
	}

	var res []*Record
	allRecords := l.Items[key].All()
	for i, item := range allRecords {
		if i >= start && i <= end {
			res = append(res, item)
		}
	}

	return res, nil
}

// getRemoveIndexes returns a slice of indices to be removed from the list based on the count
func (l *List) getRemoveIndexes(key string, count int, cmp func(r *Record) (bool, error)) ([][]byte, error) {
	if l.IsExpire(key) {
		return nil, ErrListNotFound
	}

	list, ok := l.Items[key]

	if !ok {
		return nil, ErrListNotFound
	}

	var res [][]byte
	var allItems []*Item
	if 0 == count {
		count = list.Count()
	}

	allItems = l.Items[key].AllItems()
	if count > 0 {
		for _, item := range allItems {
			if count <= 0 {
				break
			}
			r := item.r
			ok, err := cmp(r)
			if err != nil {
				return nil, err
			}
			if ok {
				res = append(res, item.key)
				count--
			}
		}
	} else {
		for i := len(allItems) - 1; i >= 0; i-- {
			if count >= 0 {
				break
			}
			r := allItems[i].r
			ok, err := cmp(r)
			if err != nil {
				return nil, err
			}
			if ok {
				res = append(res, allItems[i].key)
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
func (l *List) LRem(key string, count int, cmp func(r *Record) (bool, error)) error {
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
			list.Delete(item.key)
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
			list.Delete(item.key)
		}
	}

	return nil
}

func (l *List) getValidIndexes(key string, indexes []int) map[int]struct{} {
	idxes := make(map[int]struct{})
	listLen, err := l.Size(key)
	if err != nil || 0 == listLen {
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
