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
	dll "github.com/emirpasic/gods/lists/doublylinkedlist"
	"time"
)

var (
	// ErrListNotFound is returned when the list not found.
	ErrListNotFound = errors.New("the list not found")

	// ErrIndexOutOfRange is returned when use LSet function set index out of range.
	ErrIndexOutOfRange = errors.New("index out of range")
)

// List represents the list.
type List struct {
	Items     map[string]*dll.List
	TTL       map[string]uint32
	TimeStamp map[string]uint64
}

func NewList() *List {
	return &List{
		Items:     make(map[string]*dll.List),
		TTL:       make(map[string]uint32),
		TimeStamp: make(map[string]uint64),
	}
}

func (l *List) LPush(key string, r *Record) error {
	return l.push(key, r, true)
}

func (l *List) RPush(key string, r *Record) error {
	return l.push(key, r, false)
}

func (l *List) push(key string, r *Record, isLeft bool) error {
	if l.IsExpire(key) {
		return ErrListNotFound
	}

	list, ok := l.Items[key]
	if !ok {
		l.Items[key] = dll.New()
		list = l.Items[key]
	}

	if isLeft {
		list.Prepend(r)
	} else {
		list.Append(r)
	}

	return nil
}

func (l *List) LPop(key string) (*Record, error) {
	r, err := l.LPeek(key)
	if err != nil {
		return nil, err
	}

	l.Items[key].Remove(0)
	return r, nil
}

// RPop removes and returns the last element of the list stored at key.
func (l *List) RPop(key string) (*Record, error) {
	r, err := l.RPeek(key)
	if err != nil {
		return nil, err
	}

	l.Items[key].Remove(l.Items[key].Size() - 1)
	return r, nil
}

func (l *List) LPeek(key string) (*Record, error) {
	return l.peek(key, true)
}

func (l *List) RPeek(key string) (*Record, error) {
	return l.peek(key, false)
}

func (l *List) peek(key string, isLeft bool) (*Record, error) {
	if l.IsExpire(key) {
		return nil, ErrListNotFound
	}
	list, ok := l.Items[key]
	if !ok {
		return nil, ErrListNotFound
	}

	if isLeft {
		if r, ok := list.Get(0); ok {
			return r.(*Record), nil
		}
	} else {
		if r, ok := list.Get(l.Items[key].Size() - 1); ok {
			return r.(*Record), nil
		}
	}

	return nil, nil
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

	iterator := l.Items[key].Iterator()

	for i := 0; i <= start; i++ {
		iterator.Next()
	}

	items := make([]*Record, end-start+1)
	for i := start; i <= end; i++ {
		items[i-start] = iterator.Value().(*Record)
		iterator.Next()
	}

	return items, nil
}

// LRem removes the first count occurrences of elements equal to value from the list stored at key.
// The count argument influences the operation in the following ways:
// count > 0: Remove elements equal to value moving from head to tail.
// count < 0: Remove elements equal to value moving from tail to head.
// count = 0: Remove all elements equal to value.
func (l *List) LRem(key string, count int, cmp func(r *Record) (bool, error)) error {
	if l.IsExpire(key) {
		return ErrListNotFound
	}

	list, ok := l.Items[key]

	if !ok {
		return ErrListNotFound
	}

	iterator := list.Iterator()

	if count >= 0 {
		if count == 0 {
			count = list.Size()
		}
		iterator.Begin()

		for iterator.Next() && count > 0 {
			r := iterator.Value().(*Record)

			ok, err := cmp(r)
			if err != nil {
				return err
			}
			if ok {
				list.Remove(iterator.Index())
				count--
			}
		}
	}

	if count < 0 {
		iterator.End()

		for iterator.Prev() && count < 0 {
			r := iterator.Value().(*Record)

			ok, err := cmp(r)
			if err != nil {
				return err
			}
			if ok {
				list.Remove(iterator.Index())
				count++
			}
		}
	}

	return nil
}

// LSet sets the list element at index to value.
func (l *List) LSet(key string, index int, r *Record) error {
	if l.IsExpire(key) {
		return ErrListNotFound
	}
	if _, ok := l.Items[key]; !ok {
		return ErrListNotFound
	}

	size, _ := l.Size(key)
	if index >= size || index < 0 {
		return ErrIndexOutOfRange
	}

	l.Items[key].Set(index, r)

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

	items, err := l.LRange(key, start, end)
	if err != nil {
		return err
	}

	list := l.Items[key]

	list.Clear()
	for _, item := range items {
		list.Append(item)
	}

	return nil
}

// LRemByIndex remove the list element at specified index
func (l *List) LRemByIndex(key string, indexes []int) error {
	if l.IsExpire(key) {
		return ErrListNotFound
	}

	list := l.Items[key]

	if list.Size() == 0 {
		return nil
	}

	if len(indexes) == 1 {
		list.Remove(indexes[0])
		return nil
	}

	idxes := make(map[int]struct{})
	for _, idx := range indexes {
		if idx < 0 || idx >= list.Size() {
			continue
		}
		idxes[idx] = struct{}{}
	}

	iterator := list.Iterator()
	iterator.Begin()

	items := make([]*Record, list.Size()-len(idxes))
	i := 0
	for iterator.Next() {
		if _, ok := idxes[iterator.Index()]; !ok {
			items[i] = iterator.Value().(*Record)
			i++
		}
	}

	list.Clear()
	for _, item := range items {
		list.Append(item)
	}

	return nil
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

	return true
}

func (l *List) Size(key string) (int, error) {
	if l.IsExpire(key) {
		return 0, ErrListNotFound
	}
	if _, ok := l.Items[key]; !ok {
		return 0, ErrListNotFound
	}

	return l.Items[key].Size(), nil
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
		return 0, 0, errors.New("start or end error")
	}

	return start, end, nil
}
