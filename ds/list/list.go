// Copyright 2019 The nutsdb Authors. All rights reserved.
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

package list

import (
	"errors"
)

var (
	// ErrListNotFound is returned when the list not found.
	ErrListNotFound = errors.New("the list not found")

	// ErrIndexOutOfRange is returned when use LSet function set index out of range.
	ErrIndexOutOfRange = errors.New("index out of range")

	//ErrCount is returned when count is error.
	ErrCount = errors.New("err count")
)

// List represents the list.
type List struct {
	Items map[string][][]byte
}

// New returns returns a newly initialized List Object that implements the List.
func New() *List {
	return &List{
		Items: make(map[string][][]byte),
	}
}

// RPop removes and returns the last element of the list stored at key.
func (l *List) RPop(key string) (item []byte, err error) {
	var size int
	item, size, err = l.RPeek(key)
	if err != nil {
		return
	}

	l.Items[key] = l.Items[key][0 : size-1]

	return
}

// RPeek returns the last element of the list stored at key.
func (l *List) RPeek(key string) (item []byte, size int, err error) {
	if _, ok := l.Items[key]; !ok {
		return nil, 0, ErrListNotFound
	}

	size, _ = l.Size(key)
	if size > 0 {
		item = l.Items[key][size-1:][0]
		return
	}

	return nil, size, ErrListNotFound
}

// RPush inserts all the specified values at the tail of the list stored at key.
func (l *List) RPush(key string, values ...[]byte) (size int, err error) {
	for _, value := range values {
		l.Items[key] = append(l.Items[key], value)
	}

	return l.Size(key)
}

// LPush inserts all the specified values at the head of the list stored at key.
func (l *List) LPush(key string, values ...[]byte) (size int, err error) {
	size, _ = l.Size(key)

	valueLen := len(values)

	newSize := size + valueLen
	newList := make([][]byte, newSize)

	var i, j int

	j = valueLen
	for i = 1; i <= size; i++ {
		if i-1 < size {
			newList[j] = l.Items[key][i-1]
			j++
		}
	}

	j = 0
	for i = valueLen - 1; i >= 0; i-- {
		newList[i] = values[j]
		j++
	}

	l.Items[key] = newList

	return newSize, nil
}

// LPop removes and returns the first element of the list stored at key.
func (l *List) LPop(key string) (item []byte, err error) {
	item, err = l.LPeek(key)
	if err != nil {
		return
	}

	if l.Items[key] != nil {
		l.Items[key] = l.Items[key][1:]
		return
	}

	return nil, errors.New("list is empty")
}

// LPeek returns the first element of the list stored at key.
func (l *List) LPeek(key string) (item []byte, err error) {
	if _, ok := l.Items[key]; !ok {
		return nil, ErrListNotFound
	}

	if size, _ := l.Size(key); size > 0 {
		item = l.Items[key][0]
		return
	}

	return nil, ErrListNotFound
}

// Size returns the size of the list at given key.
func (l *List) Size(key string) (int, error) {
	if _, ok := l.Items[key]; !ok {
		return 0, ErrListNotFound
	}

	return len(l.Items[key]), nil
}

// LRange returns the specified elements of the list stored at key
// [start,end]
func (l *List) LRange(key string, start, end int) (list [][]byte, err error) {
	size, err := l.Size(key)
	if err != nil {
		return
	}

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
		return nil, errors.New("start or end error")
	}

	list = l.Items[key][start : end+1]

	return
}

// LRem removes the first count occurrences of elements equal to value from the list stored at key.
// The count argument influences the operation in the following ways:
// count > 0: Remove elements equal to value moving from head to tail.
// count < 0: Remove elements equal to value moving from tail to head.
// count = 0: Remove all elements equal to value.
func (l *List) LRem(key string, count int) (int, error) {
	if _, ok := l.Items[key]; !ok {
		return 0, ErrListNotFound
	}

	size, _ := l.Size(key)
	if count >= size {
		return 0, ErrCount
	}

	if count == 0 {
		delete(l.Items, key)
		return size, nil
	}

	if count > 0 {
		l.Items[key] = l.Items[key][count:]
	}

	if count < 0 {
		index := size + count
		if 0 < index && index <= size {
			l.Items[key] = l.Items[key][0:index]
		} else {
			return 0, ErrCount
		}
	}

	newSize, _ := l.Size(key)

	return size - newSize, nil
}

// LSet sets the list element at index to value.
func (l *List) LSet(key string, index int, value []byte) error {
	if _, ok := l.Items[key]; !ok {
		return ErrListNotFound
	}

	size, _ := l.Size(key)
	if index >= size || index < 0 {
		return ErrIndexOutOfRange
	}

	l.Items[key][index] = value

	return nil
}

// Ltrim trim an existing list so that it will contain only the specified range of elements specified.
func (l *List) Ltrim(key string, start, end int) error {
	var err error

	if _, ok := l.Items[key]; !ok {
		return ErrListNotFound
	}

	l.Items[key], err = l.LRange(key, start, end)
	if err != nil {
		return err
	}

	return nil
}
