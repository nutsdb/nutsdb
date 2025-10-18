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
	"bytes"
	"container/list"
	"regexp"
)

// ListElement wraps the data stored in the list
type ListElement struct {
	key    []byte
	record *Record
}

// DoublyLinkedList represents a doubly linked list optimized for head/tail operations.
// It uses Go's standard container/list without additional index structures.
// Best suited for workloads dominated by LPush/RPush/LPop/RPop operations.
// Note: Find and Delete operations require O(n) traversal.
type DoublyLinkedList struct {
	list *list.List // standard library doubly linked list
}

// NewDoublyLinkedList creates a new doubly linked list
func NewDoublyLinkedList() *DoublyLinkedList {
	return &DoublyLinkedList{
		list: list.New(),
	}
}

// InsertRecord inserts a record with the given key in sorted order by key (sequence number).
// Optimized for head/tail insertions (LPush/RPush pattern).
// Warning: Middle insertions require O(n) traversal. Check for duplicates requires O(n) scan.
func (dll *DoublyLinkedList) InsertRecord(key []byte, record *Record) bool {
	newElem := &ListElement{
		key:    key,
		record: record,
	}

	// Empty list - just insert
	if dll.list.Len() == 0 {
		dll.list.PushBack(newElem)
		return false
	}

	// Check if inserting at head or tail (common case for List operations)
	front := dll.list.Front().Value.(*ListElement)
	back := dll.list.Back().Value.(*ListElement)

	// Check for duplicate at head
	if bytes.Equal(key, front.key) {
		front.record = record
		return true
	}

	// Insert at head if key is smaller than front
	if bytes.Compare(key, front.key) < 0 {
		dll.list.PushFront(newElem)
		return false
	}

	// Check for duplicate at tail
	if bytes.Equal(key, back.key) {
		back.record = record
		return true
	}

	// Insert at tail if key is larger than back
	if bytes.Compare(key, back.key) > 0 {
		dll.list.PushBack(newElem)
		return false
	}

	// Middle insertion: find the correct position (O(n) operation)
	// This should be rare in typical List usage patterns
	for e := dll.list.Front(); e != nil; e = e.Next() {
		elem := e.Value.(*ListElement)
		cmp := bytes.Compare(key, elem.key)

		if cmp == 0 {
			// Update existing element
			elem.record = record
			return true
		}

		if cmp < 0 {
			// Insert before current element
			dll.list.InsertBefore(newElem, e)
			return false
		}
	}

	// Fallback (shouldn't reach here given the checks above)
	dll.list.PushBack(newElem)
	return false
}

// Delete removes a node with the given key.
// Warning: This is an O(n) operation since we don't maintain an index.
// For List use cases, prefer PopMin/PopMax for head/tail deletions.
func (dll *DoublyLinkedList) Delete(key []byte) bool {
	for e := dll.list.Front(); e != nil; e = e.Next() {
		elem := e.Value.(*ListElement)
		if bytes.Equal(elem.key, key) {
			dll.list.Remove(e)
			return true
		}
	}
	return false
}

// Find returns the record with the given key.
// Warning: This is an O(n) operation since we don't maintain an index.
func (dll *DoublyLinkedList) Find(key []byte) (*Record, bool) {
	for e := dll.list.Front(); e != nil; e = e.Next() {
		elem := e.Value.(*ListElement)
		if bytes.Equal(elem.key, key) {
			return elem.record, true
		}
	}
	return nil, false
}

// Min returns the first element (smallest key)
func (dll *DoublyLinkedList) Min() (*Item, bool) {
	front := dll.list.Front()
	if front == nil {
		return nil, false
	}
	elem := front.Value.(*ListElement)
	return &Item{
		key:    elem.key,
		record: elem.record,
	}, true
}

// Max returns the last element (largest key)
func (dll *DoublyLinkedList) Max() (*Item, bool) {
	back := dll.list.Back()
	if back == nil {
		return nil, false
	}
	elem := back.Value.(*ListElement)
	return &Item{
		key:    elem.key,
		record: elem.record,
	}, true
}

// PopMin removes and returns the first element
func (dll *DoublyLinkedList) PopMin() (*Item, bool) {
	front := dll.list.Front()
	if front == nil {
		return nil, false
	}

	elem := front.Value.(*ListElement)
	dll.list.Remove(front)

	// Construct result - keep fields in same order as PopMax for consistency
	return &Item{
		key:    elem.key,
		record: elem.record,
	}, true
}

// PopMax removes and returns the last element
func (dll *DoublyLinkedList) PopMax() (*Item, bool) {
	back := dll.list.Back()
	if back == nil {
		return nil, false
	}

	elem := back.Value.(*ListElement)
	dll.list.Remove(back)

	return &Item{
		key:    elem.key,
		record: elem.record,
	}, true
}

// All returns all records in order
func (dll *DoublyLinkedList) All() []*Record {
	records := make([]*Record, 0, dll.list.Len())
	for e := dll.list.Front(); e != nil; e = e.Next() {
		elem := e.Value.(*ListElement)
		records = append(records, elem.record)
	}
	return records
}

// AllItems returns all items in order
func (dll *DoublyLinkedList) AllItems() []*Item {
	items := make([]*Item, 0, dll.list.Len())
	for e := dll.list.Front(); e != nil; e = e.Next() {
		elem := e.Value.(*ListElement)
		items = append(items, &Item{
			key:    elem.key,
			record: elem.record,
		})
	}
	return items
}

// Count returns the number of elements
func (dll *DoublyLinkedList) Count() int {
	return dll.list.Len()
}

// Range returns records within the given key range [start, end]
func (dll *DoublyLinkedList) Range(start, end []byte) []*Record {
	records := make([]*Record, 0)

	for e := dll.list.Front(); e != nil; e = e.Next() {
		elem := e.Value.(*ListElement)
		if bytes.Compare(elem.key, start) >= 0 && bytes.Compare(elem.key, end) <= 0 {
			records = append(records, elem.record)
		}
		if bytes.Compare(elem.key, end) > 0 {
			break
		}
	}

	return records
}

// PrefixScan scans records with the given prefix
func (dll *DoublyLinkedList) PrefixScan(prefix []byte, offset, limitNum int) []*Record {
	records := make([]*Record, 0)

	for e := dll.list.Front(); e != nil; e = e.Next() {
		elem := e.Value.(*ListElement)
		if bytes.HasPrefix(elem.key, prefix) {
			if offset > 0 {
				offset--
			} else {
				records = append(records, elem.record)
				limitNum--
				if limitNum == 0 {
					break
				}
			}
		}
	}

	return records
}

// PrefixSearchScan scans records with the given prefix and regex pattern
func (dll *DoublyLinkedList) PrefixSearchScan(prefix []byte, reg string, offset, limitNum int) []*Record {
	records := make([]*Record, 0)
	rgx, err := regexp.Compile(reg)
	if err != nil {
		return records
	}

	for e := dll.list.Front(); e != nil; e = e.Next() {
		elem := e.Value.(*ListElement)
		if !bytes.HasPrefix(elem.key, prefix) {
			continue
		}

		if offset > 0 {
			offset--
			continue
		}

		if !rgx.Match(bytes.TrimPrefix(elem.key, prefix)) {
			continue
		}

		records = append(records, elem.record)
		limitNum--
		if limitNum == 0 {
			break
		}
	}

	return records
}

// Insert is an alias for InsertRecord (for compatibility with BTree interface)
func (dll *DoublyLinkedList) Insert(record *Record) bool {
	return dll.InsertRecord(record.Key, record)
}
