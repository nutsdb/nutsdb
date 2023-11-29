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
	"errors"
	"regexp"

	"github.com/tidwall/btree"
)

// ErrKeyNotFound is returned when the key is not in the b tree.
var ErrKeyNotFound = errors.New("key not found")

type Item struct {
	key    []byte
	record *Record
}

type BTree struct {
	btree *btree.BTreeG[*Item]
}

func NewBTree() *BTree {
	return &BTree{
		btree: btree.NewBTreeG[*Item](func(a, b *Item) bool {
			return bytes.Compare(a.key, b.key) == -1
		}),
	}
}

func (bt *BTree) Find(key []byte) (*Record, bool) {
	item, ok := bt.btree.Get(&Item{key: key})
	if ok {
		return item.record, ok
	}
	return nil, ok
}

func (bt *BTree) Insert(record *Record) bool {
	_, replaced := bt.btree.Set(&Item{key: record.Key, record: record})
	return replaced
}

func (bt *BTree) InsertRecord(key []byte, record *Record) bool {
	_, replaced := bt.btree.Set(&Item{key: key, record: record})
	return replaced
}

func (bt *BTree) Delete(key []byte) bool {
	_, deleted := bt.btree.Delete(&Item{key: key})
	return deleted
}

func (bt *BTree) All() []*Record {
	items := bt.btree.Items()

	records := make([]*Record, len(items))
	for i, item := range items {
		records[i] = item.record
	}

	return records
}

func (bt *BTree) AllItems() []*Item {
	items := bt.btree.Items()
	return items
}

func (bt *BTree) Range(start, end []byte) []*Record {
	records := make([]*Record, 0)

	bt.btree.Ascend(&Item{key: start}, func(item *Item) bool {
		if bytes.Compare(item.key, end) > 0 {
			return false
		}
		records = append(records, item.record)
		return true
	})

	return records
}

func (bt *BTree) PrefixScan(prefix []byte, offset, limitNum int) []*Record {
	records := make([]*Record, 0)

	bt.btree.Ascend(&Item{key: prefix}, func(item *Item) bool {
		if !bytes.HasPrefix(item.key, prefix) {
			return false
		}

		if offset > 0 {
			offset--
			return true
		}

		records = append(records, item.record)

		limitNum--
		return limitNum != 0
	})

	return records
}

func (bt *BTree) PrefixSearchScan(prefix []byte, reg string, offset, limitNum int) []*Record {
	records := make([]*Record, 0)

	rgx := regexp.MustCompile(reg)

	bt.btree.Ascend(&Item{key: prefix}, func(item *Item) bool {
		if !bytes.HasPrefix(item.key, prefix) {
			return false
		}

		if offset > 0 {
			offset--
			return true
		}

		if !rgx.Match(bytes.TrimPrefix(item.key, prefix)) {
			return true
		}

		records = append(records, item.record)

		limitNum--
		return limitNum != 0
	})

	return records
}

func (bt *BTree) Count() int {
	return bt.btree.Len()
}

func (bt *BTree) PopMin() (*Item, bool) {
	return bt.btree.PopMin()
}

func (bt *BTree) PopMax() (*Item, bool) {
	return bt.btree.PopMax()
}

func (bt *BTree) Min() (*Item, bool) {
	return bt.btree.Min()
}

func (bt *BTree) Max() (*Item, bool) {
	return bt.btree.Max()
}
