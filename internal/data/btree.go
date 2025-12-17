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

package data

import (
	"bytes"
	"regexp"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/tidwall/btree"
)

type BTree struct {
	btree *btree.BTreeG[*Item[core.Record]]
}

func NewBTree() *BTree {
	return &BTree{
		btree: btree.NewBTreeG(func(a, b *Item[core.Record]) bool {
			return bytes.Compare(a.Key, b.Key) == -1
		}),
	}
}

func (bt *BTree) Find(key []byte) (*core.Record, bool) {
	item, ok := bt.btree.Get(NewItem[core.Record](key, nil))
	if ok {
		return item.Record, ok
	}
	return nil, ok
}

func (bt *BTree) InsertRecord(key []byte, record *core.Record) bool {
	_, replaced := bt.btree.Set(NewItem(key, record))
	return replaced
}

func (bt *BTree) Delete(key []byte) bool {
	_, deleted := bt.btree.Delete(NewItem[core.Record](key, nil))
	return deleted
}

func (bt *BTree) All() []*core.Record {
	items := bt.btree.Items()

	records := make([]*core.Record, len(items))
	for i, item := range items {
		records[i] = item.Record
	}

	return records
}

func (bt *BTree) AllItems() []*Item[core.Record] {
	items := bt.btree.Items()
	return items
}

func (bt *BTree) Range(start, end []byte) []*core.Record {
	records := make([]*core.Record, 0)

	bt.btree.Ascend(&Item[core.Record]{Key: start}, func(item *Item[core.Record]) bool {
		if bytes.Compare(item.Key, end) > 0 {
			return false
		}
		records = append(records, item.Record)
		return true
	})

	return records
}

func (bt *BTree) PrefixScan(prefix []byte, offset, limitNum int) []*core.Record {
	records := make([]*core.Record, 0)

	bt.btree.Ascend(&Item[core.Record]{Key: prefix}, func(item *Item[core.Record]) bool {
		if !bytes.HasPrefix(item.Key, prefix) {
			return false
		}

		if offset > 0 {
			offset--
			return true
		}

		records = append(records, item.Record)

		limitNum--
		return limitNum != 0
	})

	return records
}

func (bt *BTree) PrefixSearchScan(prefix []byte, reg string, offset, limitNum int) []*core.Record {
	records := make([]*core.Record, 0)

	rgx := regexp.MustCompile(reg)

	bt.btree.Ascend(&Item[core.Record]{Key: prefix}, func(item *Item[core.Record]) bool {
		if !bytes.HasPrefix(item.Key, prefix) {
			return false
		}

		if offset > 0 {
			offset--
			return true
		}

		if !rgx.Match(bytes.TrimPrefix(item.Key, prefix)) {
			return true
		}

		records = append(records, item.Record)

		limitNum--
		return limitNum != 0
	})

	return records
}

func (bt *BTree) Count() int {
	return bt.btree.Len()
}

func (bt *BTree) PopMin() (*Item[core.Record], bool) {
	return bt.btree.PopMin()
}

func (bt *BTree) PopMax() (*Item[core.Record], bool) {
	return bt.btree.PopMax()
}

func (bt *BTree) Min() (*Item[core.Record], bool) {
	return bt.btree.Min()
}

func (bt *BTree) Max() (*Item[core.Record], bool) {
	return bt.btree.Max()
}

func (bt *BTree) Iter() btree.IterG[*Item[core.Record]] {
	return bt.btree.Iter()
}
