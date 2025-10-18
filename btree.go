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
	"regexp"

	"github.com/nutsdb/nutsdb/internal/data"
	"github.com/tidwall/btree"
)

// ErrKeyNotFound is returned when the key is not in the b tree.
var ErrKeyNotFound = ErrNotFoundKey

type BTree struct {
	btree *btree.BTreeG[*data.Item[data.Record]]
}

func NewBTree() *BTree {
	return &BTree{
		btree: btree.NewBTreeG(func(a, b *data.Item[data.Record]) bool {
			return bytes.Compare(a.Key, b.Key) == -1
		}),
	}
}

func (bt *BTree) Find(key []byte) (*data.Record, bool) {
	item, ok := bt.btree.Get(&data.Item[data.Record]{Key: key})
	if ok {
		return item.Record, ok
	}
	return nil, ok
}

func (bt *BTree) Insert(record *data.Record) bool {
	_, replaced := bt.btree.Set(&data.Item[data.Record]{Key: record.GetKey(), Record: record})
	return replaced
}

func (bt *BTree) InsertRecord(key []byte, record *data.Record) bool {
	_, replaced := bt.btree.Set(&data.Item[data.Record]{Key: key, Record: record})
	return replaced
}

func (bt *BTree) Delete(key []byte) bool {
	_, deleted := bt.btree.Delete(&data.Item[data.Record]{Key: key})
	return deleted
}

func (bt *BTree) All() []*data.Record {
	items := bt.btree.Items()

	records := make([]*data.Record, len(items))
	for i, item := range items {
		records[i] = item.Record
	}

	return records
}

func (bt *BTree) AllItems() []*data.Item[data.Record] {
	items := bt.btree.Items()
	return items
}

func (bt *BTree) Range(start, end []byte) []*data.Record {
	records := make([]*data.Record, 0)

	bt.btree.Ascend(&data.Item[data.Record]{Key: start}, func(item *data.Item[data.Record]) bool {
		if bytes.Compare(item.Key, end) > 0 {
			return false
		}
		records = append(records, item.Record)
		return true
	})

	return records
}

func (bt *BTree) PrefixScan(prefix []byte, offset, limitNum int) []*data.Record {
	records := make([]*data.Record, 0)

	bt.btree.Ascend(&data.Item[data.Record]{Key: prefix}, func(item *data.Item[data.Record]) bool {
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

func (bt *BTree) PrefixSearchScan(prefix []byte, reg string, offset, limitNum int) []*data.Record {
	records := make([]*data.Record, 0)

	rgx := regexp.MustCompile(reg)

	bt.btree.Ascend(&data.Item[data.Record]{Key: prefix}, func(item *data.Item[data.Record]) bool {
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

func (bt *BTree) PopMin() (*data.Item[data.Record], bool) {
	return bt.btree.PopMin()
}

func (bt *BTree) PopMax() (*data.Item[data.Record], bool) {
	return bt.btree.PopMax()
}

func (bt *BTree) Min() (*data.Item[data.Record], bool) {
	return bt.btree.Min()
}

func (bt *BTree) Max() (*data.Item[data.Record], bool) {
	return bt.btree.Max()
}
