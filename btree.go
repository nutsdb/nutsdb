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

type item struct {
	key    []byte
	record *record
}

type bTree struct {
	btree *btree.BTreeG[*item]
}

func newBTree() *bTree {
	return &bTree{
		btree: btree.NewBTreeG[*item](func(a, b *item) bool {
			return bytes.Compare(a.key, b.key) == -1
		}),
	}
}

func (bt *bTree) find(key []byte) (*record, bool) {
	item, ok := bt.btree.Get(&item{key: key})
	if ok {
		return item.record, ok
	}
	return nil, ok
}

func (bt *bTree) insert(record *record) bool {
	_, replaced := bt.btree.Set(&item{key: record.Key, record: record})
	return replaced
}

func (bt *bTree) insertRecord(key []byte, record *record) bool {
	_, replaced := bt.btree.Set(&item{key: key, record: record})
	return replaced
}

func (bt *bTree) delete(key []byte) bool {
	_, deleted := bt.btree.Delete(&item{key: key})
	return deleted
}

func (bt *bTree) all() []*record {
	items := bt.btree.Items()

	records := make([]*record, len(items))
	for i, item := range items {
		records[i] = item.record
	}

	return records
}

func (bt *bTree) allItems() []*item {
	items := bt.btree.Items()
	return items
}

func (bt *bTree) Range(start, end []byte) []*record {
	records := make([]*record, 0)

	bt.btree.Ascend(&item{key: start}, func(item *item) bool {
		if bytes.Compare(item.key, end) > 0 {
			return false
		}
		records = append(records, item.record)
		return true
	})

	return records
}

func (bt *bTree) prefixScan(prefix []byte, offset, limitNum int) []*record {
	records := make([]*record, 0)

	bt.btree.Ascend(&item{key: prefix}, func(item *item) bool {
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

func (bt *bTree) prefixSearchScan(prefix []byte, reg string, offset, limitNum int) []*record {
	records := make([]*record, 0)

	rgx := regexp.MustCompile(reg)

	bt.btree.Ascend(&item{key: prefix}, func(item *item) bool {
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

func (bt *bTree) count() int {
	return bt.btree.Len()
}

func (bt *bTree) popMin() (*item, bool) {
	return bt.btree.PopMin()
}

func (bt *bTree) popMax() (*item, bool) {
	return bt.btree.PopMax()
}

func (bt *bTree) min() (*item, bool) {
	return bt.btree.Min()
}

func (bt *bTree) max() (*item, bool) {
	return bt.btree.Max()
}
