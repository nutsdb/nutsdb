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
	"github.com/tidwall/btree"
	"regexp"
)

type Item struct {
	key []byte
	r   *Record
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
		return item.r, ok
	}
	return nil, ok
}

func (bt *BTree) Insert(key []byte, v []byte, h *Hint) bool {
	r := NewRecord().WithValue(v).WithHint(h)
	_, replaced := bt.btree.Set(&Item{key: key, r: r})
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
		records[i] = item.r
	}

	return records
}

func (bt *BTree) Range(start, end []byte) []*Record {
	records := make([]*Record, 0)

	bt.btree.Ascend(&Item{key: start}, func(item *Item) bool {
		if bytes.Compare(item.key, end) > 0 {
			return false
		}
		records = append(records, item.r)
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

		records = append(records, item.r)

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

		records = append(records, item.r)

		limitNum--
		return limitNum != 0
	})

	return records
}

func (bt *BTree) Count() int {
	return bt.btree.Len()
}
