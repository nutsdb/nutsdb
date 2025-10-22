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
	"github.com/nutsdb/nutsdb/internal/data"
	"github.com/tidwall/btree"
)

type Iterator struct {
	tx      *Tx
	options IteratorOptions
	iter    btree.IterG[*data.Item[data.Record]]
	// Cached current item to avoid repeated iter.Item() calls
	currentItem *data.Item[data.Record]
	// Track validity state to avoid unnecessary checks
	valid bool
}

type IteratorOptions struct {
	Reverse bool
}

// Returns a new iterator.
// The Release method must be called when finished with the iterator.
func NewIterator(tx *Tx, bucket string, options IteratorOptions) *Iterator {
	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return nil
	}
	iterator := &Iterator{
		tx:      tx,
		options: options,
		iter:    tx.db.Index.bTree.getWithDefault(b.Id).Iter(),
	}

	// Initialize position and cache the first item
	if options.Reverse {
		iterator.valid = iterator.iter.Last()
	} else {
		iterator.valid = iterator.iter.First()
	}

	if iterator.valid {
		iterator.currentItem = iterator.iter.Item()
	}

	return iterator
}

func (it *Iterator) Rewind() bool {
	if it.options.Reverse {
		it.valid = it.iter.Last()
	} else {
		it.valid = it.iter.First()
	}

	if it.valid {
		it.currentItem = it.iter.Item()
	} else {
		it.currentItem = nil
	}

	return it.valid
}

func (it *Iterator) Seek(key []byte) bool {
	it.valid = it.iter.Seek(&data.Item[data.Record]{Key: key})

	if it.valid {
		it.currentItem = it.iter.Item()
	} else {
		it.currentItem = nil
	}

	return it.valid
}

func (it *Iterator) Next() bool {
	if !it.valid {
		return false
	}

	if it.options.Reverse {
		it.valid = it.iter.Prev()
	} else {
		it.valid = it.iter.Next()
	}

	if it.valid {
		it.currentItem = it.iter.Item()
	} else {
		it.currentItem = nil
	}

	return it.valid
}

func (it *Iterator) Valid() bool {
	return it.valid
}

func (it *Iterator) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.currentItem.Key
}

func (it *Iterator) Value() ([]byte, error) {
	if !it.valid {
		return nil, ErrKeyNotFound
	}
	return it.tx.db.getValueByRecord(it.currentItem.Record)
}

// Item returns the current item (key + record) if valid
// This is useful for advanced use cases that need direct access to the record
func (it *Iterator) Item() *data.Item[data.Record] {
	if !it.valid {
		return nil
	}
	return it.currentItem
}

func (it *Iterator) Release() {
	it.iter.Release()
	it.currentItem = nil
	it.valid = false
}
