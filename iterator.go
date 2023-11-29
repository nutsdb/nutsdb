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
	"github.com/tidwall/btree"
)

type Iterator struct {
	tx      *Tx
	options IteratorOptions
	iter    btree.IterG[*Item]
}

type IteratorOptions struct {
	Reverse bool
}

func NewIterator(tx *Tx, bucket string, options IteratorOptions) *Iterator {
	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return nil
	}
	iterator := &Iterator{
		tx:      tx,
		options: options,
		iter:    tx.db.Index.bTree.getWithDefault(b.Id).btree.Iter(),
	}

	if options.Reverse {
		iterator.iter.Last()
	} else {
		iterator.iter.First()
	}

	return iterator
}

func (it *Iterator) Rewind() bool {
	if it.options.Reverse {
		return it.iter.Last()
	} else {
		return it.iter.First()
	}
}

func (it *Iterator) Seek(key []byte) bool {
	return it.iter.Seek(&Item{key: key})
}

func (it *Iterator) Next() bool {
	if it.options.Reverse {
		return it.iter.Prev()
	} else {
		return it.iter.Next()
	}
}

func (it *Iterator) Valid() bool {
	return it.iter.Item() != nil
}

func (it *Iterator) Key() []byte {
	return it.iter.Item().key
}

func (it *Iterator) Value() ([]byte, error) {
	return it.tx.db.getValueByRecord(it.iter.Item().record)
}
