// Copyright 2022 The nutsdb Author. All rights reserved.
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

import "github.com/nutsdb/nutsdb/internal/data"

type IdxType interface {
	data.BTree | data.Set | SortedSet | data.List
}

type defaultOp[T IdxType] struct {
	idx map[BucketId]*T
}

func (op *defaultOp[T]) computeIfAbsent(id BucketId, f func() *T) *T {
	if i, isExist := op.idx[id]; isExist {
		return i
	}
	i := f()
	op.idx[id] = i
	return i
}

func (op *defaultOp[T]) delete(id BucketId) {
	delete(op.idx, id)
}

func (op *defaultOp[T]) exist(id BucketId) (*T, bool) {
	i, isExist := op.idx[id]
	return i, isExist
}

func (op *defaultOp[T]) getIdxLen() int {
	return len(op.idx)
}

func (op *defaultOp[T]) rangeIdx(f func(elem *T)) {
	for _, t := range op.idx {
		f(t)
	}
}

type ListIdx struct {
	*defaultOp[data.List]
	opts Options
}

func (idx ListIdx) getWithDefault(id BucketId) *data.List {
	return idx.defaultOp.computeIfAbsent(id, func() *data.List {
		return data.NewList(idx.opts.ListImpl.toInternal())
	})
}

type BTreeIdx struct {
	*defaultOp[data.BTree]
}

func (idx BTreeIdx) getWithDefault(id BucketId) *data.BTree {
	return idx.defaultOp.computeIfAbsent(id, func() *data.BTree {
		return data.NewBTree()
	})
}

type SetIdx struct {
	*defaultOp[data.Set]
}

func (idx SetIdx) getWithDefault(id BucketId) *data.Set {
	return idx.defaultOp.computeIfAbsent(id, func() *data.Set {
		return data.NewSet()
	})
}

type SortedSetIdx struct {
	*defaultOp[SortedSet]
}

func (idx SortedSetIdx) getWithDefault(id BucketId, db *DB) *SortedSet {
	return idx.defaultOp.computeIfAbsent(id, func() *SortedSet {
		return NewSortedSet(db)
	})
}

type index struct {
	list      ListIdx
	bTree     BTreeIdx
	set       SetIdx
	sortedSet SortedSetIdx
	opts      Options // Store options for creating new data structures
}

func newIndex() *index {
	return newIndexWithOptions(DefaultOptions)
}

func newIndexWithOptions(opts Options) *index {
	i := new(index)
	i.opts = opts
	i.list = ListIdx{defaultOp: &defaultOp[data.List]{idx: map[BucketId]*data.List{}}, opts: opts}
	i.bTree = BTreeIdx{&defaultOp[data.BTree]{idx: map[BucketId]*data.BTree{}}}
	i.set = SetIdx{&defaultOp[data.Set]{idx: map[BucketId]*data.Set{}}}
	i.sortedSet = SortedSetIdx{&defaultOp[SortedSet]{idx: map[BucketId]*SortedSet{}}}
	return i
}
