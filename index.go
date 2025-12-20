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

import (
	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/data"
)

type IndexType interface {
	data.BTree | data.Set | SortedSet | data.List
}

type defaultOp[T IndexType] struct {
	Idx map[core.BucketId]*T
}

func (op *defaultOp[T]) computeIfAbsent(id core.BucketId, f func() *T) *T {
	if i, isExist := op.Idx[id]; isExist {
		return i
	}
	i := f()
	op.Idx[id] = i
	return i
}

func (op *defaultOp[T]) delete(id core.BucketId) {
	delete(op.Idx, id)
}

func (op *defaultOp[T]) exist(id core.BucketId) (*T, bool) {
	i, isExist := op.Idx[id]
	return i, isExist
}

func (op *defaultOp[T]) getIdxLen() int {
	return len(op.Idx)
}

func (op *defaultOp[T]) rangeIdx(f func(elem *T)) {
	for _, t := range op.Idx {
		f(t)
	}
}

type Index struct {
	List      *ListIndex
	BTree     *BTreeIndex
	Set       *SetIndex
	SortedSet *SortedSetIndex
	db        *DB
}

func (db *DB) newIndex() *Index {
	i := &Index{db: db}
	i.List = &ListIndex{defaultOp: &defaultOp[data.List]{Idx: map[core.BucketId]*data.List{}}, index: i}
	i.BTree = &BTreeIndex{defaultOp: &defaultOp[data.BTree]{Idx: map[core.BucketId]*data.BTree{}}, index: i}
	i.Set = &SetIndex{defaultOp: &defaultOp[data.Set]{Idx: map[core.BucketId]*data.Set{}}, index: i}
	i.SortedSet = &SortedSetIndex{defaultOp: &defaultOp[SortedSet]{Idx: map[core.BucketId]*SortedSet{}}, index: i}
	return i
}

type ListIndex struct {
	*defaultOp[data.List]
	index *Index
}

func (idx *ListIndex) GetWithDefault(id core.BucketId) *data.List {
	return idx.computeIfAbsent(id, func() *data.List {
		return data.NewList(idx.index.db.opt.ListImpl.toInternal())
	})
}

type BTreeIndex struct {
	*defaultOp[data.BTree]
	index *Index
}

func (idx *BTreeIndex) GetWithDefault(id core.BucketId) *data.BTree {
	return idx.computeIfAbsent(id, func() *data.BTree {
		return data.NewBTree(id, idx.index.db.ttlService.GetChecker())
	})
}

type SetIndex struct {
	*defaultOp[data.Set]
	index *Index
}

func (idx *SetIndex) GetWithDefault(id core.BucketId) *data.Set {
	return idx.computeIfAbsent(id, func() *data.Set {
		return data.NewSet()
	})
}

type SortedSetIndex struct {
	*defaultOp[SortedSet]
	index *Index
}

func (idx *SortedSetIndex) GetWithDefault(id core.BucketId) *SortedSet {
	return idx.computeIfAbsent(id, func() *SortedSet {
		return NewSortedSet(idx.index.db)
	})
}
