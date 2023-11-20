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

type IdxType interface {
	BTree | Set | SortedSet | List
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
	*defaultOp[List]
}

func (idx ListIdx) getWithDefault(id BucketId) *List {
	return idx.defaultOp.computeIfAbsent(id, func() *List {
		return NewList()
	})
}

type BTreeIdx struct {
	*defaultOp[BTree]
}

func (idx BTreeIdx) getWithDefault(id BucketId) *BTree {
	return idx.defaultOp.computeIfAbsent(id, func() *BTree {
		return NewBTree()
	})
}

type SetIdx struct {
	*defaultOp[Set]
}

func (idx SetIdx) getWithDefault(id BucketId) *Set {
	return idx.defaultOp.computeIfAbsent(id, func() *Set {
		return NewSet()
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
}

func newIndex() *index {
	i := new(index)
	i.list = ListIdx{&defaultOp[List]{idx: map[BucketId]*List{}}}
	i.bTree = BTreeIdx{&defaultOp[BTree]{idx: map[BucketId]*BTree{}}}
	i.set = SetIdx{&defaultOp[Set]{idx: map[BucketId]*Set{}}}
	i.sortedSet = SortedSetIdx{&defaultOp[SortedSet]{idx: map[BucketId]*SortedSet{}}}
	return i
}
