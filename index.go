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
	idx map[string]*T
}

func (op *defaultOp[T]) computeIfAbsent(bucket string, f func() *T) *T {
	if i, isExist := op.idx[bucket]; isExist {
		return i
	}
	i := f()
	op.idx[bucket] = i
	return i
}

func (op *defaultOp[T]) delete(bucket string) {
	delete(op.idx, bucket)
}

func (op *defaultOp[T]) exist(bucket string) (*T, bool) {
	i, isExist := op.idx[bucket]
	return i, isExist
}

func (op *defaultOp[T]) getIdxLen() int {
	return len(op.idx)
}

func (op *defaultOp[T]) handleIdxBucket(f func(bucket string) error) error {
	for bucket := range op.idx {
		err := f(bucket)
		if err != nil {
			return err
		}
	}
	return nil
}

func (op *defaultOp[T]) rangeIdx(f func(elem *T)) {
	for _, t := range op.idx {
		f(t)
	}
}

type ListIdx struct {
	*defaultOp[List]
}

func (idx ListIdx) getWithDefault(bucket string) *List {
	return idx.defaultOp.computeIfAbsent(bucket, func() *List {
		return NewList()
	})
}

type BTreeIdx struct {
	*defaultOp[BTree]
}

func (idx BTreeIdx) getWithDefault(bucket string) *BTree {
	return idx.defaultOp.computeIfAbsent(bucket, func() *BTree {
		return NewBTree()
	})
}

type SetIdx struct {
	*defaultOp[Set]
}

func (idx SetIdx) getWithDefault(bucket string) *Set {
	return idx.defaultOp.computeIfAbsent(bucket, func() *Set {
		return NewSet()
	})
}

type SortedSetIdx struct {
	*defaultOp[SortedSet]
}

func (idx SortedSetIdx) getWithDefault(bucket string, db *DB) *SortedSet {
	return idx.defaultOp.computeIfAbsent(bucket, func() *SortedSet {
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
	i.list = ListIdx{&defaultOp[List]{idx: map[string]*List{}}}
	i.bTree = BTreeIdx{&defaultOp[BTree]{idx: map[string]*BTree{}}}
	i.set = SetIdx{&defaultOp[Set]{idx: map[string]*Set{}}}
	i.sortedSet = SortedSetIdx{&defaultOp[SortedSet]{idx: map[string]*SortedSet{}}}
	return i
}
