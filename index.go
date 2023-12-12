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

type idxType interface {
	bTree | set | sortedSet | list
}

type defaultOp[T idxType] struct {
	idx map[bucketId]*T
}

func (op *defaultOp[T]) computeIfAbsent(id bucketId, f func() *T) *T {
	if i, isExist := op.idx[id]; isExist {
		return i
	}
	i := f()
	op.idx[id] = i
	return i
}

func (op *defaultOp[T]) delete(id bucketId) {
	delete(op.idx, id)
}

func (op *defaultOp[T]) exist(id bucketId) (*T, bool) {
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

type listIdx struct {
	*defaultOp[list]
}

func (idx listIdx) getWithDefault(id bucketId) *list {
	return idx.defaultOp.computeIfAbsent(id, func() *list {
		return newList()
	})
}

type bTreeIdx struct {
	*defaultOp[bTree]
}

func (idx bTreeIdx) getWithDefault(id bucketId) *bTree {
	return idx.defaultOp.computeIfAbsent(id, func() *bTree {
		return newBTree()
	})
}

type setIdx struct {
	*defaultOp[set]
}

func (idx setIdx) getWithDefault(id bucketId) *set {
	return idx.defaultOp.computeIfAbsent(id, func() *set {
		return newSet()
	})
}

type sortedSetIdx struct {
	*defaultOp[sortedSet]
}

func (idx sortedSetIdx) getWithDefault(id bucketId, db *DB) *sortedSet {
	return idx.defaultOp.computeIfAbsent(id, func() *sortedSet {
		return newSortedSet(db)
	})
}

type index struct {
	list      listIdx
	bTree     bTreeIdx
	set       setIdx
	sortedSet sortedSetIdx
}

func newIndex() *index {
	i := new(index)
	i.list = listIdx{&defaultOp[list]{idx: map[bucketId]*list{}}}
	i.bTree = bTreeIdx{&defaultOp[bTree]{idx: map[bucketId]*bTree{}}}
	i.set = setIdx{&defaultOp[set]{idx: map[bucketId]*set{}}}
	i.sortedSet = sortedSetIdx{&defaultOp[sortedSet]{idx: map[bucketId]*sortedSet{}}}
	return i
}
