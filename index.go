package nutsdb

type IdxType interface {
	BPTree | BTree | Set | SortedSet | List
}

type op[T IdxType] interface {
	exist(bucket string) (*T, bool)

	getIdx() map[string]*T

	computeIfPresent(bucket string, f func() *T) *T

	delete(bucket string)

	rangeIdx(f func(l *T))

	handleIdxBucket(f func(bucket string) error) error
}

type defaultOp[T IdxType] struct {
	idx map[string]*T
}

func (op *defaultOp[T]) exist(bucket string) (*T, bool) {
	i, isExist := op.idx[bucket]
	return i, isExist
}

func (op *defaultOp[T]) getIdx() map[string]*T {
	return op.idx
}

func (op *defaultOp[T]) computeIfPresent(bucket string, f func() *T) *T {
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

func (op *defaultOp[T]) add(bucket string, f func() *T) {
	op.idx[bucket] = f()
}

func (op *defaultOp[T]) rangeIdx(f func(l *T)) {
	for _, l := range op.idx {
		f(l)
	}
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

type ListIdx struct {
	*defaultOp[List]
}

func (idx ListIdx) get(bucket string) *List {
	return idx.defaultOp.computeIfPresent(bucket, func() *List {
		return NewList()
	})
}

type BTreeIdx struct {
	*defaultOp[BTree]
}

func (idx BTreeIdx) get(bucket string) *BTree {
	return idx.defaultOp.computeIfPresent(bucket, func() *BTree {
		return NewBTree()
	})
}

type SetIdx struct {
	*defaultOp[Set]
}

func (idx SetIdx) get(bucket string) *Set {
	return idx.defaultOp.computeIfPresent(bucket, func() *Set {
		return NewSet()
	})
}

type SortedSetIdx struct {
	*defaultOp[SortedSet]
}

func (idx SortedSetIdx) get(bucket string, db *DB) *SortedSet {
	return idx.defaultOp.computeIfPresent(bucket, func() *SortedSet {
		return NewSortedSet(db)
	})
}

type index struct {
	list      ListIdx
	bTree     BTreeIdx
	set       SetIdx
	sortedSet SortedSetIdx
}

func NewIndex() *index {
	i := new(index)
	i.list = ListIdx{&defaultOp[List]{idx: map[string]*List{}}}
	i.bTree = BTreeIdx{&defaultOp[BTree]{idx: map[string]*BTree{}}}
	i.set = SetIdx{&defaultOp[Set]{idx: map[string]*Set{}}}
	i.sortedSet = SortedSetIdx{&defaultOp[SortedSet]{idx: map[string]*SortedSet{}}}
	return i
}
