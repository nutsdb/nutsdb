package nutsdb

// BPTreeIdx represents the B+ tree index
type BPTreeIdx map[string]*BPTree

// BTreeIdx represents the B tree index
type BTreeIdx map[string]*BTree

// SetIdx represents the set index
type SetIdx map[string]*Set

// SortedSetIdx represents the sorted set index
type SortedSetIdx map[string]*SortedSet

// ListIdx represents the list index
type ListIdx map[string]*List

type index struct {
	list ListIdx
}

func NewIndex() *index {
	i := new(index)
	i.list = map[string]*List{}
	return i
}

func (i *index) existList(bucket string) bool {
	_, isExist := i.list[bucket]
	return isExist
}

func (i *index) getList(bucket string) *List {
	l, isExist := i.list[bucket]
	if isExist {
		return l
	}
	l = NewList()
	i.list[bucket] = l
	return l
}

func (i *index) deleteList(bucket string) {
	delete(i.list, bucket)
}

func (i *index) addList(bucket string) {
	l := NewList()
	i.list[bucket] = l
}

func (i *index) rangeList(f func(l *List)) {
	for _, l := range i.list {
		f(l)
	}
}

func (i *index) handleListBucket(f func(bucket string) error) error {
	for bucket := range i.list {
		err := f(bucket)
		if err != nil {
			return err
		}
	}
	return nil
}
