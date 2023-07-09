package nutsdb

import (
	"github.com/nutsdb/nutsdb/ds/list"
	"github.com/nutsdb/nutsdb/ds/set"
	"github.com/nutsdb/nutsdb/ds/zset"
)

// BPTreeIdx represents the B+ tree index
type BPTreeIdx map[string]*BPTree

// SetIdx represents the set index
type SetIdx map[string]*set.Set

// SortedSetIdx represents the sorted set index
type SortedSetIdx map[string]*zset.SortedSet

// ListIdx represents the list index
type ListIdx map[string]*list.List

type index struct {
	list ListIdx
}

func NewIndex() *index {
	i := new(index)
	i.list = map[string]*list.List{}
	return i
}

func (i *index) getList(bucket string) *list.List {
	l, isExist := i.list[bucket]
	if isExist {
		return l
	}
	l = &list.List{
		Items:     map[string][][]byte{},
		TTL:       map[string]uint32{},
		TimeStamp: map[string]uint64{},
	}
	i.list[bucket] = l
	return l
}

func (i *index) deleteList(bucket string) {
	delete(i.list, bucket)
}

func (i *index) addList(bucket string) {
	l := &list.List{
		Items:     map[string][][]byte{},
		TTL:       map[string]uint32{},
		TimeStamp: map[string]uint64{},
	}
	i.list[bucket] = l
}

func (i *index) rangeList(f func(l *list.List)) {
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
