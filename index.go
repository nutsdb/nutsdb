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
