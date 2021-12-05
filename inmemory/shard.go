package inmemory

import (
	"sync"

	"github.com/xujiajun/nutsdb"
)

type ShardDB struct {
	BPTreeIdx    nutsdb.BPTreeIdx // Hint Index
	SetIdx       nutsdb.SetIdx
	SortedSetIdx nutsdb.SortedSetIdx
	ListIdx      nutsdb.ListIdx
	mu           sync.RWMutex
	KeyCount     int
}

func InitShardDB() *ShardDB {
	return &ShardDB{
		BPTreeIdx:    make(nutsdb.BPTreeIdx),
		SetIdx:       make(nutsdb.SetIdx),
		SortedSetIdx: make(nutsdb.SortedSetIdx),
		ListIdx:      make(nutsdb.ListIdx),
		mu:           sync.RWMutex{},
		KeyCount:     0,
	}
}

func (sd *ShardDB) Lock(writable bool) {
	if writable {
		sd.mu.Lock()
	} else {
		sd.mu.RLock()
	}
}

func (sd *ShardDB) Unlock(writable bool) {
	if writable {
		sd.mu.Unlock()
	} else {
		sd.mu.RUnlock()
	}
}
