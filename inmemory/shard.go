// Copyright 2021 The nutsdb Author. All rights reserved.
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

package inmemory

import (
	"sync"

	"github.com/nutsdb/nutsdb"
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
