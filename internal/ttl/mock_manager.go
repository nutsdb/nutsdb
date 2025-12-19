// Copyright 2025 The nutsdb Author. All rights reserved.
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

package ttl

import (
	"sync"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/ttl/clock"
)

type ttlTask struct {
	bucketId   core.BucketId
	key        string
	expireTime int64 // milliseconds
	ds         uint16
	timestamp  uint64
	callback   ExpireCallback
	triggered  bool
}

// MockManager implements Manager for testing purposes.
// It is driven by a MockClock and executes callbacks synchronously when time advances.
type MockManager struct {
	mu    sync.RWMutex
	clock *clock.MockClock
	tasks map[core.BucketId]map[string]*ttlTask
}

// NewMockManager creates a new MockManager instance.
func NewMockManager(mc *clock.MockClock) *MockManager {
	mm := &MockManager{
		clock: mc,
		tasks: make(map[core.BucketId]map[string]*ttlTask),
	}
	mc.SetOnAdvance(mm.checkExpirations)
	return mm
}

// Run starts the TTL manager (no-op for MockManager)
func (mm *MockManager) Run() {}

// Exist checks if a TTL entry exists for the given bucket and key
func (mm *MockManager) Exist(bucketId core.BucketId, key string) bool {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	if bucketTasks, ok := mm.tasks[bucketId]; ok {
		_, exists := bucketTasks[key]
		return exists
	}
	return false
}

// Add adds a TTL entry with the specified expiration duration.
func (mm *MockManager) Add(bucketId core.BucketId, key string, expire time.Duration, ds uint16, timestamp uint64, callback ExpireCallback) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if _, ok := mm.tasks[bucketId]; !ok {
		mm.tasks[bucketId] = make(map[string]*ttlTask)
	}

	expireTime := mm.clock.NowMillis() + expire.Milliseconds()
	mm.tasks[bucketId][key] = &ttlTask{
		bucketId:   bucketId,
		key:        key,
		expireTime: expireTime,
		ds:         ds,
		timestamp:  timestamp,
		callback:   callback,
	}
}

// Del removes a TTL entry for the given bucket and key
func (mm *MockManager) Del(bucketId core.BucketId, key string) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if bucketTasks, ok := mm.tasks[bucketId]; ok {
		delete(bucketTasks, key)
		if len(bucketTasks) == 0 {
			delete(mm.tasks, bucketId)
		}
	}
}

// Close closes the TTL manager
func (mm *MockManager) Close() {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.tasks = nil
}

func (mm *MockManager) checkExpirations(nowMillis int64) {
	mm.mu.Lock()
	var expiredTasks []*ttlTask

	for _, bucketTasks := range mm.tasks {
		for _, task := range bucketTasks {
			if !task.triggered && task.expireTime <= nowMillis {
				task.triggered = true
				expiredTasks = append(expiredTasks, task)
			}
		}
	}
	mm.mu.Unlock()

	// Execute callbacks outside of the lock to avoid deadlocks
	for _, task := range expiredTasks {
		if task.callback != nil {
			task.callback(task.bucketId, []byte(task.key), task.ds, task.timestamp)
		}
	}
}
