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
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/ttl/clock"
	"github.com/stretchr/testify/assert"
)

func TestMockManager(t *testing.T) {
	mc := clock.NewMockClock(1000) // Start at 1000ms
	mm := NewMockManager(mc)

	bucketId := BucketId(1)
	key := "test-key"
	ds := uint16(1)
	expired := false

	callback := func(bid BucketId, k []byte, d uint16) {
		assert.Equal(t, bucketId, bid)
		assert.Equal(t, []byte(key), k)
		assert.Equal(t, ds, d)
		expired = true
	}

	// Add a task that expires in 100ms (at 1100ms)
	mm.Add(bucketId, key, 100*time.Millisecond, ds, callback)

	assert.True(t, mm.Exist(bucketId, key))

	// Advance time by 50ms (to 1050ms)
	mc.AdvanceTime(50 * time.Millisecond)
	assert.False(t, expired)
	assert.True(t, mm.Exist(bucketId, key))

	// Advance time by another 50ms (to 1100ms)
	mc.AdvanceTime(50 * time.Millisecond)
	assert.True(t, expired)
	assert.True(t, mm.Exist(bucketId, key)) // Should still exist until Del is called

	mm.Del(bucketId, key)
	assert.False(t, mm.Exist(bucketId, key))
}

func TestMockManager_Del(t *testing.T) {
	mc := clock.NewMockClock(1000)
	mm := NewMockManager(mc)

	bucketId := BucketId(1)
	key := "test-key"
	ds := uint16(1)
	expired := false

	callback := func(bid BucketId, k []byte, d uint16) {
		expired = true
	}

	mm.Add(bucketId, key, 100*time.Millisecond, ds, callback)
	assert.True(t, mm.Exist(bucketId, key))

	mm.Del(bucketId, key)
	assert.False(t, mm.Exist(bucketId, key))

	mc.AdvanceTime(200 * time.Millisecond)
	assert.False(t, expired)
}
