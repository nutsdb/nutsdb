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
	"context"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestKeyMetadataSize(t *testing.T) {
	var meta keyMetadata
	size := unsafe.Sizeof(meta)
	t.Logf("keyMetadata size: %d bytes", size)
	assert.LessOrEqual(t, size, uintptr(32), "keyMetadata size should be at most 32 bytes")
}

func TestNewKeyMetadata(t *testing.T) {
	bucketId := uint64(123)
	key := []byte("test-key")
	ds := uint16(1)
	timestamp := uint64(1234567890)

	meta := newKeyMetadata(bucketId, key, ds, timestamp)

	assert.Equal(t, bucketId, meta.bucketId)
	assert.Equal(t, ds, meta.ds)
	assert.Equal(t, timestamp, meta.timestamp)
	assert.NotZero(t, meta.keyHash)
}

func TestNewKeyReference(t *testing.T) {
	bucketId := uint64(456)
	key := []byte("another-key")
	ds := uint16(2)
	timestamp := uint64(9876543210)

	ref := newKeyReference(bucketId, key, ds, timestamp)

	assert.Equal(t, bucketId, ref.metadata.bucketId)
	assert.Equal(t, ds, ref.metadata.ds)
	assert.Equal(t, timestamp, ref.metadata.timestamp)
	assert.NotZero(t, ref.metadata.keyHash)
	assert.Equal(t, key, ref.key)
}

func TestHashKey(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
	}{
		{"empty key", []byte{}},
		{"simple key", []byte("key")},
		{"long key", []byte("this-is-a-very-long-key-for-testing-purposes")},
		{"binary key", []byte{0x00, 0x01, 0x02, 0xFF}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash1 := hashKey(tt.key)
			hash2 := hashKey(tt.key)
			assert.Equal(t, hash1, hash2, "hash should be deterministic")
		})
	}
}

func TestHashKeyUniqueness(t *testing.T) {
	key1 := []byte("key1")
	key2 := []byte("key2")

	hash1 := hashKey(key1)
	hash2 := hashKey(key2)

	assert.NotEqual(t, hash1, hash2, "different keys should produce different hashes")
}

func TestMakeRegistryKey(t *testing.T) {
	tests := []struct {
		name     string
		bucketId uint64
		keyHash  uint64
		expected string
	}{
		{"zero values", 0, 0, "0:0"},
		{"small values", 1, 2, "1:2"},
		{"large values", 18446744073709551615, 9223372036854775807, "18446744073709551615:9223372036854775807"},
		{"mixed values", 123, 456789, "123:456789"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := makeRegistryKey(tt.bucketId, tt.keyHash)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAppendUint64(t *testing.T) {
	tests := []struct {
		name     string
		n        uint64
		expected string
	}{
		{"zero", 0, "0"},
		{"one", 1, "1"},
		{"small", 42, "42"},
		{"large", 1234567890, "1234567890"},
		{"max uint64", 18446744073709551615, "18446744073709551615"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 0, 32)
			result := appendUint64(buf, tt.n)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

func TestAppendUint64WithPrefix(t *testing.T) {
	buf := []byte("prefix:")
	result := appendUint64(buf, 123)
	assert.Equal(t, "prefix:123", string(result))
}

func TestTimingWheelManager_DeregisterKey(t *testing.T) {
	clock := NewMockClock(1000000)
	config := Config{
		EnableTimingWheel: true,
		WheelSlotDuration: 100 * time.Millisecond,
		WheelSize:         10,
		QueueSize:         100,
	}
	queue := newExpirationQueue(config.QueueSize)
	mgr := NewTimingWheelManager(config, queue)

	bucketId := uint64(1)
	key := []byte("test-key")
	ds := uint16(1)
	ttl := uint32(1)
	timestamp := uint64(clock.NowMillis())

	registered := mgr.RegisterKey(bucketId, key, ds, ttl, timestamp)
	assert.True(t, registered, "key should be registered")
	assert.Equal(t, int64(1), mgr.registeredKeys.Load(), "registered keys count should be 1")

	keyHash := hashKey(key)
	_, exists := mgr.keyCache.Load(keyHash)
	assert.True(t, exists, "key should be in cache")

	deregistered := mgr.DeregisterKey(bucketId, key)
	assert.True(t, deregistered, "key should be deregistered")
	assert.Equal(t, int64(0), mgr.registeredKeys.Load(), "registered keys count should be 0")

	_, exists = mgr.keyCache.Load(keyHash)
	assert.False(t, exists, "key should be removed from cache")

	deregistered = mgr.DeregisterKey(bucketId, key)
	assert.False(t, deregistered, "deregistering non-existent key should return false")
}

func TestTimingWheelManager_DeregisterKey_WhenDisabled(t *testing.T) {
	config := Config{
		EnableTimingWheel: false,
		QueueSize:         100,
	}
	queue := newExpirationQueue(config.QueueSize)
	mgr := NewTimingWheelManager(config, queue)

	bucketId := uint64(1)
	key := []byte("test-key")

	deregistered := mgr.DeregisterKey(bucketId, key)
	assert.False(t, deregistered, "deregister should return false when wheel is disabled")
}

func TestTimingWheelManager_DeregisterKey_PreventsExpiration(t *testing.T) {
	clock := NewMockClock(1000000)
	config := Config{
		EnableTimingWheel: true,
		WheelSlotDuration: 100 * time.Millisecond,
		WheelSize:         10,
		QueueSize:         100,
	}
	queue := newExpirationQueue(config.QueueSize)
	mgr := NewTimingWheelManager(config, queue)

	bucketId := uint64(1)
	key := []byte("test-key")
	ds := uint16(1)
	ttl := uint32(1) // 1 second
	timestamp := uint64(clock.NowMillis())

	registered := mgr.RegisterKey(bucketId, key, ds, ttl, timestamp)
	assert.True(t, registered, "key should be registered")

	deregistered := mgr.DeregisterKey(bucketId, key)
	assert.True(t, deregistered, "key should be deregistered")

	metadata := newKeyMetadata(bucketId, key, ds, timestamp)
	mgr.onKeyExpired(metadata, 0)

	select {
	case event := <-queue.events:
		t.Errorf("unexpected expiration event for deregistered key: %+v", event)
	default:
	}
}

func TestTimingWheelManager_OnKeyExpired_SuccessfulPush(t *testing.T) {
	clock := NewMockClock(1000000)
	config := Config{
		EnableTimingWheel: true,
		WheelSlotDuration: 100 * time.Millisecond,
		WheelSize:         10,
		QueueSize:         100,
	}
	queue := newExpirationQueue(config.QueueSize)
	mgr := NewTimingWheelManager(config, queue)

	bucketId := uint64(1)
	key := []byte("test-key")
	ds := uint16(1)
	ttl := uint32(1) // 1 second
	timestamp := uint64(clock.NowMillis())

	registered := mgr.RegisterKey(bucketId, key, ds, ttl, timestamp)
	assert.True(t, registered, "key should be registered")
	assert.Equal(t, int64(1), mgr.registeredKeys.Load())

	metadata := newKeyMetadata(bucketId, key, ds, timestamp)
	mgr.onKeyExpired(metadata, 0)

	select {
	case event := <-queue.events:
		assert.Equal(t, bucketId, event.BucketId)
		assert.Equal(t, key, event.Key)
		assert.Equal(t, ds, event.Ds)
		assert.Equal(t, timestamp, event.Timestamp)
	default:
		t.Fatal("expected expiration event but none received")
	}

	assert.Equal(t, int64(1), mgr.triggeredDeletes.Load())
	assert.Equal(t, int64(0), mgr.failedPushes.Load())
	assert.Equal(t, int64(0), mgr.registeredKeys.Load())

	keyHash := hashKey(key)
	_, exists := mgr.keyCache.Load(keyHash)
	assert.False(t, exists, "key should be removed from cache after expiration")
}

func TestTimingWheelManager_OnKeyExpired_QueueFull(t *testing.T) {
	clock := NewMockClock(1000000)
	config := Config{
		EnableTimingWheel: true,
		WheelSlotDuration: 100 * time.Millisecond,
		WheelSize:         10,
		QueueSize:         2, // Small queue to test full condition
	}
	queue := newExpirationQueue(config.QueueSize)
	mgr := NewTimingWheelManager(config, queue)

	for i := 0; i < config.QueueSize; i++ {
		queue.push(&ExpirationEvent{
			BucketId:  uint64(i),
			Key:       []byte("filler"),
			Ds:        1,
			Timestamp: uint64(clock.NowMillis()),
		})
	}

	bucketId := uint64(100)
	key := []byte("test-key")
	ds := uint16(1)
	ttl := uint32(1) // 1 second
	timestamp := uint64(clock.NowMillis())

	registered := mgr.RegisterKey(bucketId, key, ds, ttl, timestamp)
	assert.True(t, registered, "key should be registered")

	metadata := newKeyMetadata(bucketId, key, ds, timestamp)
	mgr.onKeyExpired(metadata, 0)

	failedPushes := mgr.failedPushes.Load()
	assert.Equal(t, int64(1), failedPushes, "failed pushes should be 1")

	select {
	case event := <-mgr.retryBuffer:
		assert.Equal(t, bucketId, event.BucketId)
		assert.Equal(t, key, event.Key)
		assert.Equal(t, ds, event.Ds)
		assert.Equal(t, timestamp, event.Timestamp)
	default:
		t.Fatalf("expected event in retry buffer but none found. Failed pushes: %d, Retry buffer len: %d",
			failedPushes, len(mgr.retryBuffer))
	}
}

func TestTimingWheelManager_OnKeyExpired_MultiLap(t *testing.T) {
	clock := NewMockClock(1000000)
	config := Config{
		EnableTimingWheel: true,
		WheelSlotDuration: 100 * time.Millisecond,
		WheelSize:         10, // Total coverage: 1 second
		QueueSize:         100,
	}
	queue := newExpirationQueue(config.QueueSize)
	mgr := NewTimingWheelManager(config, queue)
	defer mgr.registry.clear()

	bucketId := uint64(1)
	key := []byte("test-key")
	ds := uint16(1)
	ttl := uint32(3) // 3 seconds, exceeds wheel coverage
	timestamp := uint64(clock.NowMillis())

	registered := mgr.RegisterKey(bucketId, key, ds, ttl, timestamp)
	assert.True(t, registered, "key should be registered")

	metadata := newKeyMetadata(bucketId, key, ds, timestamp)
	mgr.onKeyExpired(metadata, 1)

	select {
	case event := <-queue.events:
		t.Errorf("unexpected early expiration event: %+v", event)
	default:
	}

	mgr.onKeyExpired(metadata, 0)
	select {
	case event := <-queue.events:
		assert.Equal(t, bucketId, event.BucketId)
		assert.Equal(t, key, event.Key)
		assert.Equal(t, ds, event.Ds)
		assert.Equal(t, timestamp, event.Timestamp)
	default:
		t.Fatal("expected expiration event after full TTL but none received")
	}
}

func TestTimingWheelManager_OnKeyExpired_DeletedKeySkipped(t *testing.T) {
	clock := NewMockClock(1000000)
	config := Config{
		EnableTimingWheel: true,
		WheelSlotDuration: 100 * time.Millisecond,
		WheelSize:         10,
		QueueSize:         100,
	}
	queue := newExpirationQueue(config.QueueSize)
	mgr := NewTimingWheelManager(config, queue)

	bucketId := uint64(1)
	key := []byte("test-key")
	ds := uint16(1)
	timestamp := uint64(clock.NowMillis())

	metadata := newKeyMetadata(bucketId, key, ds, timestamp)

	mgr.onKeyExpired(metadata, 0)

	select {
	case event := <-queue.events:
		t.Errorf("unexpected expiration event for non-existent key: %+v", event)
	default:
	}

	assert.Equal(t, int64(0), mgr.triggeredDeletes.Load())
	assert.Equal(t, int64(0), mgr.failedPushes.Load())
}

// TestTimingWheelManager_Lifecycle tests the Start and Stop methods.
func TestTimingWheelManager_Lifecycle(t *testing.T) {
	t.Run("start and stop enabled wheel", func(t *testing.T) {
		config := Config{
			EnableTimingWheel: true,
			WheelSlotDuration: 100 * time.Millisecond,
			WheelSize:         10,
			QueueSize:         10,
		}
		queue := newExpirationQueue(config.QueueSize)
		mgr := NewTimingWheelManager(config, queue)

		ctx := context.Background()
		err := mgr.Start(ctx)
		assert.NoError(t, err)

		assert.True(t, mgr.lifecycle.IsRunning())

		err = mgr.Stop(1 * time.Second)
		assert.NoError(t, err)

		assert.True(t, mgr.lifecycle.IsStopped())
	})

	t.Run("start and stop disabled wheel", func(t *testing.T) {
		config := Config{
			EnableTimingWheel: false,
			QueueSize:         10,
		}
		queue := newExpirationQueue(config.QueueSize)
		mgr := NewTimingWheelManager(config, queue)

		ctx := context.Background()
		err := mgr.Start(ctx)
		assert.NoError(t, err)

		err = mgr.Stop(1 * time.Second)
		assert.NoError(t, err)
	})

	t.Run("stop is idempotent", func(t *testing.T) {
		config := Config{
			EnableTimingWheel: true,
			WheelSlotDuration: 100 * time.Millisecond,
			WheelSize:         10,
			QueueSize:         10,
		}
		queue := newExpirationQueue(config.QueueSize)
		mgr := NewTimingWheelManager(config, queue)

		ctx := context.Background()
		err := mgr.Start(ctx)
		assert.NoError(t, err)

		err = mgr.Stop(1 * time.Second)
		assert.NoError(t, err)

		err = mgr.Stop(1 * time.Second)
		assert.NoError(t, err)
	})

	t.Run("name returns correct value", func(t *testing.T) {
		config := Config{
			EnableTimingWheel: true,
			WheelSlotDuration: 100 * time.Millisecond,
			WheelSize:         10,
			QueueSize:         10,
		}
		queue := newExpirationQueue(config.QueueSize)
		mgr := NewTimingWheelManager(config, queue)

		assert.Equal(t, "TimingWheelManager", mgr.Name())
	})
}

// TestTimingWheelManager_GetMetrics tests the GetMetrics method.
func TestTimingWheelManager_GetMetrics(t *testing.T) {
	clock := NewMockClock(1000000)
	config := Config{
		EnableTimingWheel: true,
		WheelSlotDuration: 100 * time.Millisecond,
		WheelSize:         10,
		QueueSize:         10,
	}
	queue := newExpirationQueue(config.QueueSize)
	mgr := NewTimingWheelManager(config, queue)

	ctx := context.Background()
	err := mgr.Start(ctx)
	assert.NoError(t, err)
	defer mgr.Stop(1 * time.Second)

	metrics := mgr.GetMetrics()
	assert.Equal(t, int64(0), metrics.RegisteredKeys)
	assert.Equal(t, int64(0), metrics.TriggeredDeletes)
	assert.Equal(t, int64(0), metrics.FailedPushes)

	mgr.RegisterKey(1, []byte("key1"), 1, 1, uint64(clock.NowSeconds()))

	metrics = mgr.GetMetrics()
	assert.Equal(t, int64(1), metrics.RegisteredKeys)

	mgr.DeregisterKey(1, []byte("key1"))

	metrics = mgr.GetMetrics()
	assert.Equal(t, int64(0), metrics.RegisteredKeys)
}
