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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testKeyMeta struct {
	bucketId  uint64
	key       []byte
	ds        uint16
	timestamp uint64
}

func startTestService(t *testing.T, clock Clock, config Config, callback BatchExpiredCallback) *Service {
	t.Helper()
	service := NewService(clock, config, callback)
	require.NoError(t, service.Start(context.Background()))
	t.Cleanup(func() {
		_ = service.Stop(1 * time.Second)
	})
	return service
}

func waitForBatch(t *testing.T, eventsCh <-chan []*ExpirationEvent) []*ExpirationEvent {
	t.Helper()
	select {
	case batch := <-eventsCh:
		return batch
	case <-time.After(2 * time.Second):
		t.Fatal("expected expiration events")
		return nil
	}
}

func collectEventsWithTimeout(t *testing.T, eventsCh <-chan []*ExpirationEvent, expected int, timeout time.Duration) []*ExpirationEvent {
	t.Helper()
	collected := make([]*ExpirationEvent, 0, expected)
	deadline := time.After(timeout)
	for len(collected) < expected {
		select {
		case batch := <-eventsCh:
			collected = append(collected, batch...)
		case <-deadline:
			t.Fatalf("expected %d events, got %d", expected, len(collected))
		}
	}
	return collected
}

func collectEvents(t *testing.T, eventsCh <-chan []*ExpirationEvent, expected int) []*ExpirationEvent {
	return collectEventsWithTimeout(t, eventsCh, expected, 5*time.Second)
}

func cloneEvents(events []*ExpirationEvent) []*ExpirationEvent {
	cloned := make([]*ExpirationEvent, len(events))
	copy(cloned, events)
	return cloned
}

// TestTimingWheelIntegration_EndToEnd tests the complete timing wheel flow
// with various TTL values and verifies active deletion.
func TestTimingWheelIntegration_EndToEnd(t *testing.T) {
	clock := NewMockClock(1000000)
	config := Config{
		EnableTimingWheel: true,
		WheelSlotDuration: 100 * time.Millisecond,
		WheelSize:         20,
		QueueSize:         100,
		BatchSize:         1,
		BatchTimeout:      time.Hour,
	}

	deletedKeys := make(map[string]int)
	var mu sync.Mutex
	eventsCh := make(chan []*ExpirationEvent, 64)
	callback := func(events []*ExpirationEvent) {
		cloned := cloneEvents(events)
		mu.Lock()
		for _, event := range cloned {
			deletedKeys[string(event.Key)]++
		}
		mu.Unlock()
		eventsCh <- cloned
	}

	service := startTestService(t, clock, config, callback)
	require.NotNil(t, service.wheelManager)

	timestamp := uint64(clock.NowMillis())
	testCases := []struct {
		name     string
		key      string
		ttl      uint32
		bucketId uint64
		ds       uint16
	}{
		{"short-ttl-1", "key-short-1", 1, 1, 1},
		{"short-ttl-2", "key-short-2", 1, 1, 1},
		{"medium-ttl-1", "key-medium-1", 2, 2, 1},
		{"medium-ttl-2", "key-medium-2", 2, 2, 1},
		{"long-ttl-1", "key-long-1", 3, 3, 1},
		{"long-ttl-2", "key-long-2", 3, 3, 1},
	}

	metaByKey := make(map[string]testKeyMeta)
	for _, tc := range testCases {
		keyBytes := []byte(tc.key)
		metaByKey[tc.key] = testKeyMeta{
			bucketId:  tc.bucketId,
			key:       keyBytes,
			ds:        tc.ds,
			timestamp: timestamp,
		}
		service.RegisterKeyForActiveExpiration(
			tc.bucketId,
			keyBytes,
			tc.ds,
			tc.ttl,
			timestamp,
		)
	}

	metrics := service.wheelManager.GetMetrics()
	assert.Equal(t, int64(6), metrics.RegisteredKeys, "should have 6 registered keys")
	assert.Equal(t, int64(0), metrics.TriggeredDeletes, "should have 0 triggered deletes initially")

	expireKeys := func(keys []string) {
		for _, key := range keys {
			meta := metaByKey[key]
			service.wheelManager.onKeyExpired(newKeyMetadata(meta.bucketId, meta.key, meta.ds, meta.timestamp), 0)
		}
		collectEvents(t, eventsCh, len(keys))
	}

	expireKeys([]string{"key-short-1", "key-short-2"})
	mu.Lock()
	assert.Equal(t, 1, deletedKeys["key-short-1"])
	assert.Equal(t, 1, deletedKeys["key-short-2"])
	mu.Unlock()

	expireKeys([]string{"key-medium-1", "key-medium-2"})
	mu.Lock()
	assert.Equal(t, 1, deletedKeys["key-medium-1"])
	assert.Equal(t, 1, deletedKeys["key-medium-2"])
	mu.Unlock()

	expireKeys([]string{"key-long-1", "key-long-2"})
	mu.Lock()
	assert.Equal(t, 1, deletedKeys["key-long-1"])
	assert.Equal(t, 1, deletedKeys["key-long-2"])
	assert.Equal(t, 6, len(deletedKeys))
	mu.Unlock()

	metrics = service.wheelManager.GetMetrics()
	assert.Equal(t, int64(0), metrics.RegisteredKeys, "should have 0 registered keys after all expired")
	assert.Equal(t, int64(6), metrics.TriggeredDeletes, "should have 6 triggered deletes")
	assert.Equal(t, int64(0), metrics.FailedPushes, "should have 0 failed pushes")
}

// TestTimingWheelIntegration_KeyNotAccessibleAfterDeletion verifies that
// keys are not accessible after active deletion.
func TestTimingWheelIntegration_KeyNotAccessibleAfterDeletion(t *testing.T) {
	clock := NewMockClock(1000000)
	config := Config{
		EnableTimingWheel: true,
		WheelSlotDuration: 100 * time.Millisecond,
		WheelSize:         10,
		QueueSize:         100,
		BatchSize:         1,
		BatchTimeout:      time.Hour,
	}

	keyStore := make(map[string]bool)
	var storeMu sync.Mutex
	calls := make(chan []*ExpirationEvent, 16)

	callback := func(events []*ExpirationEvent) {
		cloned := cloneEvents(events)
		storeMu.Lock()
		for _, event := range cloned {
			delete(keyStore, string(event.Key))
		}
		storeMu.Unlock()
		calls <- cloned
	}

	service := startTestService(t, clock, config, callback)
	keys := []string{"key1", "key2", "key3"}
	timestamp := uint64(clock.NowMillis())

	for _, key := range keys {
		storeMu.Lock()
		keyStore[key] = true
		storeMu.Unlock()

		service.RegisterKeyForActiveExpiration(
			1,
			[]byte(key),
			1,
			1,
			timestamp,
		)
	}

	for _, key := range keys {
		meta := newKeyMetadata(1, []byte(key), 1, timestamp)
		service.wheelManager.onKeyExpired(meta, 0)
	}

	collectEvents(t, calls, len(keys))

	storeMu.Lock()
	for _, key := range keys {
		_, exists := keyStore[key]
		assert.False(t, exists, "key %s should not exist after expiration", key)
	}
	storeMu.Unlock()
}

// TestTimingWheelIntegration_DisabledWheel verifies that when timing wheel
// is disabled, no active deletion occurs.
func TestTimingWheelIntegration_DisabledWheel(t *testing.T) {
	clock := NewMockClock(1000000)
	config := Config{
		EnableTimingWheel: false,
		QueueSize:         100,
		BatchSize:         1,
		BatchTimeout:      time.Hour,
	}

	var deletionCount atomic.Int64
	calls := make(chan []*ExpirationEvent, 4)
	callback := func(events []*ExpirationEvent) {
		cloned := cloneEvents(events)
		deletionCount.Add(int64(len(cloned)))
		calls <- cloned
	}

	service := startTestService(t, clock, config, callback)
	require.Nil(t, service.wheelManager, "wheel manager should be nil when disabled")

	service.RegisterKeyForActiveExpiration(
		1,
		[]byte("key1"),
		1,
		1,
		uint64(clock.NowMillis()),
	)

	assert.Equal(t, int64(0), deletionCount.Load(), "no deletions should occur when wheel is disabled")
	select {
	case <-calls:
		t.Fatal("unexpected expiration event when wheel is disabled")
	default:
	}
}

// TestTimingWheelIntegration_LazyDeletionDedupesTimingWheel verifies that lazy deletion
// triggers a single deletion event and deregisters the timing wheel entry.
func TestTimingWheelIntegration_LazyDeletionDedupesTimingWheel(t *testing.T) {
	clock := NewMockClock(1000000)
	config := Config{
		EnableTimingWheel: true,
		WheelSlotDuration: 200 * time.Millisecond,
		WheelSize:         20,
		QueueSize:         100,
		BatchSize:         1,
		BatchTimeout:      time.Hour,
	}

	eventsCh := make(chan []*ExpirationEvent, 4)
	var service *Service
	callback := func(events []*ExpirationEvent) {
		cloned := cloneEvents(events)
		for _, event := range cloned {
			service.DeregisterKeyFromActiveExpiration(event.BucketId, event.Key)
		}
		eventsCh <- cloned
	}

	service = startTestService(t, clock, config, callback)
	require.NotNil(t, service.wheelManager)

	bucketId := uint64(1)
	ds := uint16(1)
	key := []byte("lazy-key")
	timestamp := uint64(clock.NowMillis())

	service.RegisterKeyForActiveExpiration(bucketId, key, ds, 1, timestamp)

	clock.AdvanceTime(2 * time.Second)
	record := &core.Record{Key: key, TTL: 1, Timestamp: timestamp}
	valid := service.GetChecker().FilterExpiredRecord(bucketId, key, record, ds)
	assert.False(t, valid)

	collectEvents(t, eventsCh, 1)

	_, exists := service.wheelManager.keyCache.Load(hashKey(key))
	assert.False(t, exists, "key should be deregistered after lazy deletion")

	service.wheelManager.onKeyExpired(newKeyMetadata(bucketId, key, ds, timestamp), 0)
	select {
	case <-eventsCh:
		t.Fatal("unexpected duplicate expiration event after deregistration")
	default:
	}
}

// TestTimingWheelIntegration_ExpirationPipelineUnifiedProcessing verifies that
// events from lazy deletion and timing wheel are processed together in order.
func TestTimingWheelIntegration_ExpirationPipelineUnifiedProcessing(t *testing.T) {
	clock := NewMockClock(1000000)
	config := Config{
		EnableTimingWheel: true,
		WheelSlotDuration: 200 * time.Millisecond,
		WheelSize:         20,
		QueueSize:         100,
		BatchSize:         2,
		BatchTimeout:      time.Hour,
	}

	eventsCh := make(chan []*ExpirationEvent, 2)
	callback := func(events []*ExpirationEvent) {
		eventsCh <- cloneEvents(events)
	}

	service := startTestService(t, clock, config, callback)
	require.NotNil(t, service.wheelManager)

	bucketId := uint64(1)
	ds := uint16(1)
	timestamp := uint64(clock.NowMillis())
	lazyKey := []byte("lazy-key")
	activeKey := []byte("active-key")

	service.RegisterKeyForActiveExpiration(bucketId, activeKey, ds, 1, timestamp)

	clock.AdvanceTime(2 * time.Second)
	record := &core.Record{Key: lazyKey, TTL: 1, Timestamp: timestamp}
	valid := service.GetChecker().FilterExpiredRecord(bucketId, lazyKey, record, ds)
	assert.False(t, valid)

	service.wheelManager.onKeyExpired(newKeyMetadata(bucketId, activeKey, ds, timestamp), 0)

	batch := waitForBatch(t, eventsCh)
	require.Len(t, batch, 2)
	assert.Equal(t, lazyKey, batch[0].Key)
	assert.Equal(t, activeKey, batch[1].Key)
}

// TestTimingWheelIntegration_MergeCleanupInteraction simulates merge cleanup
// filtering with Checker.IsExpired while timing wheel continues to function.
func TestTimingWheelIntegration_MergeCleanupInteraction(t *testing.T) {
	clock := NewMockClock(1000000)
	config := Config{
		EnableTimingWheel: true,
		WheelSlotDuration: 200 * time.Millisecond,
		WheelSize:         20,
		QueueSize:         100,
		BatchSize:         1,
		BatchTimeout:      time.Hour,
	}

	eventsCh := make(chan []*ExpirationEvent, 8)
	callback := func(events []*ExpirationEvent) {
		eventsCh <- cloneEvents(events)
	}

	service := startTestService(t, clock, config, callback)
	checker := service.GetChecker()

	bucketId := uint64(1)
	ds := uint16(1)
	timestamp := uint64(clock.NowMillis())
	records := []*core.Record{
		{Key: []byte("merge-expired"), TTL: 1, Timestamp: timestamp},
		{Key: []byte("merge-live-1"), TTL: 5, Timestamp: timestamp},
		{Key: []byte("merge-live-2"), TTL: 5, Timestamp: timestamp},
	}

	for _, record := range records {
		service.RegisterKeyForActiveExpiration(bucketId, record.Key, ds, record.TTL, record.Timestamp)
	}

	clock.AdvanceTime(2 * time.Second)

	merged := make([]*core.Record, 0, len(records))
	for _, record := range records {
		if !checker.IsExpired(record.TTL, record.Timestamp) {
			merged = append(merged, record)
		}
	}

	require.Len(t, merged, 2)
	assert.Equal(t, "merge-live-1", string(merged[0].Key))
	assert.Equal(t, "merge-live-2", string(merged[1].Key))

	for _, record := range merged {
		service.wheelManager.onKeyExpired(newKeyMetadata(bucketId, record.Key, ds, record.Timestamp), 0)
	}

	collectEvents(t, eventsCh, len(merged))
}

// TestTimingWheelIntegration_ConcurrentOperations tests concurrent
// registration and deregistration operations with deterministic expiration.
func TestTimingWheelIntegration_ConcurrentOperations(t *testing.T) {
	clock := NewMockClock(1000000)
	config := Config{
		EnableTimingWheel: true,
		WheelSlotDuration: 100 * time.Millisecond,
		WheelSize:         20,
		QueueSize:         1000,
		BatchSize:         50,
		BatchTimeout:      time.Hour,
	}

	var deletionCount atomic.Int64
	calls := make(chan []*ExpirationEvent, 64)
	callback := func(events []*ExpirationEvent) {
		cloned := cloneEvents(events)
		deletionCount.Add(int64(len(cloned)))
		calls <- cloned
	}

	service := startTestService(t, clock, config, callback)

	numGoroutines := 10
	keysPerGoroutine := 20
	var wg sync.WaitGroup

	metas := make([]testKeyMeta, 0, numGoroutines*keysPerGoroutine)
	var metasMu sync.Mutex
	baseTimestamp := uint64(clock.NowMillis())

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineId int) {
			defer wg.Done()
			for i := 0; i < keysPerGoroutine; i++ {
				key := []byte{byte(goroutineId), byte(i)}
				meta := testKeyMeta{
					bucketId:  uint64(goroutineId),
					key:       append([]byte(nil), key...),
					ds:        1,
					timestamp: baseTimestamp,
				}
				metasMu.Lock()
				metas = append(metas, meta)
				metasMu.Unlock()

				service.RegisterKeyForActiveExpiration(
					meta.bucketId,
					meta.key,
					meta.ds,
					2,
					meta.timestamp,
				)
			}
		}(g)
	}

	wg.Wait()

	metrics := service.wheelManager.GetMetrics()
	expectedKeys := int64(numGoroutines * keysPerGoroutine)
	assert.Equal(t, expectedKeys, metrics.RegisteredKeys, "all keys should be registered")

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineId int) {
			defer wg.Done()
			for i := 0; i < keysPerGoroutine/2; i++ {
				key := []byte{byte(goroutineId), byte(i)}
				service.DeregisterKeyFromActiveExpiration(uint64(goroutineId), key)
			}
		}(g)
	}

	wg.Wait()

	metrics = service.wheelManager.GetMetrics()
	expectedRemaining := expectedKeys / 2
	assert.Equal(t, expectedRemaining, metrics.RegisteredKeys, "half the keys should remain")

	for _, meta := range metas {
		service.wheelManager.onKeyExpired(newKeyMetadata(meta.bucketId, meta.key, meta.ds, meta.timestamp), 0)
	}

	collectEvents(t, calls, int(expectedRemaining))

	metrics = service.wheelManager.GetMetrics()
	assert.Equal(t, int64(0), metrics.RegisteredKeys, "all keys should be expired or deregistered")
	assert.Equal(t, expectedRemaining, metrics.TriggeredDeletes, "only remaining keys should trigger deletes")
	assert.Equal(t, expectedRemaining, deletionCount.Load())
}

// TestTimingWheelIntegration_HighLoad tests timing wheel behavior under high load
// using deterministic expiration events.
func TestTimingWheelIntegration_HighLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high load test in short mode")
	}

	clock := NewMockClock(1000000)
	const numKeys = 12000
	const numBuckets = 100
	const minTTL = 1
	const maxTTL = 4
	const numGoroutines = 20

	config := Config{
		EnableTimingWheel: true,
		WheelSlotDuration: 100 * time.Millisecond,
		WheelSize:         50,
		QueueSize:         numKeys + 100,
		BatchSize:         500,
		BatchTimeout:      time.Hour,
	}

	var deletionCount atomic.Int64
	calls := make(chan []*ExpirationEvent, 128)
	callback := func(events []*ExpirationEvent) {
		cloned := cloneEvents(events)
		deletionCount.Add(int64(len(cloned)))
		calls <- cloned
	}

	service := startTestService(t, clock, config, callback)
	metas := make([]testKeyMeta, numKeys)
	baseTimestamp := uint64(clock.NowMillis())

	var wg sync.WaitGroup
	keysPerGoroutine := numKeys / numGoroutines

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineId int) {
			defer wg.Done()
			baseIndex := goroutineId * keysPerGoroutine
			for i := 0; i < keysPerGoroutine; i++ {
				keyIndex := baseIndex + i
				bucketId := uint64(keyIndex % numBuckets)

				key := make([]byte, 8)
				key[0] = byte(keyIndex >> 24)
				key[1] = byte(keyIndex >> 16)
				key[2] = byte(keyIndex >> 8)
				key[3] = byte(keyIndex)
				key[4] = byte(goroutineId)
				key[5] = byte(i >> 8)
				key[6] = byte(i)
				key[7] = byte(bucketId)

				ttl := uint32(minTTL + (keyIndex % (maxTTL - minTTL + 1)))

				metas[keyIndex] = testKeyMeta{
					bucketId:  bucketId,
					key:       append([]byte(nil), key...),
					ds:        1,
					timestamp: baseTimestamp,
				}

				service.RegisterKeyForActiveExpiration(
					bucketId,
					key,
					1,
					ttl,
					baseTimestamp,
				)
			}
		}(g)
	}

	wg.Wait()

	metrics := service.wheelManager.GetMetrics()
	assert.Equal(t, int64(numKeys), metrics.RegisteredKeys, "all keys should be registered")
	assert.Equal(t, int64(0), metrics.TriggeredDeletes, "no deletes should have occurred yet")

	for _, meta := range metas {
		service.wheelManager.onKeyExpired(newKeyMetadata(meta.bucketId, meta.key, meta.ds, meta.timestamp), 0)
	}

	collectEventsWithTimeout(t, calls, numKeys, 10*time.Second)

	finalMetrics := service.wheelManager.GetMetrics()
	assert.Equal(t, int64(0), finalMetrics.RegisteredKeys, "all keys should be deregistered after expiration")
	assert.Equal(t, int64(numKeys), finalMetrics.TriggeredDeletes, "all keys should have triggered deletion")
	assert.Equal(t, int64(numKeys), deletionCount.Load(), "deletion callback should have received all events")
	assert.Equal(t, int64(0), finalMetrics.FailedPushes, "queue should not overflow in deterministic run")
}
