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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewService(t *testing.T) {
	mockClock := NewMockClock(1000000)

	callback := func(events []*ExpirationEvent) {}

	scanFn := func() ([]*ExpirationEvent, error) {
		return nil, nil
	}

	config := DefaultConfig()
	service := NewService(mockClock, config, callback, scanFn)

	assert.NotNil(t, service)
	assert.NotNil(t, service.GetChecker())
	assert.Equal(t, mockClock, service.GetClock())

	assert.NotNil(t, service.scanner)
	assert.NotNil(t, service.checker)
}

func TestService_GetChecker(t *testing.T) {
	mockClock := NewMockClock(1000000)
	config := DefaultConfig()
	service := NewService(mockClock, config, nil, nil)

	checker := service.GetChecker()
	assert.NotNil(t, checker)
	assert.IsType(t, &Checker{}, checker)
}

func TestService_GetClock(t *testing.T) {
	mockClock := NewMockClock(1000000)
	config := DefaultConfig()
	service := NewService(mockClock, config, nil, nil)

	clock := service.GetClock()
	assert.Equal(t, mockClock, clock)
}

func TestService_onExpired(t *testing.T) {
	mockClock := NewMockClock(1000000)
	config := DefaultConfig()

	eventsReceived := make([]*ExpirationEvent, 0)
	var mu sync.Mutex

	callback := func(events []*ExpirationEvent) {
		mu.Lock()
		eventsReceived = append(eventsReceived, events...)
		mu.Unlock()
	}

	service := NewService(mockClock, config, callback, nil)

	event := &ExpirationEvent{
		BucketId:  1,
		Key:       []byte("test-key"),
		Ds:        2,
		Timestamp: 1000000,
	}
	service.onExpired(event.BucketId, event.Key, event.Ds, event.Timestamp)

	popped, ok := service.queue.pop()
	require.True(t, ok)
	assert.Equal(t, event.BucketId, popped.BucketId)
	assert.Equal(t, event.Key, popped.Key)
	assert.Equal(t, event.Ds, popped.Ds)
}

func TestService_onExpiredBatch(t *testing.T) {
	mockClock := NewMockClock(1000000)
	config := DefaultConfig()

	eventsReceived := make([]*ExpirationEvent, 0)
	var mu sync.Mutex

	callback := func(events []*ExpirationEvent) {
		mu.Lock()
		eventsReceived = append(eventsReceived, events...)
		mu.Unlock()
	}

	service := NewService(mockClock, config, callback, nil)

	events := []*ExpirationEvent{
		{BucketId: 1, Key: []byte("key1"), Ds: 2, Timestamp: 1000000},
		{BucketId: 1, Key: []byte("key2"), Ds: 2, Timestamp: 1000000},
		{BucketId: 2, Key: []byte("key3"), Ds: 3, Timestamp: 1000000},
	}

	service.onExpiredBatch(events)

	for _, expected := range events {
		popped, ok := service.queue.pop()
		require.True(t, ok)
		assert.Equal(t, expected.BucketId, popped.BucketId)
		assert.Equal(t, expected.Key, popped.Key)
	}
}

func TestService_StartAndStop(t *testing.T) {
	mockClock := NewMockClock(1000000)

	scanCallCount := 0
	scanFn := func() ([]*ExpirationEvent, error) {
		scanCallCount++
		if scanCallCount >= 2 {
			return []*ExpirationEvent{
				{BucketId: 1, Key: []byte("key1"), Ds: 2, Timestamp: 1000000},
			}, nil
		}
		return nil, nil
	}

	eventsReceived := make([]*ExpirationEvent, 0)
	var mu sync.Mutex
	callback := func(events []*ExpirationEvent) {
		mu.Lock()
		eventsReceived = append(eventsReceived, events...)
		mu.Unlock()
	}

	config := DefaultConfig()
	config.ScanInterval = 10 * time.Millisecond
	config.BatchTimeout = 50 * time.Millisecond
	config.BatchSize = 10

	service := NewService(mockClock, config, callback, scanFn)

	require.NoError(t, service.Start(context.Background()))

	time.Sleep(50 * time.Millisecond)

	require.NoError(t, service.Stop(500*time.Millisecond))

	assert.True(t, service.queue.closed)
}

func TestService_StopCancelsScanner(t *testing.T) {
	mockClock := NewMockClock(1000000)

	var scanCalls atomic.Int64
	scanFn := func() ([]*ExpirationEvent, error) {
		scanCalls.Add(1)
		return nil, nil
	}

	config := DefaultConfig()
	config.ScanInterval = 5 * time.Millisecond
	config.BatchTimeout = 20 * time.Millisecond

	service := NewService(mockClock, config, nil, scanFn)

	require.NoError(t, service.Start(context.Background()))

	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	err := service.Stop(500 * time.Millisecond)
	elapsed := time.Since(start)
	require.NoError(t, err)

	if elapsed > 500*time.Millisecond {
		t.Fatalf("expected Stop to return quickly, took %v", elapsed)
	}

	afterStop := scanCalls.Load()
	time.Sleep(15 * time.Millisecond)
	require.Equal(t, afterStop, scanCalls.Load(), "scanner should be stopped after Stop")
}

func TestService_ProcessExpirationEvents_EmptyBatch(t *testing.T) {
	mockClock := NewMockClock(1000000)
	config := DefaultConfig()

	callback := func(events []*ExpirationEvent) {}

	scanFn := func() ([]*ExpirationEvent, error) {
		return []*ExpirationEvent{}, nil
	}

	service := NewService(mockClock, config, callback, scanFn)

	require.NoError(t, service.Start(context.Background()))

	time.Sleep(10 * time.Millisecond)

	require.NoError(t, service.Stop(100*time.Millisecond))
}

func TestService_ProcessExpirationEvents_FullBatchFlush(t *testing.T) {
	mockClock := NewMockClock(1000000)
	config := DefaultConfig()
	config.BatchSize = 3
	config.BatchTimeout = time.Second

	eventsReceived := make([]*ExpirationEvent, 0)
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	callback := func(events []*ExpirationEvent) {
		mu.Lock()
		eventsReceived = append(eventsReceived, events...)
		cond.Signal()
		mu.Unlock()
	}

	service := NewService(mockClock, config, callback, nil)

	require.NoError(t, service.Start(context.Background()))

	for i := 0; i < 3; i++ {
		event := &ExpirationEvent{
			BucketId:  1,
			Key:       []byte(fmt.Sprintf("key_%d", i)),
			Ds:        2,
			Timestamp: 1000000 + uint64(i),
		}
		service.queue.push(event)
	}

	mu.Lock()
	for len(eventsReceived) == 0 {
		cond.Wait()
	}
	mu.Unlock()

	assert.Equal(t, 3, len(eventsReceived))

	require.NoError(t, service.Stop(100*time.Millisecond))
}

func TestService_ProcessExpirationEvents_TimerFlush(t *testing.T) {
	mockClock := NewMockClock(1000000)
	config := DefaultConfig()
	config.BatchSize = 10                       // Large batch size so we don't auto-flush
	config.BatchTimeout = 50 * time.Millisecond // Short timeout

	eventsReceived := make([]*ExpirationEvent, 0)
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	callback := func(events []*ExpirationEvent) {
		mu.Lock()
		eventsReceived = append(eventsReceived, events...)
		cond.Signal()
		mu.Unlock()
	}

	service := NewService(mockClock, config, callback, nil)

	require.NoError(t, service.Start(context.Background()))

	event := &ExpirationEvent{
		BucketId:  1,
		Key:       []byte("key"),
		Ds:        2,
		Timestamp: 1000000,
	}
	service.queue.push(event)

	mu.Lock()
	for len(eventsReceived) == 0 {
		cond.Wait()
	}
	mu.Unlock()

	assert.GreaterOrEqual(t, len(eventsReceived), 1)

	require.NoError(t, service.Stop(100*time.Millisecond))
}
