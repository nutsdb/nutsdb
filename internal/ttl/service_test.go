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
	"fmt"
	"sync"
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

	// Verify scanner and checker are linked
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

	// Simulate onExpired being called directly
	event := &ExpirationEvent{
		BucketId:  1,
		Key:       []byte("test-key"),
		Ds:        2,
		Timestamp: 1000000,
	}
	service.onExpired(event.BucketId, event.Key, event.Ds, event.Timestamp)

	// Pop the event from the queue
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

	// Create batch events
	events := []*ExpirationEvent{
		{BucketId: 1, Key: []byte("key1"), Ds: 2, Timestamp: 1000000},
		{BucketId: 1, Key: []byte("key2"), Ds: 2, Timestamp: 1000000},
		{BucketId: 2, Key: []byte("key3"), Ds: 3, Timestamp: 1000000},
	}

	service.onExpiredBatch(events)

	// All events should be in the queue
	for _, expected := range events {
		popped, ok := service.queue.pop()
		require.True(t, ok)
		assert.Equal(t, expected.BucketId, popped.BucketId)
		assert.Equal(t, expected.Key, popped.Key)
	}
}

func TestService_RunAndClose(t *testing.T) {
	mockClock := NewMockClock(1000000)

	scanCallCount := 0
	scanFn := func() ([]*ExpirationEvent, error) {
		scanCallCount++
		if scanCallCount >= 2 {
			// Return events to trigger callback
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

	// Run the service
	service.Run()

	// Wait for scanner to run at least one cycle
	time.Sleep(50 * time.Millisecond)

	// Close the service
	service.Close()

	// Verify the service closed (queue is closed)
	assert.True(t, service.queue.closed)
}

func TestService_Close_Idempotent(t *testing.T) {
	mockClock := NewMockClock(1000000)
	config := DefaultConfig()
	service := NewService(mockClock, config, nil, nil)

	// Close multiple times should not panic
	service.Close()
	service.Close()
	service.Close()
}

func TestService_ProcessExpirationEvents_EmptyBatch(t *testing.T) {
	mockClock := NewMockClock(1000000)
	config := DefaultConfig()

	callback := func(events []*ExpirationEvent) {}

	// scanFn that returns empty
	scanFn := func() ([]*ExpirationEvent, error) {
		return []*ExpirationEvent{}, nil
	}

	service := NewService(mockClock, config, callback, scanFn)

	// Run in a goroutine
	go service.processExpirationEvents()

	// Close queue to trigger exit
	service.queue.close()

	// Wait a bit
	time.Sleep(10 * time.Millisecond)

	// The worker should have exited
	service.Close()
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

	// Run the worker
	go service.processExpirationEvents()

	// Push events to fill the batch (use unique keys to avoid deduplication)
	for i := 0; i < 3; i++ {
		event := &ExpirationEvent{
			BucketId:  1,
			Key:       []byte(fmt.Sprintf("key_%d", i)), // Unique key
			Ds:        2,
			Timestamp: 1000000 + uint64(i), // Unique timestamp
		}
		service.queue.push(event)
	}

	// Wait for callback to be called
	mu.Lock()
	for len(eventsReceived) == 0 {
		cond.Wait()
	}
	mu.Unlock()

	// Verify we received all 3 events (either in one batch or multiple)
	assert.Equal(t, 3, len(eventsReceived))

	service.Close()
}

func TestService_ProcessExpirationEvents_WorkerCloseSignal(t *testing.T) {
	mockClock := NewMockClock(1000000)
	config := DefaultConfig()

	callback := func(events []*ExpirationEvent) {}

	service := NewService(mockClock, config, callback, nil)

	// Run the worker
	go service.processExpirationEvents()

	// Close the workerCloseCh
	close(service.workerCloseCh)

	// Wait for worker to exit
	time.Sleep(10 * time.Millisecond)

	service.Close()
}

func TestService_ProcessExpirationEvents_QueueClose(t *testing.T) {
	mockClock := NewMockClock(1000000)
	config := DefaultConfig()

	service := NewService(mockClock, config, nil, nil)

	// Run the worker
	go service.processExpirationEvents()

	// Push an event
	event := &ExpirationEvent{
		BucketId:  1,
		Key:       []byte("key"),
		Ds:        2,
		Timestamp: 1000000,
	}
	service.queue.push(event)

	// Close the queue
	service.queue.close()

	// Wait for worker to process the close
	time.Sleep(10 * time.Millisecond)

	service.Close()
}

func TestService_ProcessExpirationEvents_TimerFlush(t *testing.T) {
	mockClock := NewMockClock(1000000)
	config := DefaultConfig()
	config.BatchSize = 10          // Large batch size so we don't auto-flush
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

	// Run the worker
	go service.processExpirationEvents()

	// Push one event (less than batch size)
	event := &ExpirationEvent{
		BucketId:  1,
		Key:       []byte("key"),
		Ds:        2,
		Timestamp: 1000000,
	}
	service.queue.push(event)

	// Wait for callback
	mu.Lock()
	for len(eventsReceived) == 0 {
		cond.Wait()
	}
	mu.Unlock()

	// Verify we received the event
	assert.GreaterOrEqual(t, len(eventsReceived), 1)

	service.Close()
}
