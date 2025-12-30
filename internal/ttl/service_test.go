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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewService(t *testing.T) {
	mockClock := NewMockClock(1000000)
	callback := func(events []*ExpirationEvent) {}
	config := DefaultConfig()

	service := NewService(mockClock, config, callback)

	assert.NotNil(t, service)
	assert.NotNil(t, service.checker)
	assert.Equal(t, mockClock, service.clock)
}

func TestService_NowMillisAndNowSeconds_WithMockClock(t *testing.T) {
	mockClock := NewMockClock(1234567890)
	config := DefaultConfig()
	service := NewService(mockClock, config, nil)

	assert.Equal(t, mockClock.NowMillis(), service.NowMillis())
	assert.Equal(t, mockClock.NowSeconds(), service.NowSeconds())
}

func TestService_IsExpired_DelegatesToChecker(t *testing.T) {
	mockClock := NewMockClock(2000)
	config := DefaultConfig()
	service := NewService(mockClock, config, nil)

	assert.Equal(t, service.checker.IsExpired(1, 500), service.IsExpired(1, 500))
}

func TestService_onExpired(t *testing.T) {
	mockClock := NewMockClock(1000000)
	config := DefaultConfig()

	service := NewService(mockClock, config, nil)

	event := &ExpirationEvent{
		BucketId:  1,
		Key:       []byte("test-key"),
		Ds:        2,
		Timestamp: 1000000,
	}
	service.onExpired(event.BucketId, event.Key, event.Ds, event.Timestamp)

	select {
	case popped := <-service.queue.events:
		assert.Equal(t, event.BucketId, popped.BucketId)
		assert.Equal(t, event.Key, popped.Key)
		assert.Equal(t, event.Ds, popped.Ds)
	default:
		t.Fatal("expected event in queue")
	}
}

func TestService_StartAndStop(t *testing.T) {
	mockClock := NewMockClock(1000000)

	eventsReceived := make([]*ExpirationEvent, 0)
	var mu sync.Mutex
	callback := func(events []*ExpirationEvent) {
		mu.Lock()
		eventsReceived = append(eventsReceived, events...)
		mu.Unlock()
	}

	config := DefaultConfig()
	config.BatchTimeout = 50 * time.Millisecond
	config.BatchSize = 10

	service := NewService(mockClock, config, callback)

	require.NoError(t, service.Start(context.Background()))
	require.NoError(t, service.Stop(500*time.Millisecond))

	assert.True(t, service.queue.closed.Load())
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

	service := NewService(mockClock, config, callback)

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

func TestService_ProcessExpirationEvents_FlushOnStop(t *testing.T) {
	mockClock := NewMockClock(1000000)
	config := DefaultConfig()
	config.BatchSize = 10
	config.BatchTimeout = time.Hour

	eventsReceived := make([]*ExpirationEvent, 0)
	var mu sync.Mutex
	received := make(chan struct{}, 1)
	callback := func(events []*ExpirationEvent) {
		mu.Lock()
		eventsReceived = append(eventsReceived, events...)
		mu.Unlock()
		select {
		case received <- struct{}{}:
		default:
		}
	}

	service := NewService(mockClock, config, callback)

	require.NoError(t, service.Start(context.Background()))

	event := &ExpirationEvent{
		BucketId:  1,
		Key:       []byte("key"),
		Ds:        2,
		Timestamp: 1000000,
	}
	service.queue.push(event)

	service.queue.close()
	require.NoError(t, service.Stop(100*time.Millisecond))

	select {
	case <-received:
	case <-time.After(1 * time.Second):
		t.Fatal("expected expiration events after stop")
	}

	mu.Lock()
	assert.GreaterOrEqual(t, len(eventsReceived), 1)
	mu.Unlock()
}
