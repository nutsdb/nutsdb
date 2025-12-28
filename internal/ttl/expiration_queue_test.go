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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExpirationQueue_PushPop(t *testing.T) {
	eq := newExpirationQueue(10)
	defer eq.close()

	event := &ExpirationEvent{
		BucketId:  1,
		Key:       []byte("test-key"),
		Ds:        2,
		Timestamp: 12345,
	}

	require.True(t, eq.push(event))

	select {
	case popped := <-eq.events:
		require.Equal(t, event.BucketId, popped.BucketId)
		require.Equal(t, event.Key, popped.Key)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for event")
	}
}

func TestExpirationQueue_Close(t *testing.T) {
	eq := newExpirationQueue(10)

	event := &ExpirationEvent{
		BucketId:  1,
		Key:       []byte("test-key"),
		Ds:        2,
		Timestamp: 12345,
	}

	eq.push(event)
	eq.close()

	require.False(t, eq.push(event))

	// Remaining events should still be readable
	select {
	case <-eq.events:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("should be able to read remaining events after close")
	}
}

func TestExpirationQueue_DropWhenFull(t *testing.T) {
	eq := newExpirationQueue(1)
	defer eq.close()

	first := &ExpirationEvent{BucketId: 1, Key: []byte("k1"), Ds: 1, Timestamp: 1}
	second := &ExpirationEvent{BucketId: 1, Key: []byte("k2"), Ds: 1, Timestamp: 2}

	require.True(t, eq.push(first))
	require.False(t, eq.push(second))

	<-eq.events

	require.True(t, eq.push(second))
}

func TestExpirationQueue_ConcurrentPush(t *testing.T) {
	eq := newExpirationQueue(100)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			eq.push(&ExpirationEvent{BucketId: 1, Key: []byte("k"), Timestamp: uint64(i)})
		}
	}()

	go func() {
		defer wg.Done()
		for i := 50; i < 100; i++ {
			eq.push(&ExpirationEvent{BucketId: 1, Key: []byte("k"), Timestamp: uint64(i)})
		}
	}()

	wg.Wait()
	eq.close()
}
