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
)

func TestExpirationQueue_PushPop(t *testing.T) {
	eq := newExpirationQueue(10)
	defer eq.close()

	event := &ExpirationEvent{
		BucketId:  1,
		Key:       []byte("test-key"),
		Ds:        2, // DataStructureBTree
		Timestamp: 12345,
	}

	// Push event
	if !eq.push(event) {
		t.Fatal("Failed to push event")
	}

	// Pop event
	popped, ok := eq.pop()
	if !ok {
		t.Fatal("Failed to pop event")
	}

	if popped.BucketId != event.BucketId {
		t.Errorf("Expected BucketId %d, got %d", event.BucketId, popped.BucketId)
	}
	if string(popped.Key) != string(event.Key) {
		t.Errorf("Expected Key %s, got %s", event.Key, popped.Key)
	}
}

func TestExpirationQueue_Deduplication(t *testing.T) {
	eq := newExpirationQueue(10)
	defer eq.close()

	event := &ExpirationEvent{
		BucketId:  1,
		Key:       []byte("test-key"),
		Ds:        2,
		Timestamp: 12345,
	}

	// Push same event twice
	if !eq.push(event) {
		t.Fatal("Failed to push first event")
	}
	if eq.push(event) {
		t.Fatal("Should not push duplicate event")
	}

	// Should only get one event
	_, ok := eq.pop()
	if !ok {
		t.Fatal("Failed to pop event")
	}

	// Queue should be empty now
	select {
	case <-eq.events:
		t.Fatal("Queue should be empty")
	case <-time.After(10 * time.Millisecond):
		// Expected - queue is empty
	}
}

func TestExpirationQueue_DifferentTimestamps(t *testing.T) {
	eq := newExpirationQueue(10)
	defer eq.close()

	event1 := &ExpirationEvent{
		BucketId:  1,
		Key:       []byte("test-key"),
		Ds:        2,
		Timestamp: 12345,
	}

	event2 := &ExpirationEvent{
		BucketId:  1,
		Key:       []byte("test-key"),
		Ds:        2,
		Timestamp: 67890, // Different timestamp
	}

	// Both should be pushed (different timestamps)
	if !eq.push(event1) {
		t.Fatal("Failed to push first event")
	}
	if !eq.push(event2) {
		t.Fatal("Failed to push second event with different timestamp")
	}

	// Should get both events
	count := 0
	for i := 0; i < 2; i++ {
		select {
		case <-eq.events:
			count++
		case <-time.After(10 * time.Millisecond):
			break
		}
	}

	if count != 2 {
		t.Errorf("Expected 2 events, got %d", count)
	}
}

func TestExpirationQueue_Close(t *testing.T) {
	eq := newExpirationQueue(10)

	event := ExpirationEvent{
		BucketId:  1,
		Key:       []byte("test-key"),
		Ds:        2,
		Timestamp: 12345,
	}

	eq.push(&event)
	eq.close()

	// Should not be able to push after close
	if eq.push(&event) {
		t.Fatal("Should not push to closed queue")
	}

	// Should still be able to pop remaining events
	_, ok := eq.pop()
	if !ok {
		t.Fatal("Should be able to pop remaining events after close")
	}
}
