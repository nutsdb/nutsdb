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
)

// ExpirationEvent represents a key expiration event.
type ExpirationEvent struct {
	BucketId  uint64
	Key       []byte
	Ds        uint16
	Timestamp uint64
}

// dedupeKey is used as the key for the seen map to avoid string allocation.
type dedupeKey struct {
	bucketId  uint64
	key       string
	timestamp uint64
}

// expirationQueue manages a queue of expiration events with deduplication.
// It prevents duplicate processing of the same key expiration.
type expirationQueue struct {
	events chan *ExpirationEvent
	seen   map[dedupeKey]struct{}
	closed bool
	mu     sync.Mutex
}

// newExpirationQueue creates a new expiration queue with the specified buffer size.
func newExpirationQueue(bufferSize int) *expirationQueue {
	return &expirationQueue{
		events: make(chan *ExpirationEvent, bufferSize),
		seen:   make(map[dedupeKey]struct{}),
		closed: false,
	}
}

// push adds an expiration event to the queue with deduplication.
// Returns true if the event was added, false if it was a duplicate or queue is closed.
func (eq *expirationQueue) push(event *ExpirationEvent) bool {
	eq.mu.Lock()
	if eq.closed {
		eq.mu.Unlock()
		return false
	}

	key := dedupeKey{
		bucketId:  event.BucketId,
		key:       string(event.Key),
		timestamp: event.Timestamp,
	}

	if _, exists := eq.seen[key]; exists {
		eq.mu.Unlock()
		return false // Duplicate event, skip
	}

	// Optimistically add to map
	eq.seen[key] = struct{}{}
	eq.mu.Unlock()

	// Try to send to channel (non-blocking)
	select {
	case eq.events <- event:
		return true
	default:
		// Channel full, remove from seen map and drop event
		eq.mu.Lock()
		delete(eq.seen, key)
		eq.mu.Unlock()
		return false
	}
}

// pop retrieves the next expiration event from the queue.
// Returns the event and true if successful, or zero value and false if queue is closed.
func (eq *expirationQueue) pop() (*ExpirationEvent, bool) {
	event, ok := <-eq.events
	if !ok {
		return &ExpirationEvent{}, false
	}

	// Remove from seen map after processing
	key := dedupeKey{
		bucketId:  event.BucketId,
		key:       string(event.Key),
		timestamp: event.Timestamp,
	}

	eq.mu.Lock()
	delete(eq.seen, key)
	eq.mu.Unlock()

	return event, true
}

// close closes the expiration queue and releases resources.
func (eq *expirationQueue) close() {
	eq.mu.Lock()
	defer eq.mu.Unlock()

	if !eq.closed {
		eq.closed = true
		close(eq.events)
	}
}
