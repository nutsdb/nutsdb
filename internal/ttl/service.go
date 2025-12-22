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
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/ttl/checker"
	"github.com/nutsdb/nutsdb/internal/ttl/clock"
)

// ExpiredCallback is a function type for handling the actual deletion of expired keys.
// It's called by the background worker to perform the deletion.
type ExpiredCallback func(event ExpirationEvent)

// Service provides a unified TTL management facade that coordinates both
// active expiration (timer-based) and passive expiration (check-based).
type Service struct {
	manager         Manager          // Active expiration using timers
	checker         *checker.Checker // Passive expiration checking
	clock           clock.Clock      // Unified clock source
	expiredCallback ExpiredCallback  // Handler for actual deletion
	queue           *expirationQueue // Queue for expiration events
	workerCloseCh   chan struct{}    // Channel to signal worker shutdown
	workerDone      chan struct{}    // Channel to wait for worker completion
}

// NewService creates a new TTL service with the specified clock and expiration deletion type.
// The service manages both active (timer-based) and passive (check-based) expiration strategies.
// If a MockClock is provided, a MockManager will be used for testing purposes.
func NewService(clk clock.Clock, expiredDeleteType ExpiredDeleteType) *Service {
	chk := checker.NewChecker(clk)

	var mgr Manager
	if mc, ok := clk.(*clock.MockClock); ok {
		mgr = NewMockManager(mc)
	} else {
		mgr = NewTimerManager(expiredDeleteType)
	}

	service := &Service{
		manager:       mgr,
		checker:       chk,
		clock:         clk,
		queue:         newExpirationQueue(1000),
		workerCloseCh: make(chan struct{}),
		workerDone:    make(chan struct{}),
	}

	// Set up unified callback routing
	chk.SetExpiredCallback(service.onExpired)

	return service
}

// Run starts the TTL service, initiating active expiration monitoring and background worker.
func (s *Service) Run() {
	go s.processExpirationEvents()
	s.manager.Run()
}

// Close stops the TTL service and releases all resources.
func (s *Service) Close() {
	s.manager.Close()

	// Close queue first to stop accepting new events
	s.queue.close()

	// Signal worker to stop (non-blocking, check if already closed)
	select {
	case <-s.workerCloseCh:
		// Already closed
	default:
		close(s.workerCloseCh)
	}

	// Don't wait for worker to finish - it may be blocked on DB operations
	// Remaining events will be filtered on next startup
}

// SetExpiredCallback sets the handler function for deleting expired keys.
// This handler is called by the background worker to perform actual deletion.
func (s *Service) SetExpiredCallback(callback ExpiredCallback) {
	s.expiredCallback = callback
}

// GetChecker returns the checker instance for use by data structures.
// Data structures use the checker for passive expiration validation during reads.
func (s *Service) GetChecker() *checker.Checker {
	return s.checker
}

// GetClock returns the unified clock instance.
func (s *Service) GetClock() clock.Clock {
	return s.clock
}

// RegisterTTL registers a key for active expiration monitoring.
// When the TTL expires, the callback will be triggered with the provided parameters.
func (s *Service) RegisterTTL(bucketId core.BucketId, key string, expire time.Duration, ds uint16, timestamp uint64) {
	s.manager.Add(bucketId, key, expire, ds, timestamp, s.onExpired)
}

// UnregisterTTL removes a key from active expiration monitoring.
func (s *Service) UnregisterTTL(bucketId core.BucketId, key string) {
	s.manager.Del(bucketId, key)
}

// ExistTTL checks if a key is currently registered for active expiration.
func (s *Service) ExistTTL(bucketId core.BucketId, key string) bool {
	return s.manager.Exist(bucketId, key)
}

// onExpired is the internal unified callback handler for expired keys.
// It routes expiration events from both manager and checker to the event queue.
func (s *Service) onExpired(bucketId core.BucketId, key []byte, ds uint16, timestamp uint64) {
	event := ExpirationEvent{
		BucketId:  bucketId,
		Key:       key,
		Ds:        ds,
		Timestamp: timestamp,
	}
	s.queue.push(event)
}

// processExpirationEvents is a background worker that processes expiration events.
// It runs in a separate goroutine and handles async deletion with proper verification.
func (s *Service) processExpirationEvents() {
	defer close(s.workerDone)

	for {
		select {
		case <-s.workerCloseCh:
			// Worker is shutting down, exit immediately
			// Remaining events will be filtered on next startup
			return
		case event, ok := <-s.queue.events:
			if !ok {
				// Queue closed
				return
			}
			if s.expiredCallback != nil {
				s.expiredCallback(event)
			}
		}
	}
}
