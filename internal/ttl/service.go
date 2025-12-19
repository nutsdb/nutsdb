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

// ExpiredCallback is a function type for handling expired record notifications.
// timestamp is the record timestamp when expiration was detected, used for validation.
type ExpiredCallback func(bucketId uint64, key []byte, ds uint16, timestamp uint64)

// Service provides a unified TTL management facade that coordinates both
// active expiration (timer-based) and passive expiration (check-based).
type Service struct {
	manager  Manager          // Active expiration using timers
	checker  *checker.Checker // Passive expiration checking
	clock    clock.Clock      // Unified clock source
	callback ExpiredCallback  // Callback for expired keys
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
		manager: mgr,
		checker: chk,
		clock:   clk,
	}

	// Set up unified callback routing
	chk.SetExpiredCallback(service.onExpired)

	return service
}

// Run starts the TTL service, initiating active expiration monitoring.
func (s *Service) Run() {
	s.manager.Run()
}

// Close stops the TTL service and releases all resources.
func (s *Service) Close() {
	s.manager.Close()
}

// SetExpiredCallback sets the callback function to be invoked when keys expire.
// This callback is triggered by both active (timer) and passive (check) expiration.
func (s *Service) SetExpiredCallback(callback ExpiredCallback) {
	s.callback = callback
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
// It routes expiration events from both manager and checker to the user-defined callback.
func (s *Service) onExpired(bucketId core.BucketId, key []byte, ds uint16, timestamp uint64) {
	if s.callback != nil {
		s.callback(bucketId, key, ds, timestamp)
	}
}
