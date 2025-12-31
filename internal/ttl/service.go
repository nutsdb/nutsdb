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
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
)

type BatchExpiredCallback func(events []*ExpirationEvent)

// Service provides TTL checking and batched expiration deletion.
// Expired keys are detected lazily during reads and collected into a queue,
// then batch-deleted by a background goroutine.
type Service struct {
	lifecycle       core.ComponentLifecycle
	checker         *Checker
	clock           Clock
	expiredCallback BatchExpiredCallback
	queue           *expirationQueue
	batchSize       int
	batchTimeout    time.Duration
	wheelManager    *TimingWheelManager // nil if timing wheel is disabled
}

func NewService(clk Clock, config Config, callback BatchExpiredCallback) *Service {
	config.Validate()

	chk := NewChecker(clk)
	queue := newExpirationQueue(config.QueueSize)

	var wheelManager *TimingWheelManager
	if config.EnableTimingWheel {
		wheelManager = NewTimingWheelManager(config, queue)
	}

	service := &Service{
		checker:         chk,
		clock:           clk,
		expiredCallback: callback,
		queue:           queue,
		batchSize:       config.BatchSize,
		batchTimeout:    config.BatchTimeout,
		wheelManager:    wheelManager,
	}

	chk.SetExpiredCallback(func(bucketId uint64, key []byte, ds uint16, timestamp uint64) {
		service.onExpired(bucketId, key, ds, timestamp)
	})

	return service
}

// NowMillis returns the current time in milliseconds via the internal clock.
// This facade method avoids exposing the Clock hierarchy to callers.
func (s *Service) NowMillis() int64 {
	return s.clock.NowMillis()
}

// NowSeconds returns the current time in seconds via the internal clock.
// This facade method avoids exposing the Clock hierarchy to callers.
func (s *Service) NowSeconds() int64 {
	return s.clock.NowSeconds()
}

// IsExpired checks whether a record is expired via the internal checker.
// This facade method avoids exposing the Checker hierarchy to callers.
func (s *Service) IsExpired(ttl uint32, timestamp uint64) bool {
	return s.checker.IsExpired(ttl, timestamp)
}

// GetChecker returns the internal checker.
// Internal use only for internal data structures.
// Deprecated: root package code should use Service.IsExpired instead.
func (s *Service) GetChecker() *Checker {
	return s.checker
}

// SetClock updates the service clock and propagates it to the checker.
// Intended for tests that need deterministic time control.
func (s *Service) SetClock(clk Clock) {
	s.clock = clk
	if s.checker != nil {
		s.checker.SetClock(clk)
	}
}

func (s *Service) onExpired(bucketId uint64, key []byte, ds uint16, timestamp uint64) {
	event := &ExpirationEvent{
		BucketId:  bucketId,
		Key:       key,
		Ds:        ds,
		Timestamp: timestamp,
	}
	s.queue.push(event)
}

func (s *Service) processExpirationEvents(ctx context.Context) {
	batch := make([]*ExpirationEvent, 0, s.batchSize)
	timer := time.NewTimer(s.batchTimeout)
	defer timer.Stop()

	flushBatch := func() {
		if len(batch) > 0 && s.expiredCallback != nil {
			s.expiredCallback(batch)
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flushBatch()
			return

		case event, ok := <-s.queue.events:
			if !ok {
				flushBatch()
				return
			}

			batch = append(batch, event)

			if len(batch) >= s.batchSize {
				flushBatch()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(s.batchTimeout)
			}

		case <-timer.C:
			flushBatch()
			timer.Reset(s.batchTimeout)
		}
	}
}

func (s *Service) Start(ctx context.Context) error {
	if err := s.lifecycle.Start(ctx); err != nil {
		return err
	}

	s.lifecycle.Go(s.processExpirationEvents)

	if s.wheelManager != nil {
		if err := s.wheelManager.Start(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) Stop(timeout time.Duration) error {
	if s.wheelManager != nil {
		if err := s.wheelManager.Stop(timeout); err != nil {
			return err
		}
	}

	s.queue.close()
	return s.lifecycle.Stop(timeout)
}

func (s *Service) Name() string {
	return "TTLService"
}

// RegisterKeyForActiveExpiration registers a key in the timing wheel for active expiration.
// Should be called when a key with TTL is written to the database.
// If the timing wheel is disabled, this is a no-op.
func (s *Service) RegisterKeyForActiveExpiration(
	bucketId uint64,
	key []byte,
	ds uint16,
	ttl uint32,
	timestamp uint64,
) {
	if s.wheelManager != nil {
		s.wheelManager.RegisterKey(bucketId, key, ds, ttl, timestamp)
	}
}

// DeregisterKeyFromActiveExpiration removes a key from the timing wheel.
// Should be called when a key is deleted before expiration or its TTL is updated.
// If the timing wheel is disabled, this is a no-op.
func (s *Service) DeregisterKeyFromActiveExpiration(bucketId uint64, key []byte) {
	if s.wheelManager != nil {
		s.wheelManager.DeregisterKey(bucketId, key)
	}
}
