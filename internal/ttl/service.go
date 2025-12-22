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
)

// BatchExpiredCallback is a function type for handling batch deletion of expired keys.
// It receives a batch of expiration events to process together in a single transaction.
type BatchExpiredCallback func(events []*ExpirationEvent)

// ScanFunc is the type for the scan function that performs expiration scanning.
// It is called by the scanner in each tick and should perform the scan within a transaction.
// This type is defined in ttl package to avoid circular dependencies.
type ScanFunc func() ([]*ExpirationEvent, error)

// Service provides unified TTL management with passive and active expiration.
// 1. Passive: Check TTL when reading a key
// 2. Active: Periodic sampling to find expired keys
//
// Benefits:
// - No timer per key (saves memory)
// - Batch processing (reduces transaction overhead)
// - Adaptive sampling algorithm
type Service struct {
	checker         *Checker             // Passive expiration
	scanner         *Scanner             // Active expiration
	clock           Clock                // Unified clock source
	scanFn          ScanFunc             // Scan function called each tick
	expiredCallback BatchExpiredCallback // Handler for actual batch deletion
	queue           *expirationQueue     // Queue for expiration events
	workerCloseCh   chan struct{}        // Channel to signal worker shutdown
	workerDone      chan struct{}        // Channel to wait for worker completion
	batchSize       int                  // Batch size for processing expiration events
	batchTimeout    time.Duration        // Timeout for processing expiration events
}

// NewService creates a TTL service with the specified clock and configuration.
func NewService(clk Clock, config Config, callback BatchExpiredCallback, scanFn ScanFunc) *Service {
	config.Validate()

	chk := NewChecker(clk)

	scanner := NewScanner(ScannerConfig{
		ScanInterval:     config.ScanInterval,
		SampleSize:       config.SampleSize,
		ExpiredThreshold: config.ExpiredThreshold,
		MaxScanKeys:      config.MaxScanKeys,
	})

	service := &Service{
		checker:         chk,
		scanner:         scanner,
		clock:           clk,
		expiredCallback: callback,
		scanFn:          scanFn,
		queue:           newExpirationQueue(config.QueueSize),
		workerCloseCh:   make(chan struct{}),
		workerDone:      make(chan struct{}),
		batchSize:       config.BatchSize,
		batchTimeout:    config.BatchTimeout,
	}

	// Set up callback routing from checker with adapter
	chk.SetExpiredCallback(func(bucketId uint64, key []byte, ds uint16, timestamp uint64) {
		service.onExpired(bucketId, key, ds, timestamp)
	})

	// Set up callback routing from scanner
	scanner.SetCallback(service.onExpiredBatch)

	// Inject checker into scanner so it can check expirations locally
	scanner.SetChecker(chk)

	return service
}

// Run starts the TTL service with periodic scanning.
// The scanFn is called each scan cycle to perform the scan within a transaction.
func (s *Service) Run() {
	go s.processExpirationEvents()
	go s.scanner.Run(s.scanFn)
}

// Close stops the TTL service and releases all resources.
func (s *Service) Close() {
	s.scanner.Stop()

	// Close queue first to stop accepting new events
	s.queue.close()

	// Signal worker to stop (non-blocking, check if already closed)
	select {
	case <-s.workerCloseCh:
		// Already closed
	default:
		close(s.workerCloseCh)
	}

	// Wait for worker to finish with timeout
	select {
	case <-s.workerDone:
	case <-time.After(time.Second):
		// Timeout, worker may be blocked
	}
}

// GetChecker returns the checker instance for use by data structures.
// Data structures use the checker for passive expiration validation during reads.
func (s *Service) GetChecker() *Checker {
	return s.checker
}

// GetClock returns the unified clock instance.
func (s *Service) GetClock() Clock {
	return s.clock
}

// onExpired is the callback for single expiration events (from passive checking).
// It routes expiration events to the queue for batch processing.
func (s *Service) onExpired(bucketId uint64, key []byte, ds uint16, timestamp uint64) {
	event := &ExpirationEvent{
		BucketId:  bucketId,
		Key:       key,
		Ds:        ds,
		Timestamp: timestamp,
	}
	s.queue.push(event)
}

// onExpiredBatch is the callback for batch expiration events (from scanner).
// It routes all events to the queue for batch processing by calling onExpired for each.
func (s *Service) onExpiredBatch(events []*ExpirationEvent) {
	for _, event := range events {
		s.onExpired(event.BucketId, event.Key, event.Ds, event.Timestamp)
	}
}

// processExpirationEvents processes expiration events in batches for efficient deletion.
func (s *Service) processExpirationEvents() {
	defer close(s.workerDone)

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
		case <-s.workerCloseCh:
			// Worker is shutting down, flush remaining batch
			flushBatch()
			return

		case event, ok := <-s.queue.events:
			if !ok {
				// Queue closed, flush remaining batch
				flushBatch()
				return
			}

			batch = append(batch, event)

			// Flush when batch is full
			if len(batch) >= s.batchSize {
				flushBatch()
				// Reset timer
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
