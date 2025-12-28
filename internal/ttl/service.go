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
}

func NewService(clk Clock, config Config, callback BatchExpiredCallback) *Service {
	config.Validate()

	chk := NewChecker(clk)

	service := &Service{
		checker:         chk,
		clock:           clk,
		expiredCallback: callback,
		queue:           newExpirationQueue(config.QueueSize),
		batchSize:       config.BatchSize,
		batchTimeout:    config.BatchTimeout,
	}

	chk.SetExpiredCallback(func(bucketId uint64, key []byte, ds uint16, timestamp uint64) {
		service.onExpired(bucketId, key, ds, timestamp)
	})

	return service
}

func (s *Service) GetChecker() *Checker {
	return s.checker
}

func (s *Service) GetClock() Clock {
	return s.clock
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

	return nil
}

func (s *Service) Stop(timeout time.Duration) error {
	s.queue.close()
	return s.lifecycle.Stop(timeout)
}

func (s *Service) Name() string {
	return "TTLService"
}
