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
	"math/rand"
	"sync"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
)

// ScanCallback is called when expired keys are found during scanning.
// It receives a batch of expiration events to be processed together.
type ScanCallback = BatchExpiredCallback

// RecordSample represents a sampled record for scanning.
type RecordSample struct {
	BucketId uint64
	Record   *core.Record
}

// Scanner periodically scans for expired keys using adaptive sampling.
type Scanner struct {
	scanInterval     time.Duration // Scan interval (default: 100ms, 10 times per second)
	sampleSize       int           // Number of keys to sample each time (default: 20)
	expiredThreshold float64       // Expired rate threshold (default: 0.25)
	maxScanKeys      int           // Maximum keys to scan per cycle (default: 10000)
	stopCh           chan struct{}
	callback         ScanCallback
	checker          *Checker
	rand             *rand.Rand
	mu               sync.RWMutex
	running          bool
}

// ScannerConfig contains configuration for the Scanner.
type ScannerConfig struct {
	ScanInterval     time.Duration // Scan interval (default: 100ms)
	SampleSize       int           // Keys per sample (default: 20)
	ExpiredThreshold float64       // Continue threshold (default: 0.25)
	MaxScanKeys      int           // Max keys per cycle
}

// NewScanner creates a new scanner with the specified configuration.
func NewScanner(config ScannerConfig) *Scanner {
	if config.ScanInterval <= 0 {
		config.ScanInterval = 100 * time.Millisecond
	}
	if config.SampleSize <= 0 {
		config.SampleSize = 20
	}
	if config.ExpiredThreshold <= 0 {
		config.ExpiredThreshold = 0.25
	}
	if config.MaxScanKeys <= 0 {
		config.MaxScanKeys = 10000
	}

	return &Scanner{
		scanInterval:     config.ScanInterval,
		sampleSize:       config.SampleSize,
		expiredThreshold: config.ExpiredThreshold,
		maxScanKeys:      config.MaxScanKeys,
		stopCh:           make(chan struct{}),
		rand:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// SetCallback sets the callback function for expired key notifications.
func (s *Scanner) SetCallback(callback ScanCallback) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.callback = callback
}

// SetChecker sets the checker for expiration verification.
func (s *Scanner) SetChecker(checker *Checker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checker = checker
}

// Run starts the scanner with periodic scanning.
// The getIndex function is called each scan cycle to get current index state.
func (s *Scanner) Run(getIndex func() IndexAccessor) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.stopCh = make(chan struct{})
	s.mu.Unlock()

	ticker := time.NewTicker(s.scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			indexAccessor := getIndex()
			// Ensure IndexAccessor has all required functions
			if indexAccessor.RangeBuckets != nil &&
				indexAccessor.SampleRecords != nil &&
				indexAccessor.GetDataStructure != nil {
				s.scanWithAdaptiveLoop(indexAccessor)
			}
		}
	}
}

// Stop stops the scanner.
func (s *Scanner) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		close(s.stopCh)
		s.running = false
	}
}

// scanWithAdaptiveLoop implements adaptive scanning.
// It continues sampling if the expired rate exceeds the threshold.
func (s *Scanner) scanWithAdaptiveLoop(indexAccessor IndexAccessor) {
	s.mu.RLock()
	callback := s.callback
	checker := s.checker
	s.mu.RUnlock()

	if callback == nil || checker == nil {
		return
	}

	totalScanned := 0

	for {
		// Check scan limit
		if totalScanned >= s.maxScanKeys {
			break
		}

		// Random sample keys from all buckets
		samples := s.randomSampleFromAllBuckets(indexAccessor, s.sampleSize)
		if len(samples) == 0 {
			break
		}

		// Check for expired keys
		expiredCount := 0
		batch := make([]*ExpirationEvent, 0, len(samples))

		for _, sample := range samples {
			record := sample.Record
			// Use local checker for expiration check
			if checker.IsExpired(record.TTL, record.Timestamp) {
				expiredCount++
				batch = append(batch, &ExpirationEvent{
					BucketId:  sample.BucketId,
					Key:       record.Key,
					Ds:        indexAccessor.GetDataStructure(sample.BucketId),
					Timestamp: record.Timestamp,
				})
			}
		}

		// Push expired keys to callback
		if len(batch) > 0 {
			callback(batch)
		}

		totalScanned += len(samples)

		// Calculate expired rate
		expiredRate := float64(expiredCount) / float64(len(samples))

		if expiredRate < s.expiredThreshold {
			break
		}

		// Continue sampling if many keys are expired
	}
}

// randomSampleFromAllBuckets samples random keys from all buckets.
func (s *Scanner) randomSampleFromAllBuckets(indexAccessor IndexAccessor, n int) []RecordSample {
	// Collect keys from all buckets
	allSamples := make([]RecordSample, 0, n*10)

	indexAccessor.RangeBuckets(func(bucketId uint64) bool {
		// Sample records directly
		records, err := indexAccessor.SampleRecords(bucketId, n)
		if err != nil {
			return true // Continue to next bucket
		}

		for _, record := range records {
			allSamples = append(allSamples, RecordSample{
				BucketId: bucketId,
				Record:   record,
			})
		}
		return true
	})

	// Shuffle and pick n samples
	if len(allSamples) <= n {
		return allSamples
	}

	// Fisher-Yates shuffle
	for i := len(allSamples) - 1; i > 0; i-- {
		j := s.rand.Intn(i + 1)
		allSamples[i], allSamples[j] = allSamples[j], allSamples[i]
	}

	return allSamples[:n]
}
