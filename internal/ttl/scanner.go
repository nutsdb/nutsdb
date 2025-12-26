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
	"math/rand"
	"sync"
	"time"
)

// ScanCallback is called when expired keys are found during scanning.
// It receives a batch of expiration events to be processed together.
type ScanCallback = BatchExpiredCallback

// Scanner periodically scans for expired keys using adaptive sampling.
type Scanner struct {
	scanInterval     time.Duration // Scan interval (default: 100ms, 10 times per second)
	sampleSize       int           // Number of keys to sample each time (default: 20)
	expiredThreshold float64       // Expired rate threshold (default: 0.25)
	maxScanKeys      int           // Maximum keys to scan per cycle (default: 10000)
	callback         ScanCallback
	checker          *Checker
	rand             *rand.Rand
	mu               sync.RWMutex
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
// The scanFn is called each scan cycle to perform the scan within a transaction.
func (s *Scanner) Run(ctx context.Context, scanFn ScanFunc) {
	ticker := time.NewTicker(s.scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events, err := scanFn()
			if err != nil {
				// Log error but continue scanning
				continue
			}
			if len(events) > 0 {
				s.mu.RLock()
				callback := s.callback
				s.mu.RUnlock()
				if callback != nil {
					callback(events)
				}
			}
		}
	}
}
