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

import "time"

// Config contains all TTL-related configuration.
type Config struct {
	// Batch processing
	BatchSize    int           // Batch size for deletion (default: 100)
	BatchTimeout time.Duration // Batch timeout (default: 100ms)
	QueueSize    int           // Event queue size (default: 1000)

	// Scanner configuration
	ScanInterval     time.Duration // Scan interval (default: 100ms)
	SampleSize       int           // Keys per sample (default: 20)
	ExpiredThreshold float64       // Continue threshold (default: 0.25)
	MaxScanKeys      int           // Max keys per scan cycle (default: 10000)
}

// DefaultConfig returns the default TTL configuration.
func DefaultConfig() Config {
	return Config{
		// Batch processing
		BatchSize:    100,
		BatchTimeout: 1000 * time.Millisecond,
		QueueSize:    1000,

		ScanInterval:     1000 * time.Millisecond,
		SampleSize:       20,
		ExpiredThreshold: 0.25,
		MaxScanKeys:      10000,
	}
}

// Validate validates the configuration and fills in defaults for zero values.
func (c *Config) Validate() {
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.BatchTimeout <= 0 {
		c.BatchTimeout = 100 * time.Millisecond
	}
	if c.QueueSize <= 0 {
		c.QueueSize = 1000
	}
	if c.ScanInterval <= 0 {
		c.ScanInterval = 100 * time.Millisecond
	}
	if c.SampleSize <= 0 {
		c.SampleSize = 20
	}
	if c.ExpiredThreshold <= 0 {
		c.ExpiredThreshold = 0.25
	}
	if c.MaxScanKeys <= 0 {
		c.MaxScanKeys = 10000
	}
}
