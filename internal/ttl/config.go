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

// Config contains TTL-related configuration for batch deletion.
type Config struct {
	BatchSize    int           // Batch size for deletion (default: 100)
	BatchTimeout time.Duration // Batch timeout (default: 1s)
	QueueSize    int           // Event queue size (default: 1000)
}

// DefaultConfig returns the default TTL configuration.
func DefaultConfig() Config {
	return Config{
		BatchSize:    100,
		BatchTimeout: 1 * time.Second,
		QueueSize:    1000,
	}
}

// Validate validates the configuration and fills in defaults for zero values.
func (c *Config) Validate() {
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.BatchTimeout <= 0 {
		c.BatchTimeout = 1 * time.Second
	}
	if c.QueueSize <= 0 {
		c.QueueSize = 1000
	}
}
