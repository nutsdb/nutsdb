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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	// Batch processing defaults
	assert.Equal(t, 100, config.BatchSize)
	assert.Equal(t, 1000*time.Millisecond, config.BatchTimeout)
	assert.Equal(t, 1000, config.QueueSize)

	// Scanner configuration defaults
	assert.Equal(t, 1000*time.Millisecond, config.ScanInterval)
	assert.Equal(t, 20, config.SampleSize)
	assert.Equal(t, 0.25, config.ExpiredThreshold)
	assert.Equal(t, 10000, config.MaxScanKeys)
}

func TestConfig_Validate(t *testing.T) {
	t.Run("validate with zero values sets defaults", func(t *testing.T) {
		config := Config{}
		config.Validate()

		assert.Equal(t, 100, config.BatchSize)
		assert.Equal(t, 100*time.Millisecond, config.BatchTimeout)
		assert.Equal(t, 1000, config.QueueSize)
		assert.Equal(t, 100*time.Millisecond, config.ScanInterval)
		assert.Equal(t, 20, config.SampleSize)
		assert.Equal(t, 0.25, config.ExpiredThreshold)
		assert.Equal(t, 10000, config.MaxScanKeys)
	})

	t.Run("validate with negative values sets defaults", func(t *testing.T) {
		config := Config{
			BatchSize:         -1,
			BatchTimeout:      -time.Second,
			QueueSize:         -100,
			ScanInterval:      -time.Millisecond,
			SampleSize:        -5,
			ExpiredThreshold:  -0.1,
			MaxScanKeys:       -1000,
		}
		config.Validate()

		assert.Equal(t, 100, config.BatchSize)
		assert.Equal(t, 100*time.Millisecond, config.BatchTimeout)
		assert.Equal(t, 1000, config.QueueSize)
		assert.Equal(t, 100*time.Millisecond, config.ScanInterval)
		assert.Equal(t, 20, config.SampleSize)
		assert.Equal(t, 0.25, config.ExpiredThreshold)
		assert.Equal(t, 10000, config.MaxScanKeys)
	})

	t.Run("validate with partial zero values", func(t *testing.T) {
		config := Config{
			BatchSize:    200,
			BatchTimeout: 500 * time.Millisecond,
			QueueSize:    0, // Should get default
		}
		config.Validate()

		assert.Equal(t, 200, config.BatchSize)
		assert.Equal(t, 500*time.Millisecond, config.BatchTimeout)
		assert.Equal(t, 1000, config.QueueSize) // Default
	})

	t.Run("validate with valid values preserves them", func(t *testing.T) {
		config := Config{
			BatchSize:         500,
			BatchTimeout:      2 * time.Second,
			QueueSize:         2000,
			ScanInterval:      500 * time.Millisecond,
			SampleSize:        50,
			ExpiredThreshold:  0.5,
			MaxScanKeys:       50000,
		}
		config.Validate()

		assert.Equal(t, 500, config.BatchSize)
		assert.Equal(t, 2*time.Second, config.BatchTimeout)
		assert.Equal(t, 2000, config.QueueSize)
		assert.Equal(t, 500*time.Millisecond, config.ScanInterval)
		assert.Equal(t, 50, config.SampleSize)
		assert.Equal(t, 0.5, config.ExpiredThreshold)
		assert.Equal(t, 50000, config.MaxScanKeys)
	})
}
