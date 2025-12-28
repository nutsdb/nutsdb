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

	assert.Equal(t, 100, config.BatchSize)
	assert.Equal(t, 1*time.Second, config.BatchTimeout)
	assert.Equal(t, 1000, config.QueueSize)
}

func TestConfig_Validate(t *testing.T) {
	t.Run("with zero values uses defaults", func(t *testing.T) {
		config := Config{}
		config.Validate()

		assert.Equal(t, 100, config.BatchSize)
		assert.Equal(t, 1*time.Second, config.BatchTimeout)
		assert.Equal(t, 1000, config.QueueSize)
	})

	t.Run("with negative values uses defaults", func(t *testing.T) {
		config := Config{
			BatchSize:    -10,
			BatchTimeout: -time.Second,
			QueueSize:    -100,
		}
		config.Validate()

		assert.Equal(t, 100, config.BatchSize)
		assert.Equal(t, 1*time.Second, config.BatchTimeout)
		assert.Equal(t, 1000, config.QueueSize)
	})

	t.Run("with custom values keeps them", func(t *testing.T) {
		config := Config{
			BatchSize:    200,
			BatchTimeout: 2 * time.Second,
			QueueSize:    2000,
		}
		config.Validate()

		assert.Equal(t, 200, config.BatchSize)
		assert.Equal(t, 2*time.Second, config.BatchTimeout)
		assert.Equal(t, 2000, config.QueueSize)
	})
}
