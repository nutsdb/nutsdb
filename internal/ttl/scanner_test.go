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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewScanner(t *testing.T) {
	t.Run("with default values", func(t *testing.T) {
		config := ScannerConfig{}
		scanner := NewScanner(config)

		assert.Equal(t, 100*time.Millisecond, scanner.scanInterval)
		assert.Equal(t, 20, scanner.sampleSize)
		assert.Equal(t, 0.25, scanner.expiredThreshold)
		assert.Equal(t, 10000, scanner.maxScanKeys)
	})

	t.Run("with custom values", func(t *testing.T) {
		config := ScannerConfig{
			ScanInterval:     500 * time.Millisecond,
			SampleSize:       50,
			ExpiredThreshold: 0.5,
			MaxScanKeys:      20000,
		}
		scanner := NewScanner(config)

		assert.Equal(t, 500*time.Millisecond, scanner.scanInterval)
		assert.Equal(t, 50, scanner.sampleSize)
		assert.Equal(t, 0.5, scanner.expiredThreshold)
		assert.Equal(t, 20000, scanner.maxScanKeys)
	})

	t.Run("with zero values uses defaults", func(t *testing.T) {
		config := ScannerConfig{
			ScanInterval:     0,
			SampleSize:       0,
			ExpiredThreshold: 0,
			MaxScanKeys:      0,
		}
		scanner := NewScanner(config)

		assert.Equal(t, 100*time.Millisecond, scanner.scanInterval)
		assert.Equal(t, 20, scanner.sampleSize)
		assert.Equal(t, 0.25, scanner.expiredThreshold)
		assert.Equal(t, 10000, scanner.maxScanKeys)
	})

	t.Run("with negative values uses defaults", func(t *testing.T) {
		config := ScannerConfig{
			ScanInterval:     -time.Second,
			SampleSize:       -10,
			ExpiredThreshold: -0.1,
			MaxScanKeys:      -1000,
		}
		scanner := NewScanner(config)

		assert.Equal(t, 100*time.Millisecond, scanner.scanInterval)
		assert.Equal(t, 20, scanner.sampleSize)
		assert.Equal(t, 0.25, scanner.expiredThreshold)
		assert.Equal(t, 10000, scanner.maxScanKeys)
	})
}

func TestScanner_SetCallback(t *testing.T) {
	scanner := NewScanner(ScannerConfig{})

	callback := func(events []*ExpirationEvent) {}

	scanner.SetCallback(callback)

	// Verify callback is set
	scanner.mu.RLock()
	cb := scanner.callback
	scanner.mu.RUnlock()

	assert.NotNil(t, cb)
}

func TestScanner_SetChecker(t *testing.T) {
	scanner := NewScanner(ScannerConfig{})
	mockClock := NewMockClock(1000000)
	checker := NewChecker(mockClock)

	scanner.SetChecker(checker)

	scanner.mu.RLock()
	assert.Equal(t, checker, scanner.checker)
	scanner.mu.RUnlock()
}

func TestScanner_Run_ContextCancel(t *testing.T) {
	scanner := NewScanner(ScannerConfig{
		ScanInterval: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())

	scanFn := func() ([]*ExpirationEvent, error) {
		return nil, nil
	}

	done := make(chan struct{})
	go func() {
		scanner.Run(ctx, scanFn)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Scanner did not exit after context cancellation")
	}
}
