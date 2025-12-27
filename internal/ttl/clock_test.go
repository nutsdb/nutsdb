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
	"sync"
	"testing"
	"time"
)

func TestRealClock(t *testing.T) {
	clock := NewRealClock()

	// Test that NowMillis returns a reasonable time
	nowMillis := clock.NowMillis()
	if nowMillis <= 0 {
		t.Errorf("Expected positive timestamp, got %d", nowMillis)
	}

	// Test that NowSeconds returns a reasonable time
	nowSeconds := clock.NowSeconds()
	if nowSeconds <= 0 {
		t.Errorf("Expected positive timestamp, got %d", nowSeconds)
	}

	// Test that milliseconds and seconds are consistent
	expectedSeconds := nowMillis / 1000
	if abs(nowSeconds-expectedSeconds) > 1 {
		t.Errorf("Milliseconds and seconds not consistent: %d ms -> %d s, expected ~%d s",
			nowMillis, nowSeconds, expectedSeconds)
	}
}

func TestMockClock(t *testing.T) {
	initialTime := int64(1640995200000) // 2022-01-01 00:00:00 UTC in milliseconds
	clock := NewMockClock(initialTime)

	// Test initial time
	if clock.NowMillis() != initialTime {
		t.Errorf("Expected %d, got %d", initialTime, clock.NowMillis())
	}

	expectedSeconds := initialTime / 1000
	if clock.NowSeconds() != expectedSeconds {
		t.Errorf("Expected %d seconds, got %d", expectedSeconds, clock.NowSeconds())
	}
}

func TestMockClockAdvanceTime(t *testing.T) {
	initialTime := int64(1640995200000) // 2022-01-01 00:00:00 UTC in milliseconds
	clock := NewMockClock(initialTime)

	// Advance by 1 hour
	duration := time.Hour
	clock.AdvanceTime(duration)

	expectedTime := initialTime + duration.Milliseconds()
	if clock.NowMillis() != expectedTime {
		t.Errorf("Expected %d, got %d", expectedTime, clock.NowMillis())
	}

	expectedSeconds := expectedTime / 1000
	if clock.NowSeconds() != expectedSeconds {
		t.Errorf("Expected %d seconds, got %d", expectedSeconds, clock.NowSeconds())
	}
}

func TestMockClockSetTime(t *testing.T) {
	initialTime := int64(1640995200000) // 2022-01-01 00:00:00 UTC in milliseconds
	clock := NewMockClock(initialTime)

	// Set to a different time
	newTime := int64(1672531200000) // 2023-01-01 00:00:00 UTC in milliseconds
	clock.SetTime(newTime)

	if clock.NowMillis() != newTime {
		t.Errorf("Expected %d, got %d", newTime, clock.NowMillis())
	}

	expectedSeconds := newTime / 1000
	if clock.NowSeconds() != expectedSeconds {
		t.Errorf("Expected %d seconds, got %d", expectedSeconds, clock.NowSeconds())
	}
}

func TestMockClockConcurrentSafety(t *testing.T) {
	initialTime := int64(1640995200000) // 2022-01-01 00:00:00 UTC in milliseconds
	clock := NewMockClock(initialTime)

	var wg sync.WaitGroup
	numGoroutines := 10
	numOperations := 100

	// Test concurrent reads
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				_ = clock.NowMillis()
				_ = clock.NowSeconds()
			}
		}()
	}

	// Test concurrent writes
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				if j%2 == 0 {
					clock.AdvanceTime(time.Millisecond)
				} else {
					clock.SetTime(initialTime + int64(id*1000) + int64(j))
				}
			}
		}(i)
	}

	wg.Wait()

	// If we reach here without data races, the test passes
	// The exact final time is not deterministic due to concurrent writes,
	// but we can verify the clock is still functional
	finalTime := clock.NowMillis()
	if finalTime < 0 {
		t.Errorf("Clock time became negative: %d", finalTime)
	}
}

// Helper function to calculate absolute difference
func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
