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

package nutsdb

import (
	"testing"
)

func TestOptionsWithClock(t *testing.T) {
	mockClock := NewMockClock(1640995200000)

	opts := DefaultOptions
	WithClock(mockClock)(&opts)

	if opts.Clock != mockClock {
		t.Error("Expected custom clock to be set in options")
	}
}

func TestDefaultOptionsHasClock(t *testing.T) {
	opts := DefaultOptions

	if opts.Clock == nil {
		t.Error("Expected default options to have a clock")
	}

	// Verify it's a RealClock by checking it returns reasonable time
	nowMillis := opts.Clock.NowMillis()
	if nowMillis <= 0 {
		t.Errorf("Expected positive timestamp from default clock, got %d", nowMillis)
	}
}

// Helper function to calculate absolute difference
func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
