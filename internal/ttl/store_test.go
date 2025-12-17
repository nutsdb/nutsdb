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

	"github.com/nutsdb/nutsdb/internal/clock"
)

// DataStructureBTree constant is already defined in checker_test.go

func TestNewBaseStore(t *testing.T) {
	store := NewBaseStore(DataStructureBTree)
	if store == nil {
		t.Fatal("NewBaseStore returned nil")
	}
	if store.ds != DataStructureBTree {
		t.Errorf("Expected ds to be %d, got %d", DataStructureBTree, store.ds)
	}
	if store.ttlChecker != nil {
		t.Error("Expected ttlChecker to be nil initially")
	}
}

func TestBaseStore_SetTTLChecker(t *testing.T) {
	store := NewBaseStore(DataStructureBTree)
	clk := clock.NewMockClock(1000000) // Start at 1000 seconds in milliseconds
	checker := NewChecker(clk)

	store.SetTTLChecker(checker)
	if store.ttlChecker != checker {
		t.Error("TTL checker was not set correctly")
	}
}

func TestBaseStore_isExpired(t *testing.T) {
	store := NewBaseStore(DataStructureBTree)
	clk := clock.NewMockClock(1000000) // Start at 1000 seconds in milliseconds
	checker := NewChecker(clk)
	store.SetTTLChecker(checker)

	tests := []struct {
		name      string
		ttl       uint32
		timestamp uint64
		expected  bool
	}{
		{
			name:      "persistent record",
			ttl:       Persistent,
			timestamp: 500000, // 500 seconds ago
			expected:  false,
		},
		{
			name:      "valid record",
			ttl:       600,    // 600 seconds TTL
			timestamp: 500000, // Created at 500 seconds, expires at 1100 seconds
			expected:  false,
		},
		{
			name:      "expired record",
			ttl:       300,    // 300 seconds TTL
			timestamp: 500000, // Created at 500 seconds, expired at 800 seconds
			expected:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := store.isExpired(tt.ttl, tt.timestamp)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestBaseStore_calculateRemainingTTL(t *testing.T) {
	store := NewBaseStore(DataStructureBTree)
	clk := clock.NewMockClock(1000000) // Start at 1000 seconds in milliseconds
	checker := NewChecker(clk)
	store.SetTTLChecker(checker)

	tests := []struct {
		name      string
		ttl       uint32
		timestamp uint64
		expected  int64
	}{
		{
			name:      "persistent record",
			ttl:       Persistent,
			timestamp: 500000,
			expected:  -1,
		},
		{
			name:      "valid record with remaining time",
			ttl:       600,    // 600 seconds TTL
			timestamp: 500000, // Created at 500 seconds, expires at 1100 seconds, 100 seconds remaining
			expected:  100,
		},
		{
			name:      "expired record",
			ttl:       300,    // 300 seconds TTL
			timestamp: 500000, // Created at 500 seconds, expired at 800 seconds
			expected:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := store.calculateRemainingTTL(tt.ttl, tt.timestamp)
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestBaseStore_isValid(t *testing.T) {
	store := NewBaseStore(DataStructureBTree)
	clk := clock.NewMockClock(1000000) // Start at 1000 seconds in milliseconds
	checker := NewChecker(clk)
	store.SetTTLChecker(checker)

	// Test with valid record
	key := []byte("test-key")
	result := store.isValid(key, 600, 500000) // Valid record
	if !result {
		t.Error("Expected true for valid record")
	}

	// Test with expired record
	result = store.isValid(key, 300, 500000) // Expired record
	if result {
		t.Error("Expected false for expired record")
	}
}

func TestBaseStore_triggerExpiredCallback(t *testing.T) {
	store := NewBaseStore(DataStructureBTree)
	clk := clock.NewMockClock(1000000)
	checker := NewChecker(clk)

	// Set up callback to track calls
	var callbackCalled bool
	var callbackKey []byte
	var callbackDs uint16

	checker.SetExpiredCallback(func(key []byte, ds uint16) {
		callbackCalled = true
		callbackKey = key
		callbackDs = ds
	})

	store.SetTTLChecker(checker)

	// Trigger callback
	key := []byte("test-key")
	store.triggerExpiredCallback(key)

	if !callbackCalled {
		t.Error("Expected callback to be called")
	}
	if string(callbackKey) != string(key) {
		t.Errorf("Expected callback key %s, got %s", string(key), string(callbackKey))
	}
	if callbackDs != DataStructureBTree {
		t.Errorf("Expected callback ds %d, got %d", DataStructureBTree, callbackDs)
	}
}

func TestConvertSecondsToMillis(t *testing.T) {
	tests := []struct {
		seconds  uint32
		expected int64
	}{
		{0, 0},
		{1, 1000},
		{60, 60000},
		{3600, 3600000},
	}

	for _, tt := range tests {
		result := ConvertSecondsToMillis(tt.seconds)
		if result != tt.expected {
			t.Errorf("ConvertSecondsToMillis(%d) = %d, expected %d", tt.seconds, result, tt.expected)
		}
	}
}

func TestConvertMillisToSeconds(t *testing.T) {
	tests := []struct {
		millis   int64
		expected int64
	}{
		{0, 0},
		{1000, 1},
		{60000, 60},
		{3600000, 3600},
		{1500, 1}, // Test truncation
	}

	for _, tt := range tests {
		result := ConvertMillisToSeconds(tt.millis)
		if result != tt.expected {
			t.Errorf("ConvertMillisToSeconds(%d) = %d, expected %d", tt.millis, result, tt.expected)
		}
	}
}

func TestCalculateExpirationTime(t *testing.T) {
	tests := []struct {
		name      string
		timestamp uint64
		ttl       uint32
		expected  int64
	}{
		{
			name:      "persistent record",
			timestamp: 1000000,
			ttl:       Persistent,
			expected:  -1,
		},
		{
			name:      "normal record",
			timestamp: 1000000, // 1000 seconds
			ttl:       300,     // 300 seconds TTL
			expected:  1300000, // Expires at 1300 seconds
		},
		{
			name:      "short TTL",
			timestamp: 1000000, // 1000 seconds
			ttl:       1,       // 1 second TTL
			expected:  1001000, // Expires at 1001 seconds
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateExpirationTime(tt.timestamp, tt.ttl)
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestIsExpiredAt(t *testing.T) {
	tests := []struct {
		name      string
		ttl       uint32
		timestamp uint64
		checkTime int64
		expected  bool
	}{
		{
			name:      "persistent record",
			ttl:       Persistent,
			timestamp: 1000000,
			checkTime: 2000000,
			expected:  false,
		},
		{
			name:      "not expired yet",
			ttl:       300,     // 300 seconds TTL
			timestamp: 1000000, // Created at 1000 seconds
			checkTime: 1200000, // Check at 1200 seconds (before 1300 expiration)
			expected:  false,
		},
		{
			name:      "exactly at expiration",
			ttl:       300,     // 300 seconds TTL
			timestamp: 1000000, // Created at 1000 seconds
			checkTime: 1300000, // Check at 1300 seconds (exactly at expiration)
			expected:  true,
		},
		{
			name:      "expired",
			ttl:       300,     // 300 seconds TTL
			timestamp: 1000000, // Created at 1000 seconds
			checkTime: 1400000, // Check at 1400 seconds (after expiration)
			expected:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsExpiredAt(tt.ttl, tt.timestamp, tt.checkTime)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}
