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

package checker

import (
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/ttl/clock"
)

// Data structure constants for testing
const (
	DataStructureBTree uint16 = 2
)

func TestChecker_IsExpired(t *testing.T) {
	clk := clock.NewMockClock(1000000) // Start at 1000 seconds in milliseconds
	checker := NewChecker(clk)

	tests := []struct {
		name      string
		ttl       uint32
		timestamp uint64
		expected  bool
	}{
		{
			name:      "persistent record should not expire",
			ttl:       Persistent,
			timestamp: 500000, // 500 seconds ago
			expected:  false,
		},
		{
			name:      "non-expired record",
			ttl:       100,    // 100 seconds TTL
			timestamp: 950000, // 950 seconds, expires at 1050 seconds
			expected:  false,
		},
		{
			name:      "expired record",
			ttl:       50,     // 50 seconds TTL
			timestamp: 900000, // 900 seconds, expires at 950 seconds
			expected:  true,
		},
		{
			name:      "exactly at expiration time",
			ttl:       100,    // 100 seconds TTL
			timestamp: 900000, // 900 seconds, expires at 1000 seconds (now)
			expected:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := checker.IsExpired(tt.ttl, tt.timestamp)
			if result != tt.expected {
				t.Errorf("IsExpired() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestChecker_FilterExpiredRecord(t *testing.T) {
	clk := clock.NewMockClock(1000000) // Start at 1000 seconds in milliseconds
	checker := NewChecker(clk)

	// Track callback invocations
	var callbackBucketId uint64
	var callbackKey []byte
	var callbackDs uint16
	callbackInvoked := false

	checker.SetExpiredCallback(func(bucketId uint64, key []byte, ds uint16) {
		callbackBucketId = bucketId
		callbackKey = key
		callbackDs = ds
		callbackInvoked = true
	})

	tests := []struct {
		name           string
		record         *core.Record
		expectedValid  bool
		expectCallback bool
	}{
		{
			name:           "nil record should return false",
			record:         nil,
			expectedValid:  false,
			expectCallback: false,
		},
		{
			name: "valid record should return true",
			record: &core.Record{
				Key:       []byte("valid-key"),
				TTL:       100,    // 100 seconds TTL
				Timestamp: 950000, // 950 seconds, expires at 1050 seconds
			},
			expectedValid:  true,
			expectCallback: false,
		},
		{
			name: "expired record should return false and trigger callback",
			record: &core.Record{
				Key:       []byte("expired-key"),
				TTL:       50,     // 50 seconds TTL
				Timestamp: 900000, // 900 seconds, expires at 950 seconds
			},
			expectedValid:  false,
			expectCallback: true,
		},
	}

	testBucketId := uint64(1)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset callback state
			callbackInvoked = false
			callbackBucketId = 0
			callbackKey = nil
			callbackDs = 0

			key := []byte("test-key")
			if tt.record != nil {
				key = tt.record.Key
			}

			result := checker.FilterExpiredRecord(testBucketId, key, tt.record, DataStructureBTree)

			if result != tt.expectedValid {
				t.Errorf("FilterExpiredRecord() = %v, want %v", result, tt.expectedValid)
			}

			if tt.expectCallback && !callbackInvoked {
				t.Error("Expected callback to be invoked but it wasn't")
			}

			if !tt.expectCallback && callbackInvoked {
				t.Error("Expected callback not to be invoked but it was")
			}

			if tt.expectCallback && callbackInvoked {
				if callbackBucketId != testBucketId {
					t.Errorf("Callback bucketId = %d, want %d", callbackBucketId, testBucketId)
				}
				if string(callbackKey) != string(key) {
					t.Errorf("Callback key = %s, want %s", string(callbackKey), string(key))
				}
				if callbackDs != DataStructureBTree {
					t.Errorf("Callback ds = %d, want %d", callbackDs, DataStructureBTree)
				}
			}
		})
	}
}

func TestChecker_FilterExpiredRecords(t *testing.T) {
	clk := clock.NewMockClock(1000000) // Start at 1000 seconds in milliseconds
	checker := NewChecker(clk)

	// Track callback invocations
	var callbackKeys [][]byte
	checker.SetExpiredCallback(func(bucketId uint64, key []byte, ds uint16) {
		callbackKeys = append(callbackKeys, key)
	})

	records := []*core.Record{
		{
			Key:       []byte("persistent"),
			TTL:       Persistent,
			Timestamp: 500000, // Should never expire
		},
		{
			Key:       []byte("valid"),
			TTL:       100,    // 100 seconds TTL
			Timestamp: 950000, // 950 seconds, expires at 1050 seconds
		},
		{
			Key:       []byte("expired1"),
			TTL:       50,     // 50 seconds TTL
			Timestamp: 900000, // 900 seconds, expires at 950 seconds
		},
		{
			Key:       []byte("expired2"),
			TTL:       30,     // 30 seconds TTL
			Timestamp: 800000, // 800 seconds, expires at 830 seconds
		},
	}

	// Reset callback state
	callbackKeys = nil

	testBucketId := uint64(1)
	result := checker.FilterExpiredRecords(testBucketId, records, DataStructureBTree)

	// Should have 2 valid records (persistent and valid)
	if len(result) != 2 {
		t.Errorf("FilterExpiredRecords() returned %d records, want 2", len(result))
	}

	// Check that the correct records are returned
	expectedKeys := []string{"persistent", "valid"}
	for i, record := range result {
		if string(record.Key) != expectedKeys[i] {
			t.Errorf("Record %d key = %s, want %s", i, string(record.Key), expectedKeys[i])
		}
	}

	// Check that callbacks were invoked for expired records
	if len(callbackKeys) != 2 {
		t.Errorf("Expected 2 callback invocations, got %d", len(callbackKeys))
	}

	expectedExpiredKeys := []string{"expired1", "expired2"}
	for i, key := range callbackKeys {
		if string(key) != expectedExpiredKeys[i] {
			t.Errorf("Callback %d key = %s, want %s", i, string(key), expectedExpiredKeys[i])
		}
	}
}

func TestChecker_FilterExpiredItems(t *testing.T) {
	clk := clock.NewMockClock(1000000) // Start at 1000 seconds in milliseconds
	checker := NewChecker(clk)

	// Track callback invocations
	var callbackKeys [][]byte
	checker.SetExpiredCallback(func(bucketId uint64, key []byte, ds uint16) {
		callbackKeys = append(callbackKeys, key)
	})

	items := []*core.Item[core.Record]{
		{
			Key: []byte("persistent"),
			Record: &core.Record{
				Key:       []byte("persistent"),
				TTL:       Persistent,
				Timestamp: 500000, // Should never expire
			},
		},
		{
			Key: []byte("valid"),
			Record: &core.Record{
				Key:       []byte("valid"),
				TTL:       100,    // 100 seconds TTL
				Timestamp: 950000, // 950 seconds, expires at 1050 seconds
			},
		},
		{
			Key: []byte("expired"),
			Record: &core.Record{
				Key:       []byte("expired"),
				TTL:       50,     // 50 seconds TTL
				Timestamp: 900000, // 900 seconds, expires at 950 seconds
			},
		},
		{
			Key:    []byte("nil-record"),
			Record: nil, // Should be filtered out
		},
	}

	// Reset callback state
	callbackKeys = nil

	testBucketId := uint64(1)
	result := checker.FilterExpiredItems(testBucketId, items, DataStructureBTree)

	// Should have 2 valid items (persistent and valid)
	if len(result) != 2 {
		t.Errorf("FilterExpiredItems() returned %d items, want 2", len(result))
	}

	// Check that the correct items are returned
	expectedKeys := []string{"persistent", "valid"}
	for i, item := range result {
		if string(item.Key) != expectedKeys[i] {
			t.Errorf("Item %d key = %s, want %s", i, string(item.Key), expectedKeys[i])
		}
	}

	// Check that callback was invoked for expired record
	if len(callbackKeys) != 1 {
		t.Errorf("Expected 1 callback invocation, got %d", len(callbackKeys))
	}

	if len(callbackKeys) > 0 && string(callbackKeys[0]) != "expired" {
		t.Errorf("Callback key = %s, want expired", string(callbackKeys[0]))
	}
}

func TestChecker_TimeAdvancement(t *testing.T) {
	clk := clock.NewMockClock(1000000) // Start at 1000 seconds in milliseconds
	checker := NewChecker(clk)

	record := &core.Record{
		Key:       []byte("test-key"),
		TTL:       60,     // 60 seconds TTL
		Timestamp: 950000, // 950 seconds, expires at 1010 seconds
	}

	// Initially should not be expired
	if checker.IsExpired(record.TTL, record.Timestamp) {
		t.Error("Record should not be expired initially")
	}

	// Advance time by 5 seconds (to 1005 seconds)
	clk.AdvanceTime(5 * time.Second)
	if checker.IsExpired(record.TTL, record.Timestamp) {
		t.Error("Record should not be expired after 5 seconds")
	}

	// Advance time by another 10 seconds (to 1015 seconds)
	clk.AdvanceTime(10 * time.Second)
	if !checker.IsExpired(record.TTL, record.Timestamp) {
		t.Error("Record should be expired after 15 seconds total")
	}
}

// Integration tests for TTLChecker
func TestCheckerIntegration(t *testing.T) {
	clk := clock.NewMockClock(1000000) // Start at 1000 seconds in milliseconds
	checker := NewChecker(clk)

	// Test that the checker is properly created and functional
	if checker == nil {
		t.Error("Expected Checker to be created")
	}

	// Test basic expiration logic
	expired := checker.IsExpired(50, 900000) // 50 seconds TTL, 900 seconds timestamp, expires at 950 seconds
	if !expired {
		t.Error("Expected record to be expired")
	}

	notExpired := checker.IsExpired(100, 950000) // 100 seconds TTL, 950 seconds timestamp, expires at 1050 seconds
	if notExpired {
		t.Error("Expected record not to be expired")
	}
}
