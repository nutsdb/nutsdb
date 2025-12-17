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

package data_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/data"
	"github.com/nutsdb/nutsdb/internal/ttl/checker"
	"github.com/nutsdb/nutsdb/internal/ttl/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestRecord creates a record with the given key, TTL, and timestamp
func createTestRecord(key string, ttlVal uint32, timestamp uint64) *core.Record {
	return core.NewRecord().
		WithKey([]byte(key)).
		WithValue([]byte("value_" + key)).
		WithTTL(ttlVal).
		WithTimestamp(timestamp)
}

func TestBTree_TTL_Find(t *testing.T) {
	btree := data.NewBTree()
	mockClock := clock.NewMockClock(1000000) // Current time: 1000 seconds in millis
	checker := checker.NewChecker(mockClock)
	btree.SetTTLChecker(checker)

	// Insert a record that is not expired (TTL 100s, timestamp 999000ms = 999s)
	// Expiration: 999s + 100s = 1099s = 1099000ms > 1000000ms (current)
	validRecord := createTestRecord("valid_key", 100, 999000)
	btree.InsertRecord(validRecord.Key, validRecord)

	// Insert a record that is expired (TTL 10s, timestamp 900000ms = 900s)
	// Expiration: 900s + 10s = 910s = 910000ms < 1000000ms (current)
	expiredRecord := createTestRecord("expired_key", 10, 900000)
	btree.InsertRecord(expiredRecord.Key, expiredRecord)

	// Insert a persistent record (TTL 0)
	persistentRecord := createTestRecord("persistent_key", core.Persistent, 0)
	btree.InsertRecord(persistentRecord.Key, persistentRecord)

	t.Run("find valid record", func(t *testing.T) {
		record, found := btree.Find([]byte("valid_key"))
		require.True(t, found)
		assert.Equal(t, []byte("valid_key"), record.Key)
	})

	t.Run("find expired record returns not found", func(t *testing.T) {
		record, found := btree.Find([]byte("expired_key"))
		require.False(t, found)
		assert.Nil(t, record)
	})

	t.Run("find persistent record", func(t *testing.T) {
		record, found := btree.Find([]byte("persistent_key"))
		require.True(t, found)
		assert.Equal(t, []byte("persistent_key"), record.Key)
	})

	t.Run("find non-existent key", func(t *testing.T) {
		record, found := btree.Find([]byte("non_existent"))
		require.False(t, found)
		assert.Nil(t, record)
	})
}

func TestBTree_TTL_ExpiredCallback(t *testing.T) {
	btree := data.NewBTree()
	mockClock := clock.NewMockClock(1000000)
	checker := checker.NewChecker(mockClock)
	btree.SetTTLChecker(checker)

	var callbackKey []byte
	var callbackDs uint16
	btree.SetExpiredCallback(func(key []byte, ds uint16) {
		callbackKey = key
		callbackDs = ds
	})

	// Insert an expired record
	expiredRecord := createTestRecord("expired_key", 10, 900000)
	btree.InsertRecord(expiredRecord.Key, expiredRecord)

	// Find should trigger callback
	_, found := btree.Find([]byte("expired_key"))
	require.False(t, found)
	assert.Equal(t, []byte("expired_key"), callbackKey)
	assert.Equal(t, core.DataStructureBTree, callbackDs)
}

func TestBTree_TTL_All(t *testing.T) {
	btree := data.NewBTree()
	mockClock := clock.NewMockClock(1000000)
	checker := checker.NewChecker(mockClock)
	btree.SetTTLChecker(checker)

	// Insert mix of valid and expired records
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key_%02d", i)
		var ttlVal uint32
		var timestamp uint64
		if i%2 == 0 {
			// Valid records: TTL 100s, timestamp 999000ms
			ttlVal = 100
			timestamp = 999000
		} else {
			// Expired records: TTL 10s, timestamp 900000ms
			ttlVal = 10
			timestamp = 900000
		}
		record := createTestRecord(key, ttlVal, timestamp)
		btree.InsertRecord(record.Key, record)
	}

	records := btree.All()
	// Should only return 5 valid records (even indices)
	assert.Len(t, records, 5)
	for _, r := range records {
		// All returned records should be valid (even index keys)
		assert.Contains(t, string(r.Key), "_0")
	}
}

func TestBTree_TTL_Range(t *testing.T) {
	btree := data.NewBTree()
	mockClock := clock.NewMockClock(1000000)
	checker := checker.NewChecker(mockClock)
	btree.SetTTLChecker(checker)

	// Insert records
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key_%02d", i)
		var ttlVal uint32
		if i%2 == 0 {
			ttlVal = 100 // Valid
		} else {
			ttlVal = 10 // Expired
		}
		record := createTestRecord(key, ttlVal, 999000)
		if ttlVal == 10 {
			record.Timestamp = 900000 // Make it expired
		}
		btree.InsertRecord(record.Key, record)
	}

	records := btree.Range([]byte("key_00"), []byte("key_05"))
	// Should only return valid records in range (0, 2, 4)
	assert.Len(t, records, 3)
}

func TestBTree_TTL_PrefixScan(t *testing.T) {
	btree := data.NewBTree()
	mockClock := clock.NewMockClock(1000000)
	checker := checker.NewChecker(mockClock)
	btree.SetTTLChecker(checker)

	// Insert records with prefix
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("prefix_%02d", i)
		var ttlVal uint32
		if i%2 == 0 {
			ttlVal = 100 // Valid
		} else {
			ttlVal = 10 // Expired
		}
		record := createTestRecord(key, ttlVal, 999000)
		if ttlVal == 10 {
			record.Timestamp = 900000
		}
		btree.InsertRecord(record.Key, record)
	}

	t.Run("prefix scan with limit", func(t *testing.T) {
		records := btree.PrefixScan([]byte("prefix_"), 0, 3)
		assert.Len(t, records, 3)
		// Should be first 3 valid records
		assert.Equal(t, "prefix_00", string(records[0].Key))
		assert.Equal(t, "prefix_02", string(records[1].Key))
		assert.Equal(t, "prefix_04", string(records[2].Key))
	})

	t.Run("prefix scan with offset", func(t *testing.T) {
		records := btree.PrefixScan([]byte("prefix_"), 2, 2)
		assert.Len(t, records, 2)
		// Should skip first 2 valid records
		assert.Equal(t, "prefix_04", string(records[0].Key))
		assert.Equal(t, "prefix_06", string(records[1].Key))
	})
}

func TestBTree_TTL_MinMax(t *testing.T) {
	btree := data.NewBTree()
	mockClock := clock.NewMockClock(1000000)
	checker := checker.NewChecker(mockClock)
	btree.SetTTLChecker(checker)

	// Insert records where min and max are expired
	records := []struct {
		key       string
		ttlVal    uint32
		timestamp uint64
	}{
		{"aaa", 10, 900000},  // Expired (min key)
		{"bbb", 100, 999000}, // Valid
		{"ccc", 100, 999000}, // Valid
		{"zzz", 10, 900000},  // Expired (max key)
	}

	for _, r := range records {
		record := createTestRecord(r.key, r.ttlVal, r.timestamp)
		btree.InsertRecord(record.Key, record)
	}

	t.Run("min returns first non-expired", func(t *testing.T) {
		item, found := btree.Min()
		require.True(t, found)
		assert.Equal(t, "bbb", string(item.Key))
	})

	t.Run("max returns last non-expired", func(t *testing.T) {
		item, found := btree.Max()
		require.True(t, found)
		assert.Equal(t, "ccc", string(item.Key))
	})
}

func TestBTree_TTL_PopMinMax(t *testing.T) {
	btree := data.NewBTree()
	mockClock := clock.NewMockClock(1000000)
	checker := checker.NewChecker(mockClock)
	btree.SetTTLChecker(checker)

	// Insert records where min is expired
	records := []struct {
		key       string
		ttlVal    uint32
		timestamp uint64
	}{
		{"aaa", 10, 900000},  // Expired
		{"bbb", 100, 999000}, // Valid
		{"ccc", 100, 999000}, // Valid
	}

	for _, r := range records {
		record := createTestRecord(r.key, r.ttlVal, r.timestamp)
		btree.InsertRecord(record.Key, record)
	}

	t.Run("pop min skips expired", func(t *testing.T) {
		item, found := btree.PopMin()
		require.True(t, found)
		assert.Equal(t, "bbb", string(item.Key))
	})
}

func TestBTree_TTL_NoChecker(t *testing.T) {
	// Test that BTree works normally without TTL checker
	btree := data.NewBTree()

	// Insert a record that would be expired if TTL was enabled
	record := createTestRecord("key", 10, 900000)
	btree.InsertRecord(record.Key, record)

	// Without TTL checker, record should be found
	found, ok := btree.Find([]byte("key"))
	require.True(t, ok)
	assert.Equal(t, []byte("key"), found.Key)
}

func TestBTree_TTL_AllItems(t *testing.T) {
	btree := data.NewBTree()
	mockClock := clock.NewMockClock(1000000)
	checker := checker.NewChecker(mockClock)
	btree.SetTTLChecker(checker)

	// Insert mix of valid and expired records
	validRecord := createTestRecord("valid", 100, 999000)
	expiredRecord := createTestRecord("expired", 10, 900000)
	btree.InsertRecord(validRecord.Key, validRecord)
	btree.InsertRecord(expiredRecord.Key, expiredRecord)

	items := btree.AllItems()
	assert.Len(t, items, 1)
	assert.Equal(t, "valid", string(items[0].Key))
}

func TestBTree_TTL_TimeAdvancement(t *testing.T) {
	btree := data.NewBTree()
	mockClock := clock.NewMockClock(1000000) // Start at 1000 seconds
	checker := checker.NewChecker(mockClock)
	btree.SetTTLChecker(checker)

	// Insert a record with TTL 100s, timestamp 1000000ms (current time)
	// Expiration: 1000s + 100s = 1100s = 1100000ms
	record := createTestRecord("key", 100, 1000000)
	btree.InsertRecord(record.Key, record)

	// Record should be valid now
	found, ok := btree.Find([]byte("key"))
	require.True(t, ok)
	assert.NotNil(t, found)

	// Advance time past expiration
	mockClock.AdvanceTime(101 * time.Second) // Advance 101 seconds

	// Record should now be expired
	found, ok = btree.Find([]byte("key"))
	require.False(t, ok)
	assert.Nil(t, found)
}
