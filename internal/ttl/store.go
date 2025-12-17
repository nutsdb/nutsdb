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

// Store defines the interface for data structures that support TTL operations.
// This interface enables pluggable TTL implementations across different data structures.
type Store interface {
	// SetTTLChecker injects the TTL checker for expiration logic.
	SetTTLChecker(tc *Checker)

	// IsExpired checks if a key is expired.
	// Returns true if the key exists and has expired.
	IsExpired(key []byte) bool

	// GetTTL returns the remaining TTL for a key in seconds.
	// Returns -1 for persistent keys, error for expired/non-existent keys.
	GetTTL(key []byte) (int64, error)

	// Delete removes a key from the store.
	Delete(key []byte) error
}

// BaseStore provides common TTL functionality for data structures.
// Data structures can embed this to get TTL support with minimal code.
type BaseStore struct {
	ttlChecker *Checker
	ds         uint16 // Data structure type for callbacks
}

// NewBaseStore creates a new BaseStore with the specified data structure type.
func NewBaseStore(ds uint16) *BaseStore {
	return &BaseStore{ds: ds}
}

// SetTTLChecker injects the TTL checker for expiration logic.
func (b *BaseStore) SetTTLChecker(tc *Checker) {
	b.ttlChecker = tc
}

// isExpired checks if a record with given TTL and timestamp is expired.
// This is a helper method that delegates to the TTL checker.
func (b *BaseStore) isExpired(ttl uint32, timestamp uint64) bool {
	return b.ttlChecker.IsExpired(ttl, timestamp)
}

// triggerExpiredCallback notifies about an expired key.
// This triggers the cleanup callback if one is configured.
func (b *BaseStore) triggerExpiredCallback(key []byte) {
	b.ttlChecker.triggerExpiredCallback(key, b.ds)
}

// calculateRemainingTTL calculates remaining TTL in seconds.
// Returns -1 for persistent records, 0 for expired records, positive value for valid records.
func (b *BaseStore) calculateRemainingTTL(ttl uint32, timestamp uint64) int64 {
	if ttl == Persistent {
		return -1 // Persistent record
	}

	now := b.ttlChecker.clock.NowMillis()
	expirationTime := int64(timestamp) + int64(ttl)*1000 // Convert TTL seconds to milliseconds
	remaining := expirationTime - now

	if remaining <= 0 {
		return 0 // Expired
	}

	return remaining / 1000 // Convert back to seconds
}

// isValid checks if a record is valid (not expired) and triggers cleanup if needed.
// Returns true if the record is valid, false if expired.
func (b *BaseStore) isValid(key []byte, ttl uint32, timestamp uint64) bool {
	if b.isExpired(ttl, timestamp) {
		b.triggerExpiredCallback(key)
		return false
	}
	return true
}

// ConvertSecondsToMillis converts TTL in seconds to milliseconds for internal calculations.
// This utility ensures consistent timestamp units throughout the system.
func ConvertSecondsToMillis(seconds uint32) int64 {
	return int64(seconds) * 1000
}

// ConvertMillisToSeconds converts milliseconds to seconds for TTL calculations.
// This utility ensures consistent timestamp units throughout the system.
func ConvertMillisToSeconds(millis int64) int64 {
	return millis / 1000
}

// CalculateExpirationTime calculates the expiration time in milliseconds.
// Takes a timestamp in milliseconds and TTL in seconds, returns expiration time in milliseconds.
func CalculateExpirationTime(timestampMillis uint64, ttlSeconds uint32) int64 {
	if ttlSeconds == Persistent {
		return -1 // Never expires
	}
	return int64(timestampMillis) + ConvertSecondsToMillis(ttlSeconds)
}

// IsExpiredAt checks if a record would be expired at a specific time.
// This utility is useful for testing and time-based calculations.
func IsExpiredAt(ttl uint32, timestamp uint64, checkTimeMillis int64) bool {
	if ttl == Persistent {
		return false
	}
	expirationTime := CalculateExpirationTime(timestamp, ttl)
	return checkTimeMillis >= expirationTime
}
