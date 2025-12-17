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
	"github.com/nutsdb/nutsdb/internal/clock"
	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/data"
)

// Persistent represents the data persistent flag
const Persistent uint32 = core.Persistent

// Checker handles TTL expiration logic using a unified clock.
// This is the single source of truth for TTL checking across all data structures.
type Checker struct {
	clock     clock.Clock
	onExpired func(key []byte, ds uint16) // Optional callback for cleanup
}

// NewChecker creates a new Checker with the specified clock.
func NewChecker(clk clock.Clock) *Checker {
	return &Checker{
		clock: clk,
	}
}

// SetExpiredCallback sets a callback to be invoked when expired records are detected.
// The callback receives the expired key and data structure type.
func (tc *Checker) SetExpiredCallback(callback func(key []byte, ds uint16)) {
	tc.onExpired = callback
}

// IsExpired checks if a record is expired based on TTL and timestamp.
// This is the single source of truth for TTL checking across all data structures.
// TTL is in seconds, timestamp is in milliseconds for internal consistency.
func (tc *Checker) IsExpired(ttl uint32, timestamp uint64) bool {
	if ttl == Persistent {
		return false
	}

	now := tc.clock.NowMillis()
	expirationTime := int64(timestamp) + int64(ttl)*1000 // Convert TTL seconds to milliseconds
	return now >= expirationTime
}

// FilterExpiredRecord checks a single record and triggers cleanup if expired.
// Returns true if the record is valid (not expired), false if expired.
func (tc *Checker) FilterExpiredRecord(key []byte, record *core.Record, ds uint16) bool {
	if record == nil {
		return false
	}
	if tc.IsExpired(record.TTL, record.Timestamp) {
		tc.triggerExpiredCallback(key, ds)
		return false // Record is expired
	}
	return true // Record is valid
}

// triggerExpiredCallback invokes the expired callback if set.
func (tc *Checker) triggerExpiredCallback(key []byte, ds uint16) {
	if tc.onExpired != nil {
		tc.onExpired(key, ds)
	}
}

// FilterExpiredRecords filters a slice of records, removing expired ones.
// Returns a new slice containing only valid (non-expired) records.
func (tc *Checker) FilterExpiredRecords(records []*core.Record, ds uint16) []*core.Record {
	valid := make([]*core.Record, 0, len(records))
	for _, record := range records {
		if !tc.IsExpired(record.TTL, record.Timestamp) {
			valid = append(valid, record)
		} else {
			tc.triggerExpiredCallback(record.Key, ds)
		}
	}
	return valid
}

// FilterExpiredItems filters a slice of items, removing expired ones.
// Returns a new slice containing only valid (non-expired) items.
func (tc *Checker) FilterExpiredItems(items []*data.Item[core.Record], ds uint16) []*data.Item[core.Record] {
	valid := make([]*data.Item[core.Record], 0, len(items))
	for _, item := range items {
		if item.Record != nil && !tc.IsExpired(item.Record.TTL, item.Record.Timestamp) {
			valid = append(valid, item)
		} else if item.Record != nil {
			tc.triggerExpiredCallback(item.Key, ds)
		}
	}
	return valid
}
