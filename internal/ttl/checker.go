// Copyright 2025 The nutsdb Author. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package ttl

import (
	"github.com/nutsdb/nutsdb/internal/core"
)

// Persistent represents the data persistent flag (TTL = 0 means never expire)
const Persistent uint32 = 0

// ExpiredCallback is a function type for handling expired record notifications.
type ExpiredCallback func(bucketId uint64, key []byte, ds uint16, timestamp uint64)

// Checker handles TTL expiration logic using a unified clock.
type Checker struct {
	Clock     Clock
	onExpired ExpiredCallback
}

// NewChecker creates a new Checker with the specified clock.
func NewChecker(clk Clock) *Checker {
	return &Checker{Clock: clk}
}

// SetExpiredCallback sets a callback to be invoked when expired records are detected.
func (c *Checker) SetExpiredCallback(callback ExpiredCallback) {
	c.onExpired = callback
}

// IsExpired checks if a record is expired based on TTL and timestamp.
// TTL is in seconds, timestamp is in milliseconds.
func (c *Checker) IsExpired(ttl uint32, timestamp uint64) bool {
	if ttl == Persistent {
		return false
	}
	now := c.Clock.NowMillis()
	expirationTime := int64(timestamp) + int64(ttl)*1000
	return now >= expirationTime
}

// FilterExpiredRecord checks a single record and triggers cleanup if expired.
// Returns true if the record is valid (not expired).
func (c *Checker) FilterExpiredRecord(bucketId uint64, key []byte, record *core.Record, ds uint16) bool {
	if record == nil {
		return false
	}
	if c.IsExpired(record.TTL, record.Timestamp) {
		c.triggerCallback(bucketId, key, ds, record.Timestamp)
		return false
	}
	return true
}

// FilterExpiredRecords filters a slice of records, removing expired ones.
// Returns a new slice containing only valid (non-expired) records.
func (c *Checker) FilterExpiredRecords(bucketId uint64, records []*core.Record, ds uint16) []*core.Record {
	valid := make([]*core.Record, 0, len(records))
	for _, record := range records {
		if !c.IsExpired(record.TTL, record.Timestamp) {
			valid = append(valid, record)
		} else {
			c.triggerCallback(bucketId, record.Key, ds, record.Timestamp)
		}
	}
	return valid
}

// FilterExpiredItems filters a slice of items, removing expired ones.
// Returns a new slice containing only valid (non-expired) items.
func (c *Checker) FilterExpiredItems(bucketId uint64, items []*core.Item[core.Record], ds uint16) []*core.Item[core.Record] {
	valid := make([]*core.Item[core.Record], 0, len(items))
	for _, item := range items {
		if item.Record != nil && !c.IsExpired(item.Record.TTL, item.Record.Timestamp) {
			valid = append(valid, item)
		} else if item.Record != nil {
			c.triggerCallback(bucketId, item.Key, ds, item.Record.Timestamp)
		}
	}
	return valid
}

// CalculateRemainingTTL calculates remaining TTL in seconds.
// Returns -1 for persistent, 0 for expired, positive for remaining seconds.
func (c *Checker) CalculateRemainingTTL(ttl uint32, timestamp uint64) int64 {
	if ttl == Persistent {
		return -1
	}
	now := c.Clock.NowMillis()
	expirationTime := int64(timestamp) + int64(ttl)*1000
	remaining := expirationTime - now
	if remaining <= 0 {
		return 0
	}
	return remaining / 1000
}

// triggerCallback invokes the expired callback if set.
func (c *Checker) triggerCallback(bucketId uint64, key []byte, ds uint16, timestamp uint64) {
	if c.onExpired != nil {
		c.onExpired(bucketId, key, ds, timestamp)
	}
}
