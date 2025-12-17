// Copyright 2023 The nutsdb Author. All rights reserved.
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

package data

import (
	"bytes"
	"errors"
	"regexp"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/ttl/checker"
	"github.com/tidwall/btree"
)

// ErrKeyNotFound is returned when a key is not found in the BTree.
var ErrKeyNotFound = errors.New("key not found")

// BTree represents a B-tree index with optional TTL support.
// TTL checking is performed at the index layer, automatically filtering
// expired records during retrieval operations.
type BTree struct {
	btree           *btree.BTreeG[*core.Item[core.Record]]
	ttlChecker      *checker.Checker
	expiredCallback checker.ExpiredCallback
}

// NewBTree creates a new BTree instance without TTL support.
// Use SetTTLChecker to enable TTL functionality.
func NewBTree() *BTree {
	return &BTree{
		btree: btree.NewBTreeG(func(a, b *core.Item[core.Record]) bool {
			return bytes.Compare(a.Key, b.Key) == -1
		}),
	}
}

// SetTTLChecker injects the TTL checker for expiration logic.
// This enables automatic filtering of expired records during retrieval.
func (bt *BTree) SetTTLChecker(tc *checker.Checker) {
	bt.ttlChecker = tc
}

// SetExpiredCallback sets a callback to be invoked when expired records are detected.
// The callback receives the expired key and data structure type.
func (bt *BTree) SetExpiredCallback(callback checker.ExpiredCallback) {
	bt.expiredCallback = callback
}

// isExpired checks if a record is expired using the TTL checker.
// Returns false if no TTL checker is configured (TTL disabled).
func (bt *BTree) isExpired(record *core.Record) bool {
	if bt.ttlChecker == nil || record == nil {
		return false
	}
	return bt.ttlChecker.IsExpired(record.TTL, record.Timestamp)
}

// triggerExpiredCallback notifies about an expired key if callback is set.
func (bt *BTree) triggerExpiredCallback(key []byte) {
	if bt.expiredCallback != nil {
		bt.expiredCallback(key, core.DataStructureBTree)
	}
}

// isValid checks if an item is valid (not expired).
// If expired, triggers the callback and returns false.
// Used for single item check scenarios (Find, GetTTL, etc.)
func (bt *BTree) isValid(item *core.Item[core.Record]) bool {
	if bt.isExpired(item.Record) {
		bt.triggerExpiredCallback(item.Key)
		return false
	}
	return true
}

// withTTLFilter wraps an iterator callback to automatically filter expired records.
// Used for iteration scenarios (Range, PrefixScan, Min, Max, etc.)
// Expired records are skipped (callback triggered) and iteration continues.
func (bt *BTree) withTTLFilter(fn func(*core.Item[core.Record]) bool) func(*core.Item[core.Record]) bool {
	return func(item *core.Item[core.Record]) bool {
		if !bt.isValid(item) {
			return true // Skip expired record, continue iteration
		}
		return fn(item)
	}
}

// Find retrieves a record by key, automatically filtering expired records.
// If the record is expired, it returns nil and false, and triggers the expired callback.
func (bt *BTree) Find(key []byte) (*core.Record, bool) {
	item, ok := bt.btree.Get(core.NewItem[core.Record](key, nil))
	if !ok || !bt.isValid(item) {
		return nil, false
	}
	return item.Record, true
}

func (bt *BTree) InsertRecord(key []byte, record *core.Record) bool {
	_, replaced := bt.btree.Set(core.NewItem(key, record))
	return replaced
}

func (bt *BTree) Delete(key []byte) bool {
	_, deleted := bt.btree.Delete(core.NewItem[core.Record](key, nil))
	return deleted
}

// All returns all non-expired records in the BTree.
// Expired records are automatically filtered and trigger the expired callback.
func (bt *BTree) All() []*core.Record {
	items := bt.btree.Items()
	records := make([]*core.Record, 0, len(items))
	for _, item := range items {
		if bt.isValid(item) {
			records = append(records, item.Record)
		}
	}
	return records
}

// AllItems returns all non-expired items in the BTree.
// Expired items are automatically filtered and trigger the expired callback.
func (bt *BTree) AllItems() []*core.Item[core.Record] {
	items := bt.btree.Items()
	validItems := make([]*core.Item[core.Record], 0, len(items))
	for _, item := range items {
		if bt.isValid(item) {
			validItems = append(validItems, item)
		}
	}
	return validItems
}

// Range returns records in the specified key range, filtering expired ones.
// Expired records are automatically filtered and trigger the expired callback.
func (bt *BTree) Range(start, end []byte) []*core.Record {
	records := make([]*core.Record, 0)
	bt.btree.Ascend(&core.Item[core.Record]{Key: start}, bt.withTTLFilter(func(item *core.Item[core.Record]) bool {
		if bytes.Compare(item.Key, end) > 0 {
			return false
		}
		records = append(records, item.Record)
		return true
	}))
	return records
}

// PrefixScan returns records with the specified prefix, filtering expired ones.
// Expired records are automatically filtered and trigger the expired callback.
// Note: Expired records do not count towards offset or limit.
func (bt *BTree) PrefixScan(prefix []byte, offset, limitNum int) []*core.Record {
	records := make([]*core.Record, 0)
	bt.btree.Ascend(&core.Item[core.Record]{Key: prefix}, bt.withTTLFilter(func(item *core.Item[core.Record]) bool {
		if !bytes.HasPrefix(item.Key, prefix) {
			return false
		}
		if offset > 0 {
			offset--
			return true
		}
		records = append(records, item.Record)
		limitNum--
		return limitNum != 0
	}))
	return records
}

// PrefixSearchScan returns records with the specified prefix matching the regex, filtering expired ones.
// Expired records are automatically filtered and trigger the expired callback.
// Note: Expired records do not count towards offset or limit.
func (bt *BTree) PrefixSearchScan(prefix []byte, reg string, offset, limitNum int) []*core.Record {
	records := make([]*core.Record, 0)
	rgx := regexp.MustCompile(reg)
	bt.btree.Ascend(&core.Item[core.Record]{Key: prefix}, bt.withTTLFilter(func(item *core.Item[core.Record]) bool {
		if !bytes.HasPrefix(item.Key, prefix) {
			return false
		}
		if offset > 0 {
			offset--
			return true
		}
		if !rgx.Match(bytes.TrimPrefix(item.Key, prefix)) {
			return true
		}
		records = append(records, item.Record)
		limitNum--
		return limitNum != 0
	}))
	return records
}

func (bt *BTree) Count() int {
	return bt.btree.Len()
}

// PopMin removes and returns the minimum key record, filtering expired ones.
// If the minimum record is expired, it removes it and continues to the next minimum.
func (bt *BTree) PopMin() (*core.Item[core.Record], bool) {
	for {
		item, ok := bt.btree.PopMin()
		if !ok {
			return nil, false
		}
		if bt.isValid(item) {
			return item, true
		}
	}
}

// PopMax removes and returns the maximum key record, filtering expired ones.
// If the maximum record is expired, it removes it and continues to the next maximum.
func (bt *BTree) PopMax() (*core.Item[core.Record], bool) {
	for {
		item, ok := bt.btree.PopMax()
		if !ok {
			return nil, false
		}
		if bt.isValid(item) {
			return item, true
		}
	}
}

// Min returns the minimum key record, filtering expired ones.
// If the minimum record is expired, it searches for the next non-expired minimum.
// Uses iterator for efficient traversal without loading all items into memory.
func (bt *BTree) Min() (*core.Item[core.Record], bool) {
	var result *core.Item[core.Record]
	bt.btree.Scan(bt.withTTLFilter(func(item *core.Item[core.Record]) bool {
		result = item
		return false // Found first valid item, stop iteration
	}))
	if result == nil {
		return nil, false
	}
	return result, true
}

// Max returns the maximum key record, filtering expired ones.
// If the maximum record is expired, it searches for the next non-expired maximum.
// Uses reverse iterator for efficient traversal without loading all items into memory.
func (bt *BTree) Max() (*core.Item[core.Record], bool) {
	var result *core.Item[core.Record]
	bt.btree.Reverse(bt.withTTLFilter(func(item *core.Item[core.Record]) bool {
		result = item
		return false // Found first valid item, stop iteration
	}))
	if result == nil {
		return nil, false
	}
	return result, true
}

func (bt *BTree) Iter() btree.IterG[*core.Item[core.Record]] {
	return bt.btree.Iter()
}

// GetTTL returns the remaining TTL for a key in seconds.
// Returns:
//   - (-1, nil) for persistent keys (TTL = 0)
//   - (remaining_seconds, nil) for valid keys with TTL
//   - (0, ErrKeyNotFound) for expired or non-existent keys
//
// If no TTL checker is configured, returns (-1, nil) for any existing key.
func (bt *BTree) GetTTL(key []byte) (int64, error) {
	item, ok := bt.btree.Get(core.NewItem[core.Record](key, nil))
	if !ok || !bt.isValid(item) {
		return 0, ErrKeyNotFound
	}
	// If no TTL checker, treat all records as persistent
	if bt.ttlChecker == nil {
		return -1, nil
	}
	return bt.ttlChecker.CalculateRemainingTTL(item.Record.TTL, item.Record.Timestamp), nil
}

// IsExpiredKey checks if a key exists and is expired.
// Returns:
//   - true if the key exists and has expired
//   - false if the key doesn't exist, is persistent, or is still valid
//
// If no TTL checker is configured, always returns false.
func (bt *BTree) IsExpiredKey(key []byte) bool {
	if bt.ttlChecker == nil {
		return false
	}

	item, ok := bt.btree.Get(core.NewItem[core.Record](key, nil))
	if !ok {
		return false
	}

	return bt.isExpired(item.Record)
}
