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
	"regexp"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/ttl/checker"
	"github.com/tidwall/btree"
)

// BTree represents a B-tree index with optional TTL support.
// TTL checking is performed at the index layer, automatically filtering
// expired records during retrieval operations.
type BTree struct {
	btree *btree.BTreeG[*core.Item[core.Record]]

	// TTL support fields
	ttlChecker      *checker.Checker        // TTL checker for expiration logic (optional)
	expiredCallback checker.ExpiredCallback // Callback for expired record notifications (optional)
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

// Find retrieves a record by key, automatically filtering expired records.
// If the record is expired, it returns nil and false, and triggers the expired callback.
func (bt *BTree) Find(key []byte) (*core.Record, bool) {
	item, ok := bt.btree.Get(core.NewItem[core.Record](key, nil))
	if !ok {
		return nil, false
	}

	// Check if record is expired
	if bt.isExpired(item.Record) {
		bt.triggerExpiredCallback(key)
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
		if bt.isExpired(item.Record) {
			bt.triggerExpiredCallback(item.Key)
			continue
		}
		records = append(records, item.Record)
	}

	return records
}

// AllItems returns all non-expired items in the BTree.
// Expired items are automatically filtered and trigger the expired callback.
func (bt *BTree) AllItems() []*core.Item[core.Record] {
	items := bt.btree.Items()

	// If no TTL checker, return all items
	if bt.ttlChecker == nil {
		return items
	}

	validItems := make([]*core.Item[core.Record], 0, len(items))
	for _, item := range items {
		if bt.isExpired(item.Record) {
			bt.triggerExpiredCallback(item.Key)
			continue
		}
		validItems = append(validItems, item)
	}

	return validItems
}

// Range returns records in the specified key range, filtering expired ones.
// Expired records are automatically filtered and trigger the expired callback.
func (bt *BTree) Range(start, end []byte) []*core.Record {
	records := make([]*core.Record, 0)

	bt.btree.Ascend(&core.Item[core.Record]{Key: start}, func(item *core.Item[core.Record]) bool {
		if bytes.Compare(item.Key, end) > 0 {
			return false
		}
		// Check if record is expired
		if bt.isExpired(item.Record) {
			bt.triggerExpiredCallback(item.Key)
			return true // Continue to next item
		}
		records = append(records, item.Record)
		return true
	})

	return records
}

// PrefixScan returns records with the specified prefix, filtering expired ones.
// Expired records are automatically filtered and trigger the expired callback.
// Note: Expired records do not count towards offset or limit.
func (bt *BTree) PrefixScan(prefix []byte, offset, limitNum int) []*core.Record {
	records := make([]*core.Record, 0)

	bt.btree.Ascend(&core.Item[core.Record]{Key: prefix}, func(item *core.Item[core.Record]) bool {
		if !bytes.HasPrefix(item.Key, prefix) {
			return false
		}

		// Check if record is expired
		if bt.isExpired(item.Record) {
			bt.triggerExpiredCallback(item.Key)
			return true // Continue to next item, don't count towards offset/limit
		}

		if offset > 0 {
			offset--
			return true
		}

		records = append(records, item.Record)

		limitNum--
		return limitNum != 0
	})

	return records
}

// PrefixSearchScan returns records with the specified prefix matching the regex, filtering expired ones.
// Expired records are automatically filtered and trigger the expired callback.
// Note: Expired records do not count towards offset or limit.
func (bt *BTree) PrefixSearchScan(prefix []byte, reg string, offset, limitNum int) []*core.Record {
	records := make([]*core.Record, 0)

	rgx := regexp.MustCompile(reg)

	bt.btree.Ascend(&core.Item[core.Record]{Key: prefix}, func(item *core.Item[core.Record]) bool {
		if !bytes.HasPrefix(item.Key, prefix) {
			return false
		}

		// Check if record is expired
		if bt.isExpired(item.Record) {
			bt.triggerExpiredCallback(item.Key)
			return true // Continue to next item
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
	})

	return records
}

func (bt *BTree) Count() int {
	return bt.btree.Len()
}

// PopMin removes and returns the minimum key record, filtering expired ones.
// If the minimum record is expired, it removes it and continues to the next minimum.
func (bt *BTree) PopMin() (*core.Item[core.Record], bool) {
	// If no TTL checker, use original behavior
	if bt.ttlChecker == nil {
		return bt.btree.PopMin()
	}

	// Pop items until we find a non-expired one
	for {
		item, ok := bt.btree.PopMin()
		if !ok {
			return nil, false
		}
		if !bt.isExpired(item.Record) {
			return item, true
		}
		// Item was expired, trigger callback and continue
		bt.triggerExpiredCallback(item.Key)
	}
}

// PopMax removes and returns the maximum key record, filtering expired ones.
// If the maximum record is expired, it removes it and continues to the next maximum.
func (bt *BTree) PopMax() (*core.Item[core.Record], bool) {
	// If no TTL checker, use original behavior
	if bt.ttlChecker == nil {
		return bt.btree.PopMax()
	}

	// Pop items until we find a non-expired one
	for {
		item, ok := bt.btree.PopMax()
		if !ok {
			return nil, false
		}
		if !bt.isExpired(item.Record) {
			return item, true
		}
		// Item was expired, trigger callback and continue
		bt.triggerExpiredCallback(item.Key)
	}
}

// Min returns the minimum key record, filtering expired ones.
// If the minimum record is expired, it searches for the next non-expired minimum.
func (bt *BTree) Min() (*core.Item[core.Record], bool) {
	// If no TTL checker, use original behavior
	if bt.ttlChecker == nil {
		return bt.btree.Min()
	}

	// Get all items and find the first non-expired one
	items := bt.btree.Items()
	for _, item := range items {
		if bt.isExpired(item.Record) {
			bt.triggerExpiredCallback(item.Key)
			continue
		}
		return item, true
	}

	return nil, false
}

// Max returns the maximum key record, filtering expired ones.
// If the maximum record is expired, it searches for the next non-expired maximum.
func (bt *BTree) Max() (*core.Item[core.Record], bool) {
	// If no TTL checker, use original behavior
	if bt.ttlChecker == nil {
		return bt.btree.Max()
	}

	// Get all items and find the last non-expired one
	items := bt.btree.Items()
	for i := len(items) - 1; i >= 0; i-- {
		item := items[i]
		if bt.isExpired(item.Record) {
			bt.triggerExpiredCallback(item.Key)
			continue
		}
		return item, true
	}

	return nil, false
}

func (bt *BTree) Iter() btree.IterG[*core.Item[core.Record]] {
	return bt.btree.Iter()
}
