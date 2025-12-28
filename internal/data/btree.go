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
	"math/rand"
	"regexp"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/ttl"
	"github.com/tidwall/btree"
)

// ErrKeyNotFound is returned when a key is not found in the BTree.
var ErrKeyNotFound = errors.New("key not found")

// BTree represents a B-tree index with optional TTL support.
type BTree struct {
	index      *btree.BTreeG[*core.Item[core.Record]]
	ttlChecker *ttl.Checker
	bucketId   uint64
	rand       *rand.Rand
}

// NewBTree creates a new BTree instance with optional TTL support.
// If no ttlChecker is provided, TTL checking is disabled (useful for internal use like List).
func NewBTree(bucketId uint64, ttlCheckers ...*ttl.Checker) *BTree {
	bt := &BTree{
		index: btree.NewBTreeG(func(a, b *core.Item[core.Record]) bool {
			return bytes.Compare(a.Key, b.Key) == -1
		}),
		bucketId: bucketId,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	if len(ttlCheckers) > 0 {
		bt.ttlChecker = ttlCheckers[0]
	}
	// If no ttlChecker provided, bt.ttlChecker remains nil (TTL checking disabled)
	return bt
}

// isValid checks if an item is valid (not expired) using the TTL checker.
// If no TTL checker is configured, all items are considered valid.
// This method triggers expiration callbacks for expired items.
func (bt *BTree) isValid(item *core.Item[core.Record]) bool {
	if bt.ttlChecker == nil {
		return item.Record != nil
	}
	return bt.ttlChecker.FilterExpiredRecord(bt.bucketId, item.Key, item.Record, core.DataStructureBTree)
}

// getItem retrieves an item by key without TTL validation.
func (bt *BTree) getItem(key []byte) (*core.Item[core.Record], bool) {
	return bt.index.Get(core.NewItem[core.Record](key, nil))
}

// getValidItem retrieves an item by key with TTL validation.
// Triggers expiration callbacks for expired items.
func (bt *BTree) getValidItem(key []byte) (*core.Item[core.Record], bool) {
	if item, ok := bt.getItem(key); ok && bt.isValid(item) {
		return item, true
	}
	return nil, false
}

// scan returns a Scanner for fluent query construction.
func (bt *BTree) scan() Scanner {
	return newBTreeScanner(bt)
}

// Find retrieves a record by key, automatically filtering expired records.
// Triggers expiration callbacks for expired records.
func (bt *BTree) Find(key []byte) (*core.Record, bool) {
	if item, ok := bt.getValidItem(key); ok {
		return item.Record, true
	}
	return nil, false
}

// FindForVerification retrieves a record by key WITHOUT triggering expiration callbacks.
// This is used for internal verification (e.g., checking timestamps before deletion).
// Returns the record even if expired, allowing caller to check timestamp.
func (bt *BTree) FindForVerification(key []byte) (*core.Record, bool) {
	item, ok := bt.getItem(key)
	if !ok || item.Record == nil {
		return nil, false
	}
	return item.Record, true
}

// SampleRandomRecords randomly samples 'count' records from the BTree.
func (bt *BTree) SampleRandomRecords(count int) []*core.Record {
	length := bt.index.Len()
	if length == 0 {
		return nil
	}

	if count > length {
		count = length
	}

	records := make([]*core.Record, 0, count)
	// Use a map to track visited indices to avoid duplicates
	visited := make(map[int]struct{})

	for len(records) < count {
		// Safety break if we can't find new items (shouldn't happen with logic above but good practice)
		if len(visited) >= length {
			break
		}

		idx := bt.rand.Intn(length)
		if _, exists := visited[idx]; exists {
			continue
		}
		visited[idx] = struct{}{}

		item, ok := bt.index.GetAt(idx)
		if ok && item != nil {
			records = append(records, item.Record)
		}
	}

	return records
}

func (bt *BTree) InsertRecord(key []byte, record *core.Record) bool {
	_, replaced := bt.index.Set(core.NewItem(key, record))
	return replaced
}

func (bt *BTree) Delete(key []byte) bool {
	_, deleted := bt.index.Delete(core.NewItem[core.Record](key, nil))
	return deleted
}

// All returns all non-expired records in the BTree.
func (bt *BTree) All() []*core.Record {
	return bt.scan().Collect()
}

// AllItems returns all non-expired items in the BTree.
func (bt *BTree) AllItems() []*core.Item[core.Record] {
	return bt.scan().CollectItems()
}

// Range returns records in the specified key range, filtering expired ones.
func (bt *BTree) Range(start, end []byte) []*core.Record {
	return bt.scan().From(start).To(end).Collect()
}

// PrefixScan returns records with the specified prefix, filtering expired ones.
func (bt *BTree) PrefixScan(prefix []byte, offset, limitNum int) []*core.Record {
	return bt.scan().Prefix(prefix).Skip(offset).Take(limitNum).Collect()
}

// PrefixSearchScan returns records with the specified prefix matching the regex.
func (bt *BTree) PrefixSearchScan(prefix []byte, reg string, offset, limitNum int) []*core.Record {
	return bt.scan().Prefix(prefix).Match(reg).Skip(offset).Take(limitNum).Collect()
}

func (bt *BTree) Count() int {
	return bt.index.Len()
}

// PopMin removes and returns the minimum key record, filtering expired ones.
func (bt *BTree) PopMin() (*core.Item[core.Record], bool) {
	return bt.popUntilValid(bt.index.PopMin)
}

// PopMax removes and returns the maximum key record, filtering expired ones.
func (bt *BTree) PopMax() (*core.Item[core.Record], bool) {
	return bt.popUntilValid(bt.index.PopMax)
}

// popUntilValid pops items until finding a valid (non-expired) one.
func (bt *BTree) popUntilValid(pop func() (*core.Item[core.Record], bool)) (*core.Item[core.Record], bool) {
	for {
		item, ok := pop()
		if !ok {
			return nil, false
		}
		if bt.isValid(item) {
			return item, true
		}
	}
}

// Min returns the minimum key record, filtering expired ones.
func (bt *BTree) Min() (*core.Item[core.Record], bool) {
	return bt.scan().Ascending().First()
}

// Max returns the maximum key record, filtering expired ones.
func (bt *BTree) Max() (*core.Item[core.Record], bool) {
	return bt.scan().Descending().First()
}

func (bt *BTree) Iter() btree.IterG[*core.Item[core.Record]] {
	return bt.index.Iter()
}

// GetTTL returns the remaining TTL for a key in seconds.
// Returns (-1, nil) for persistent, (remaining, nil) for valid, (0, ErrKeyNotFound) for expired/missing.
// Returns (0, ErrKeyNotFound) if TTL checking is disabled.
func (bt *BTree) GetTTL(key []byte) (int64, error) {
	if bt.ttlChecker == nil {
		return 0, ErrKeyNotFound
	}
	if item, ok := bt.getValidItem(key); ok {
		return bt.ttlChecker.CalculateRemainingTTL(item.Record.TTL, item.Record.Timestamp), nil
	}
	return 0, ErrKeyNotFound
}

// IsExpiredKey checks if a key exists and is expired.
// Returns false if TTL checking is disabled.
func (bt *BTree) IsExpiredKey(key []byte) bool {
	if bt.ttlChecker == nil {
		return false
	}
	if item, ok := bt.getItem(key); ok {
		return bt.ttlChecker.IsExpired(item.Record.TTL, item.Record.Timestamp)
	}
	return false
}

// Ensure BTreeScanner implements Scanner interface.
var _ Scanner = (*BTreeScanner)(nil)

// BTreeScanner provides a fluent API for building BTree scan operations.
type BTreeScanner struct {
	bt         *BTree
	ttlChecker *ttl.Checker
	ds         uint16

	// scan parameters
	direction ScanDirection
	pivot     []byte
	prefix    []byte
	startKey  []byte
	endKey    []byte
	regex     *regexp.Regexp
	offset    int
	limit     int
	filter    func(*core.Item[core.Record]) bool
	skipTTL   bool
}

// newBTreeScanner creates a new BTreeScanner for the given BTree.
func newBTreeScanner(bt *BTree) *BTreeScanner {
	return &BTreeScanner{
		bt:         bt,
		ttlChecker: bt.ttlChecker,
		ds:         core.DataStructureBTree,
		direction:  Forward,
		limit:      -1, // no limit by default
	}
}

// Direction sets the scan direction (Forward or Reverse).
func (b *BTreeScanner) Direction(d ScanDirection) Scanner {
	b.direction = d
	return b
}

// Ascending sets forward iteration direction.
func (b *BTreeScanner) Ascending() Scanner {
	b.direction = Forward
	return b
}

// Descending sets reverse iteration direction.
func (b *BTreeScanner) Descending() Scanner {
	b.direction = Reverse
	return b
}

// From sets the starting key for the scan (inclusive).
func (b *BTreeScanner) From(key []byte) Scanner {
	b.pivot = key
	b.startKey = key
	return b
}

// To sets the ending key for the scan (inclusive).
func (b *BTreeScanner) To(key []byte) Scanner {
	b.endKey = key
	return b
}

// Prefix sets a key prefix filter.
func (b *BTreeScanner) Prefix(prefix []byte) Scanner {
	b.prefix = prefix
	if b.pivot == nil {
		b.pivot = prefix
	}
	return b
}

// Match sets a regex pattern to match against keys (after prefix removal if prefix is set).
func (b *BTreeScanner) Match(pattern string) Scanner {
	b.regex = regexp.MustCompile(pattern)
	return b
}

// Skip sets the number of matching records to skip.
func (b *BTreeScanner) Skip(n int) Scanner {
	b.offset = n
	return b
}

// Take sets the maximum number of records to return.
func (b *BTreeScanner) Take(n int) Scanner {
	b.limit = n
	return b
}

// Where adds a custom filter predicate.
func (b *BTreeScanner) Where(fn func(*core.Item[core.Record]) bool) Scanner {
	b.filter = fn
	return b
}

// IncludeExpired disables TTL filtering (includes expired records).
func (b *BTreeScanner) IncludeExpired() Scanner {
	b.skipTTL = true
	return b
}

// WithDataStructure sets the data structure type for TTL callback.
func (b *BTreeScanner) WithDataStructure(ds uint16) Scanner {
	b.ds = ds
	return b
}

// checkItem validates an item against all filters and returns the iteration result.
func (b *BTreeScanner) checkItem(item *core.Item[core.Record]) iterResult {
	// Range/Prefix checks first - can terminate iteration early
	if !b.inRange(item) {
		return iterStop
	}

	if b.prefix != nil && !bytes.HasPrefix(item.Key, b.prefix) {
		if b.direction == Forward {
			return iterStop
		}
		return iterContinue
	}

	// TTL check
	if !b.skipTTL && !b.isValid(item) {
		return iterContinue
	}

	// Regex check
	if b.regex != nil {
		key := item.Key
		if b.prefix != nil {
			key = bytes.TrimPrefix(key, b.prefix)
		}
		if !b.regex.Match(key) {
			return iterContinue
		}
	}

	// Custom filter
	if b.filter != nil && !b.filter(item) {
		return iterContinue
	}

	return iterMatch
}

// Collect executes the scan and returns matching records.
func (b *BTreeScanner) Collect() []*core.Record {
	items := b.CollectItems()
	records := make([]*core.Record, len(items))
	for i, item := range items {
		records[i] = item.Record
	}
	return records
}

// CollectItems executes the scan and returns matching items (with keys).
func (b *BTreeScanner) CollectItems() []*core.Item[core.Record] {
	results := make([]*core.Item[core.Record], 0)
	offset := b.offset
	limit := b.limit

	b.buildIterator()(func(item *core.Item[core.Record]) bool {
		switch b.checkItem(item) {
		case iterStop:
			return false
		case iterContinue:
			return true
		}

		if offset > 0 {
			offset--
			return true
		}

		results = append(results, item)

		if limit > 0 {
			limit--
			return limit != 0
		}
		return true
	})

	return results
}

// First returns the first matching record.
func (b *BTreeScanner) First() (*core.Item[core.Record], bool) {
	b.limit = 1
	items := b.CollectItems()
	if len(items) == 0 {
		return nil, false
	}
	return items[0], true
}

// Count returns the number of matching records.
func (b *BTreeScanner) Count() int {
	count := 0
	b.buildIterator()(func(item *core.Item[core.Record]) bool {
		switch b.checkItem(item) {
		case iterStop:
			return false
		case iterContinue:
			return true
		}
		count++
		return true
	})
	return count
}

// ForEach iterates over matching records without collecting them.
func (b *BTreeScanner) ForEach(fn func(*core.Item[core.Record]) bool) {
	offset := b.offset
	limit := b.limit

	b.buildIterator()(func(item *core.Item[core.Record]) bool {
		switch b.checkItem(item) {
		case iterStop:
			return false
		case iterContinue:
			return true
		}

		if offset > 0 {
			offset--
			return true
		}

		if limit > 0 {
			limit--
			if limit == 0 {
				fn(item)
				return false
			}
		}
		return fn(item)
	})
}

// buildIterator returns the appropriate btree iterator based on direction and pivot.
func (b *BTreeScanner) buildIterator() func(func(*core.Item[core.Record]) bool) {
	pivot := b.pivot

	if b.direction == Reverse {
		if pivot != nil {
			return func(fn func(*core.Item[core.Record]) bool) {
				b.bt.index.Descend(&core.Item[core.Record]{Key: pivot}, fn)
			}
		}
		return b.bt.index.Reverse
	}

	// Forward
	if pivot != nil {
		return func(fn func(*core.Item[core.Record]) bool) {
			b.bt.index.Ascend(&core.Item[core.Record]{Key: pivot}, fn)
		}
	}
	return b.bt.index.Scan
}

// isValid checks if an item is not expired.
func (b *BTreeScanner) isValid(item *core.Item[core.Record]) bool {
	if b.ttlChecker == nil || item.Record == nil {
		return true
	}
	return b.ttlChecker.FilterExpiredRecord(b.bt.bucketId, item.Key, item.Record, b.ds)
}

// inRange checks if an item is within the specified key range.
func (b *BTreeScanner) inRange(item *core.Item[core.Record]) bool {
	if b.direction == Forward {
		return (b.startKey == nil || bytes.Compare(item.Key, b.startKey) >= 0) &&
			(b.endKey == nil || bytes.Compare(item.Key, b.endKey) <= 0)
	}
	// Reverse: item should be >= endKey (lower bound) and <= startKey (upper bound/pivot)
	return (b.endKey == nil || bytes.Compare(item.Key, b.endKey) >= 0) &&
		(b.startKey == nil || bytes.Compare(item.Key, b.startKey) <= 0)
}
