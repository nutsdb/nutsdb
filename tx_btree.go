// Copyright 2019 The nutsdb Author. All rights reserved.
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
	"errors"
	"math"
	"math/big"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/nutsdb/nutsdb/internal/data"
	"github.com/xujiajun/utils/strconv2"
)

const (
	getAllType    uint8 = 0
	getKeysType   uint8 = 1
	getValuesType uint8 = 2
)

func (tx *Tx) PutWithTimestamp(bucket string, key, value []byte, ttl uint32, timestamp uint64) error {
	return tx.put(bucket, key, value, ttl, DataSetFlag, timestamp, DataStructureBTree)
}

// Put sets the value for a key in the bucket.
// a wrapper of the function put.
func (tx *Tx) Put(bucket string, key, value []byte, ttl uint32) error {
	return tx.put(bucket, key, value, ttl, DataSetFlag, uint64(time.Now().UnixMilli()), DataStructureBTree)
}

// PutIfNotExists set the value for a key in the bucket only if the key doesn't exist already.
func (tx *Tx) PutIfNotExists(bucket string, key, value []byte, ttl uint32) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	status, b := tx.getBucketAndItsStatus(DataStructureBTree, bucket)
	if isBucketNotFoundStatus(status) {
		return ErrNotFoundBucket
	}
	bucketId := b.Id
	_, err := tx.pendingWrites.Get(DataStructureBTree, bucket, key)
	if err == nil {
		// the key-value is exists.
		return nil
	}

	idx, bucketExists := tx.db.Index.bTree.exist(bucketId)
	if !bucketExists {
		return tx.put(bucket, key, value, ttl, DataSetFlag, uint64(time.Now().UnixMilli()), DataStructureBTree)
	}
	record, recordExists := idx.Find(key)

	if recordExists && !record.IsExpired() {
		return nil
	}

	return tx.put(bucket, key, value, ttl, DataSetFlag, uint64(time.Now().UnixMilli()), DataStructureBTree)
}

// PutIfExists set the value for a key in the bucket only if the key already exits.
func (tx *Tx) PutIfExists(bucket string, key, value []byte, ttl uint32) error {
	return tx.update(bucket, key, func(_ []byte) ([]byte, error) {
		return value, nil
	}, func(_ uint32) (uint32, error) {
		return ttl, nil
	})
}

// Get retrieves the value for a key in the bucket.
// The returned value is only valid for the life of the transaction.
func (tx *Tx) Get(bucket string, key []byte) (value []byte, err error) {
	return tx.get(bucket, key)
}

func (tx *Tx) get(bucket string, key []byte) (value []byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	status, b := tx.getBucketAndItsStatus(DataStructureBTree, bucket)
	if isBucketNotFoundStatus(status) {
		return nil, ErrNotFoundBucket
	}
	bucketId := b.Id

	status, entry := tx.findEntryAndItsStatus(DataStructureBTree, bucket, string(key))
	switch status {
	case EntryDeleted:
		return nil, ErrKeyNotFound
	case EntryUpdated:
		return entry.Value, nil
	}

	if idx, ok := tx.db.Index.bTree.exist(bucketId); ok {
		record, found := idx.Find(key)
		if !found {
			return nil, ErrKeyNotFound
		}

		if record.IsExpired() {
			tx.putDeleteLog(bucketId, key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
			return nil, ErrNotFoundKey
		}

		value, err = tx.db.getValueByRecord(record)
		if err != nil {
			return nil, err
		}
		return value, nil

	} else {
		return nil, ErrKeyNotFound
	}
}

func (tx *Tx) ValueLen(bucket string, key []byte) (int, error) {
	value, err := tx.get(bucket, key)
	return len(value), err
}

func (tx *Tx) GetMaxKey(bucket string) ([]byte, error) {
	return tx.getMaxOrMinKey(bucket, true)
}

func (tx *Tx) GetMinKey(bucket string) ([]byte, error) {
	return tx.getMaxOrMinKey(bucket, false)
}

func (tx *Tx) getMaxOrMinKey(bucket string, isMax bool) ([]byte, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	status, b := tx.getBucketAndItsStatus(DataStructureBTree, bucket)
	if isBucketNotFoundStatus(status) {
		return nil, ErrNotFoundBucket
	}
	bucketId := b.Id

	var (
		key           []byte = nil
		actuallyFound        = false
	)

	key, actuallyFound = tx.pendingWrites.MaxOrMinKey(bucket, isMax)

	if idx, ok := tx.db.Index.bTree.exist(bucketId); ok {
		var (
			item  *data.Item[data.Record]
			found bool
		)

		if isMax {
			item, found = idx.Max()
		} else {
			item, found = idx.Min()
		}

		if !found {
			if actuallyFound {
				return key, nil
			}
			return nil, ErrKeyNotFound
		} else {
			actuallyFound = found
		}

		if item.Record.IsExpired() {
			tx.putDeleteLog(bucketId, item.Key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
			if actuallyFound {
				return key, nil
			}
			return nil, ErrKeyNotFound
		}
		if isMax {
			key = compareAndReturn(key, item.Key, 1)
		} else {
			key = compareAndReturn(key, item.Key, -1)
		}
	}
	if actuallyFound {
		return key, nil
	}
	return nil, ErrKeyNotFound
}

// GetAll returns all keys and values in the given bucket.
func (tx *Tx) GetAll(bucket string) ([][]byte, [][]byte, error) {
	return tx.getAllOrKeysOrValues(bucket, getAllType)
}

// GetKeys returns all keys in the given bucket.
func (tx *Tx) GetKeys(bucket string) ([][]byte, error) {
	keys, _, err := tx.getAllOrKeysOrValues(bucket, getKeysType)
	return keys, err
}

// GetValues returns all values in the given bucket.
func (tx *Tx) GetValues(bucket string) ([][]byte, error) {
	_, values, err := tx.getAllOrKeysOrValues(bucket, getValuesType)
	return values, err
}

func (tx *Tx) getAllOrKeysOrValues(bucket string, typ uint8) ([][]byte, [][]byte, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, nil, err
	}

	bucketId, err := tx.db.bm.GetBucketID(DataStructureBTree, bucket)
	if err != nil {
		return nil, nil, err
	}

	idx, bucketExists := tx.db.Index.bTree.exist(bucketId)
	if !bucketExists {
		return [][]byte{}, [][]byte{}, nil
	}

	records := idx.All()

	var (
		keys   [][]byte
		values [][]byte
	)

	switch typ {
	case getAllType:
		keys, values, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, bucketId, true, true)
	case getKeysType:
		keys, _, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, bucketId, true, false)
	case getValuesType:
		_, values, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, bucketId, false, true)
	}

	if err != nil {
		return nil, nil, err
	}

	return keys, values, nil
}

func (tx *Tx) GetSet(bucket string, key, value []byte) (oldValue []byte, err error) {
	return oldValue, tx.update(bucket, key, func(b []byte) ([]byte, error) {
		oldValue = b
		return value, nil
	}, func(oldTTL uint32) (uint32, error) {
		return oldTTL, nil
	})
}

// Has returns true if the record exists. It checks without retrieving the
// value from disk making lookups significantly faster while keeping memory
// usage down as well. It does require the `HintKeyAndRAMIdxMode` option to be
// enabled to function as described.
func (tx *Tx) Has(bucket string, key []byte) (exists bool, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return false, err
	}

	status, b := tx.getBucketAndItsStatus(DataStructureBTree, bucket)
	if isBucketNotFoundStatus(status) {
		return false, ErrNotFoundBucket
	}
	bucketId := b.Id

	status, _ = tx.findEntryAndItsStatus(DataStructureBTree, bucket, string(key))
	switch status {
	case EntryDeleted:
		return false, ErrKeyNotFound
	case EntryUpdated:
		return true, nil
	}

	idx, bucketExists := tx.db.Index.bTree.exist(bucketId)
	if !bucketExists {
		return false, ErrBucketNotExist
	}
	record, recordExists := idx.Find(key)

	if recordExists && !record.IsExpired() {
		return true, nil
	}

	return false, nil
}

// RangeScanEntries query a range at given bucket, start and end slice. It will
// return keys and/or values based on the includeKeys and includeValues flags.
func (tx *Tx) RangeScanEntries(bucket string, start, end []byte, includeKeys, includeValues bool) (keys, values [][]byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, nil, err
	}
	status, b := tx.getBucketAndItsStatus(DataStructureBTree, bucket)
	if isBucketNotFoundStatus(status) {
		return nil, nil, ErrNotFoundBucket
	}
	bucketId := b.Id
	pendingKeys, pendingValues := tx.pendingWrites.getDataByRange(start, end, b.Name)
	if index, ok := tx.db.Index.bTree.exist(bucketId); ok {
		records := index.Range(start, end)

		// 如果需要合并 pending，则必须拿到 keys 进行有序 merge；
		// 否则可以按需提取，避免不必要的 keys 分配。
		needKeysForMerge := includeKeys || len(pendingKeys) > 0

		keys, values, err = tx.getHintIdxDataItemsWrapper(
			records, ScanNoLimit, bucketId, needKeysForMerge, includeValues,
		)
		if err != nil && len(pendingKeys) == 0 && len(pendingValues) == 0 {
			// If there is no item in pending and persist db,
			// return error itself.
			return nil, nil, err
		}
	}

	// 仅当存在 pending 时才进行 merge，避免 values-only 且无 pending 的场景强制构建 keys。
	if len(pendingKeys) > 0 || len(pendingValues) > 0 {
		keys, values = mergeKeyValues(pendingKeys, pendingValues, keys, values)
	}

	// Check for empty results before setting to nil
	if includeKeys && len(keys) == 0 {
		return nil, nil, ErrRangeScan
	}
	if includeValues && len(values) == 0 {
		return nil, nil, ErrRangeScan
	}

	if !includeKeys {
		keys = nil
	}
	if !includeValues {
		values = nil
	}

	return
}

// RangeScan query a range at given bucket, start and end slice.
func (tx *Tx) RangeScan(bucket string, start, end []byte) (values [][]byte, err error) {
	// RangeScan is kept as an API call to not break upstream projects that
	// rely on it.
	_, values, err = tx.RangeScanEntries(bucket, start, end, false, true)
	return
}

// PrefixScanEntries iterates over a key prefix at given bucket, prefix and
// limitNum.  If reg is set a regular expression will be used to filter the
// found entries. LimitNum will limit the number of entries return. It will
// return keys and/or values based on the includeKeys and includeValues flags.
func (tx *Tx) PrefixScanEntries(bucket string, prefix []byte, reg string, offsetNum int, limitNum int, includeKeys, includeValues bool) (keys, values [][]byte, err error) {
	// This function is a bit awkward but that is to maintain backwards
	// compatibility while enabling the caller to pick and choose which
	// variation to call.
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, nil, err
	}
	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return nil, nil, err
	}
	bucketId := b.Id

	xerr := func(e error) error {
		// Return expected error types based on Scan/SearchScan.
		if reg == "" {
			return ErrPrefixScan
		}
		return ErrPrefixSearchScan
	}

	if idx, ok := tx.db.Index.bTree.exist(bucketId); ok {
		var records []*data.Record
		if reg == "" {
			records = idx.PrefixScan(prefix, offsetNum, limitNum)
		} else {
			records = idx.PrefixSearchScan(prefix, reg, offsetNum, limitNum)
		}
		keys, values, err = tx.getHintIdxDataItemsWrapper(records, limitNum, bucketId, includeKeys, includeValues)
		if err != nil {
			return nil, nil, xerr(err)
		}
	}

	if includeKeys && len(keys) == 0 {
		return nil, nil, xerr(err)
	}
	if includeValues && len(values) == 0 {
		return nil, nil, xerr(err)
	}
	return
}

// PrefixScan iterates over a key prefix at given bucket, prefix and limitNum.
// LimitNum will limit the number of entries return.
func (tx *Tx) PrefixScan(bucket string, prefix []byte, offsetNum int, limitNum int) (values [][]byte, err error) {
	// PrefixScan is kept as an API call to not break upstream projects
	// that rely on it.
	_, values, err = tx.PrefixScanEntries(bucket, prefix, "", offsetNum, limitNum, false, true)
	return values, err
}

// PrefixSearchScan iterates over a key prefix at given bucket, prefix, match regular expression and limitNum.
// LimitNum will limit the number of entries return.
func (tx *Tx) PrefixSearchScan(bucket string, prefix []byte, reg string, offsetNum int, limitNum int) (values [][]byte, err error) {
	// PrefixSearchScan is kept as an API call to not break upstream projects
	// that rely on it.
	_, values, err = tx.PrefixScanEntries(bucket, prefix, reg, offsetNum, limitNum, false, true)
	return values, err
}

// Delete removes a key from the bucket at given bucket and key.
func (tx *Tx) Delete(bucket string, key []byte) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}
	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return err
	}
	bucketId := b.Id

	//Find the key in the transaction-local map (entriesInBTree)
	status, _ := tx.findEntryAndItsStatus(DataStructureBTree, bucket, string(key))
	switch status {
	case EntryDeleted:
		return ErrKeyNotFound
	case EntryUpdated:
		return tx.put(bucket, key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
	}

	if idx, ok := tx.db.Index.bTree.exist(bucketId); ok {
		if _, found := idx.Find(key); !found {
			return ErrKeyNotFound
		}
	} else {
		return ErrKeyNotFound
	}

	return tx.put(bucket, key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
}

// getHintIdxDataItemsWrapper returns keys and values when prefix scanning or range scanning.
func (tx *Tx) getHintIdxDataItemsWrapper(records []*data.Record, limitNum int, bucketId BucketId, needKeys bool, needValues bool) (keys [][]byte, values [][]byte, err error) {
	// Pre-allocate capacity to reduce slice re-growth
	estimatedSize := len(records)
	if limitNum > 0 && limitNum < estimatedSize {
		estimatedSize = limitNum
	}

	if needKeys {
		keys = make([][]byte, 0, estimatedSize)
	}
	if needValues {
		values = make([][]byte, 0, estimatedSize)
	}

	processedCount := 0
	needAny := needKeys || needValues

	for _, record := range records {
		if record.IsExpired() {
			tx.putDeleteLog(bucketId, record.Key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
			continue
		}

		// 正确的 limit 控制：与是否需要 keys/values 无关
		if limitNum > 0 && needAny && processedCount >= limitNum {
			break
		}

		if needKeys {
			keys = append(keys, record.Key)
		}
		if needValues {
			value, err := tx.db.getValueByRecord(record)
			if err != nil {
				return nil, nil, err
			}
			values = append(values, value)
		}

		if needAny {
			processedCount++
		}
	}

	return keys, values, nil
}

func (tx *Tx) tryGet(bucket string, key []byte, solveRecord func(record *data.Record, entry *Entry, found bool, bucketId BucketId) error) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}
	status, b := tx.getBucketAndItsStatus(DataStructureBTree, bucket)
	if isBucketNotFoundStatus(status) {
		return ErrNotFoundBucket
	}
	bucketId := b.Id
	var (
		record *data.Record = nil
		found  bool
	)
	entry, err := tx.pendingWrites.Get(DataStructureBTree, bucket, key)
	found = err == nil
	if idx, ok := tx.db.Index.bTree.exist(bucketId); ok {
		record, ok = idx.Find(key)
		if ok {
			found = true
		}
	}
	return solveRecord(record, entry, found, bucketId)
}

func (tx *Tx) update(bucket string, key []byte, getNewValue func([]byte) ([]byte, error), getNewTTL func(uint32) (uint32, error)) error {
	return tx.tryGet(bucket, key, func(record *data.Record, pendingEntry *Entry, found bool, bucketId BucketId) error {
		if !found {
			return ErrKeyNotFound
		}

		if record != nil {
			// If record is timeout, this entry should not be update to table.
			if err := tx.revertExpiredTTLRecord(bucketId, record); err != nil {
				return err
			}
		}

		value, ttl, err := tx.loadValue(record, pendingEntry)
		if err != nil {
			return err
		}

		newValue, err := getNewValue(value)
		if err != nil {
			return err
		}

		newTTL, err := getNewTTL(ttl)
		if err != nil {
			return err
		}
		return tx.put(bucket, key, newValue, newTTL, DataSetFlag, uint64(time.Now().UnixMilli()), DataStructureBTree)
	})
}

func (tx *Tx) updateOrPut(bucket string, key, value []byte, getUpdatedValue func([]byte) ([]byte, error)) error {
	return tx.tryGet(bucket, key, func(record *data.Record, pendingEntry *Entry, found bool, bucketId BucketId) error {
		if !found {
			return tx.put(bucket, key, value, Persistent, DataSetFlag, uint64(time.Now().Unix()), DataStructureBTree)
		}

		if record != nil {
			if err := tx.revertExpiredTTLRecord(bucketId, record); err != nil {
				return err
			}
		}
		value, ttl, err := tx.loadValue(record, pendingEntry)
		if err != nil {
			return err
		}
		newValue, err := getUpdatedValue(value)
		if err != nil {
			return err
		}

		return tx.put(bucket, key, newValue, ttl, DataSetFlag, uint64(time.Now().Unix()), DataStructureBTree)
	})
}

func bigIntIncr(a string, b string) string {
	bigIntA, _ := new(big.Int).SetString(a, 10)
	bigIntB, _ := new(big.Int).SetString(b, 10)
	bigIntA.Add(bigIntA, bigIntB)
	return bigIntA.String()
}

func (tx *Tx) integerIncr(bucket string, key []byte, increment int64) error {
	return tx.update(bucket, key, func(value []byte) ([]byte, error) {
		intValue, err := strconv2.StrToInt64(string(value))

		if err != nil && errors.Is(err, strconv.ErrRange) {
			return []byte(bigIntIncr(string(value), strconv2.Int64ToStr(increment))), nil
		}

		if err != nil {
			return nil, ErrValueNotInteger
		}

		if (increment > 0 && math.MaxInt64-increment < intValue) || (increment < 0 && math.MinInt64-increment > intValue) {
			return []byte(bigIntIncr(string(value), strconv2.Int64ToStr(increment))), nil
		}

		atomic.AddInt64(&intValue, increment)
		return []byte(strconv2.Int64ToStr(intValue)), nil
	}, func(oldTTL uint32) (uint32, error) {
		return oldTTL, nil
	})
}

func (tx *Tx) Incr(bucket string, key []byte) error {
	return tx.integerIncr(bucket, key, 1)
}

func (tx *Tx) Decr(bucket string, key []byte) error {
	return tx.integerIncr(bucket, key, -1)
}

func (tx *Tx) IncrBy(bucket string, key []byte, increment int64) error {
	return tx.integerIncr(bucket, key, increment)
}

func (tx *Tx) DecrBy(bucket string, key []byte, decrement int64) error {
	return tx.integerIncr(bucket, key, -1*decrement)
}

func (tx *Tx) GetBit(bucket string, key []byte, offset int) (byte, error) {
	if offset >= math.MaxInt || offset < 0 {
		return 0, ErrOffsetInvalid
	}

	value, err := tx.Get(bucket, key)
	if err != nil {
		return 0, err
	}

	if len(value) <= offset {
		return 0, nil
	}

	return value[offset], nil
}

func (tx *Tx) SetBit(bucket string, key []byte, offset int, bit byte) error {
	if offset >= math.MaxInt || offset < 0 {
		return ErrOffsetInvalid
	}

	valueIfKeyNotFound := make([]byte, offset+1)
	valueIfKeyNotFound[offset] = bit

	return tx.updateOrPut(bucket, key, valueIfKeyNotFound, func(value []byte) ([]byte, error) {
		if len(value) <= offset {
			value = append(value, make([]byte, offset-len(value)+1)...)
			value[offset] = bit
			return value, nil
		} else {
			value[offset] = bit
			return value, nil
		}
	})
}

// GetTTL returns remaining TTL of a value by key.
// It returns
// (-1, nil) If TTL is Persistent
// (0, ErrBucketNotFound|ErrKeyNotFound) If expired or not found
// (TTL, nil) If the record exists with a TTL
// Note: The returned remaining TTL will be in seconds. For example,
// remainingTTL is 500ms, It'll return 0.
func (tx *Tx) GetTTL(bucket string, key []byte) (int64, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return 0, err
	}

	status, b := tx.getBucketAndItsStatus(DataStructureBTree, bucket)
	if isBucketNotFoundStatus(status) {
		return 0, ErrBucketNotFound
	}
	bucketId := b.Id

	idx, bucketExists := tx.db.Index.bTree.exist(bucketId)

	var (
		record      *data.Record = nil
		recordFound bool         = false
	)
	if pendingTTL, err := tx.pendingWrites.GetTTL(DataStructureBTree, bucket, key); err == nil {
		return pendingTTL, nil
	}
	if bucketExists {
		record, recordFound = idx.Find(key)
	}
	if !recordFound || record.IsExpired() {
		return 0, ErrKeyNotFound
	}

	if record.TTL == Persistent {
		return -1, nil
	}

	remTTL := expireTime(record.Timestamp, record.TTL)
	if remTTL >= 0 {
		return int64(remTTL.Seconds()), nil
	} else {
		return 0, ErrKeyNotFound
	}
}

// Persist updates record's TTL as Persistent if the record exists.
func (tx *Tx) Persist(bucket string, key []byte) error {
	return tx.update(bucket, key, func(oldValue []byte) ([]byte, error) {
		return oldValue, nil
	}, func(_ uint32) (uint32, error) {
		return Persistent, nil
	})
}

func (tx *Tx) MSet(bucket string, ttl uint32, args ...[]byte) error {
	if len(args) == 0 {
		return nil
	}

	if len(args)%2 != 0 {
		return ErrKVArgsLenNotEven
	}

	for i := 0; i < len(args); i += 2 {
		if err := tx.put(bucket, args[i], args[i+1], ttl, DataSetFlag, uint64(time.Now().Unix()), DataStructureBTree); err != nil {
			return err
		}
	}

	return nil
}

func (tx *Tx) MGet(bucket string, keys ...[]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	values := make([][]byte, len(keys))
	for i, key := range keys {
		value, err := tx.Get(bucket, key)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}

	return values, nil
}

func (tx *Tx) Append(bucket string, key, appendage []byte) error {
	if len(appendage) == 0 {
		return nil
	}

	return tx.updateOrPut(bucket, key, appendage, func(value []byte) ([]byte, error) {
		return append(value, appendage...), nil
	})
}

func (tx *Tx) GetRange(bucket string, key []byte, start, end int) ([]byte, error) {
	if start > end {
		return nil, ErrStartGreaterThanEnd
	}

	value, err := tx.get(bucket, key)
	if err != nil {
		return nil, err
	}

	if start >= len(value) {
		return nil, nil
	}

	if end >= len(value) {
		return value[start:], nil
	}

	return value[start : end+1], nil
}

// loadValue in order to load value during pending and
// stored items, we need this function to load value and
// TTL.
func (tx *Tx) loadValue(
	rec *data.Record,
	pendingEntry *Entry,
) (value []byte, ttl uint32, err error) {

	if rec == nil && pendingEntry == nil {
		return nil, 0, ErrNotFoundKey
	}
	if pendingEntry != nil {
		return pendingEntry.Value, pendingEntry.Meta.TTL, nil
	}
	return rec.Value, rec.TTL, nil
}

// revertExpiredTTLRecord revert record if it is expired.
func (tx *Tx) revertExpiredTTLRecord(
	bucketId BucketId,
	rec *data.Record,
) (err error) {
	if rec.IsExpired() {
		tx.putDeleteLog(bucketId, rec.Key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
		return ErrNotFoundKey
	}
	return nil
}
