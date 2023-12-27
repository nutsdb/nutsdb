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

	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return err
	}
	bucketId := b.Id

	idx, bucketExists := tx.db.Index.bTree.exist(bucketId)
	if !bucketExists {
		return ErrNotFoundBucket
	}
	record, recordExists := idx.Find(key)

	if recordExists && !record.IsExpired() {
		return nil
	}

	return tx.put(bucket, key, value, ttl, DataSetFlag, uint64(time.Now().UnixMilli()), DataStructureBTree)
}

// PutIfExits set the value for a key in the bucket only if the key already exits.
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

	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return nil, err
	}
	bucketId := b.Id

	bucketStatus := tx.getBucketStatus(DataStructureBTree, bucket)
	if bucketStatus == BucketStatusDeleted {
		return nil, ErrBucketNotFound
	}

	status, entry := tx.findEntryAndItsStatus(DataStructureBTree, bucket, string(key))
	if status != NotFoundEntry && entry != nil {
		if status == EntryDeleted {
			return nil, ErrKeyNotFound
		} else {
			return entry.Value, nil
		}
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
		return nil, ErrNotFoundBucket
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

	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return nil, err
	}
	bucketId := b.Id

	if idx, ok := tx.db.Index.bTree.exist(bucketId); ok {
		var (
			item  *Item
			found bool
		)

		if isMax {
			item, found = idx.Max()
		} else {
			item, found = idx.Min()
		}

		if !found {
			return nil, ErrKeyNotFound
		}

		if item.record.IsExpired() {
			tx.putDeleteLog(bucketId, item.key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
			return nil, ErrNotFoundKey
		}

		return item.key, nil
	} else {
		return nil, ErrNotFoundBucket
	}
}

// GetAll returns all keys and values of the bucket stored at given bucket.
func (tx *Tx) GetAll(bucket string) ([][]byte, [][]byte, error) {
	return tx.getAllOrKeysOrValues(bucket, getAllType)
}

// GetKeys returns all keys of the bucket stored at given bucket.
func (tx *Tx) GetKeys(bucket string) ([][]byte, error) {
	keys, _, err := tx.getAllOrKeysOrValues(bucket, getKeysType)
	return keys, err
}

// GetValues returns all values of the bucket stored at given bucket.
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

	if index, ok := tx.db.Index.bTree.exist(bucketId); ok {
		records := index.All()

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

	return nil, nil, nil
}

func (tx *Tx) GetSet(bucket string, key, value []byte) (oldValue []byte, err error) {
	return oldValue, tx.update(bucket, key, func(b []byte) ([]byte, error) {
		oldValue = b
		return value, nil
	}, func(oldTTL uint32) (uint32, error) {
		return oldTTL, nil
	})
}

// RangeScan query a range at given bucket, start and end slice.
func (tx *Tx) RangeScan(bucket string, start, end []byte) (values [][]byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}
	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return nil, err
	}
	bucketId := b.Id

	if index, ok := tx.db.Index.bTree.exist(bucketId); ok {
		records := index.Range(start, end)
		if err != nil {
			return nil, ErrRangeScan
		}

		_, values, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, bucketId, false, true)
		if err != nil {
			return nil, ErrRangeScan
		}
	}

	if len(values) == 0 {
		return nil, ErrRangeScan
	}

	return
}

// PrefixScan iterates over a key prefix at given bucket, prefix and limitNum.
// LimitNum will limit the number of entries return.
func (tx *Tx) PrefixScan(bucket string, prefix []byte, offsetNum int, limitNum int) (values [][]byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}
	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return nil, err
	}
	bucketId := b.Id

	if idx, ok := tx.db.Index.bTree.exist(bucketId); ok {
		records := idx.PrefixScan(prefix, offsetNum, limitNum)
		_, values, err = tx.getHintIdxDataItemsWrapper(records, limitNum, bucketId, false, true)
		if err != nil {
			return nil, ErrPrefixScan
		}
	}

	if len(values) == 0 {
		return nil, ErrPrefixScan
	}

	return
}

// PrefixSearchScan iterates over a key prefix at given bucket, prefix, match regular expression and limitNum.
// LimitNum will limit the number of entries return.
func (tx *Tx) PrefixSearchScan(bucket string, prefix []byte, reg string, offsetNum int, limitNum int) (values [][]byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}
	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return nil, err
	}
	bucketId := b.Id

	if idx, ok := tx.db.Index.bTree.exist(bucketId); ok {
		records := idx.PrefixSearchScan(prefix, reg, offsetNum, limitNum)
		_, values, err = tx.getHintIdxDataItemsWrapper(records, limitNum, bucketId, false, true)
		if err != nil {
			return nil, ErrPrefixSearchScan
		}
	}

	if len(values) == 0 {
		return nil, ErrPrefixSearchScan
	}

	return
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

	if idx, ok := tx.db.Index.bTree.exist(bucketId); ok {
		if _, found := idx.Find(key); !found {
			return ErrKeyNotFound
		}
	} else {
		return ErrNotFoundBucket
	}

	return tx.put(bucket, key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
}

// getHintIdxDataItemsWrapper returns keys and values when prefix scanning or range scanning.
func (tx *Tx) getHintIdxDataItemsWrapper(records []*Record, limitNum int, bucketId BucketId, needKeys bool, needValues bool) (keys [][]byte, values [][]byte, err error) {
	for _, record := range records {
		if record.IsExpired() {
			tx.putDeleteLog(bucketId, record.Key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
			continue
		}

		if limitNum > 0 && len(values) < limitNum || limitNum == ScanNoLimit {
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
		}
	}

	return keys, values, nil
}

func (tx *Tx) tryGet(bucket string, key []byte, solveRecord func(record *Record, found bool, bucketId BucketId) error) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	bucketId, err := tx.db.bm.GetBucketID(DataStructureBTree, bucket)
	if err != nil {
		return err
	}

	if idx, ok := tx.db.Index.bTree.exist(bucketId); ok {
		record, found := idx.Find(key)
		return solveRecord(record, found, bucketId)
	} else {
		return ErrBucketNotFound
	}
}

func (tx *Tx) update(bucket string, key []byte, getNewValue func([]byte) ([]byte, error), getNewTTL func(uint32) (uint32, error)) error {
	return tx.tryGet(bucket, key, func(record *Record, found bool, bucketId BucketId) error {
		if !found {
			return ErrKeyNotFound
		}

		if record.IsExpired() {
			tx.putDeleteLog(bucketId, key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
			return ErrNotFoundKey
		}

		value, err := tx.db.getValueByRecord(record)
		if err != nil {
			return err
		}
		newValue, err := getNewValue(value)
		if err != nil {
			return err
		}

		newTTL, err := getNewTTL(record.TTL)
		if err != nil {
			return err
		}

		return tx.put(bucket, key, newValue, newTTL, DataSetFlag, uint64(time.Now().Unix()), DataStructureBTree)
	})
}

func (tx *Tx) updateOrPut(bucket string, key, value []byte, getUpdatedValue func([]byte) ([]byte, error)) error {
	return tx.tryGet(bucket, key, func(record *Record, found bool, bucketId BucketId) error {
		if !found {
			return tx.put(bucket, key, value, Persistent, DataSetFlag, uint64(time.Now().Unix()), DataStructureBTree)
		}

		value, err := tx.db.getValueByRecord(record)
		if err != nil {
			return err
		}
		newValue, err := getUpdatedValue(value)
		if err != nil {
			return err
		}

		return tx.put(bucket, key, newValue, record.TTL, DataSetFlag, uint64(time.Now().Unix()), DataStructureBTree)
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

	bucketId, err := tx.db.bm.GetBucketID(DataStructureBTree, bucket)
	if err != nil {
		return 0, err
	}
	idx, bucketExists := tx.db.Index.bTree.exist(bucketId)

	if !bucketExists {
		return 0, ErrBucketNotFound
	}

	record, recordFound := idx.Find(key)

	if !recordFound || record.IsExpired() {
		return 0, ErrKeyNotFound
	}

	if record.TTL == Persistent {
		return -1, nil
	}

	remTTL := tx.db.expireTime(record.Timestamp, record.TTL)
	if remTTL >= 0 {
		return int64(remTTL.Seconds()), nil
	} else {
		return 0, ErrKeyNotFound
	}
}

// Persist updates record's TTL as Persistent if the record exits.
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
