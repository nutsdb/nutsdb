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
	return tx.updateIfExists(bucket, key, value, ttl, false)
}

func (tx *Tx) updateIfExists(bucket string, key, value []byte, ttl uint32, updateTTLOnly bool) error {
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
		if updateTTLOnly {
			return tx.put(bucket, key, record.Value, ttl, DataSetFlag, uint64(time.Now().UnixMilli()), DataStructureBTree)
		}
		return tx.put(bucket, key, value, ttl, DataSetFlag, uint64(time.Now().UnixMilli()), DataStructureBTree)
	}

	return nil
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
func (tx *Tx) GetAll(bucket string) (values [][]byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return nil, err
	}
	bucketId := b.Id

	if index, ok := tx.db.Index.bTree.exist(bucketId); ok {
		records := index.All()

		if len(records) == 0 {
			return nil, ErrBucketEmpty
		}

		values, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, bucketId)
		if err != nil {
			return nil, ErrBucketEmpty
		}
	}

	if len(values) == 0 {
		return nil, ErrBucketEmpty
	}

	return
}

func (tx *Tx) GetSet(bucket string, key, value []byte) (oldValue []byte, err error) {
	return oldValue, tx.update(bucket, key, func(b []byte) ([]byte, error) {
		oldValue = b
		return value, nil
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

		values, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, bucketId)
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
		values, err = tx.getHintIdxDataItemsWrapper(records, limitNum, bucketId)
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
		values, err = tx.getHintIdxDataItemsWrapper(records, limitNum, bucketId)
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

// getHintIdxDataItemsWrapper returns wrapped entries when prefix scanning or range scanning.
func (tx *Tx) getHintIdxDataItemsWrapper(records []*Record, limitNum int, bucketId BucketId) (values [][]byte, err error) {
	for _, record := range records {
		bucket, err := tx.db.bm.GetBucketById(bucketId)
		if err != nil {
			return nil, err
		}
		if record.IsExpired() {
			tx.putDeleteLog(bucket.Id, record.Key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
			continue
		}
		if limitNum > 0 && len(values) < limitNum || limitNum == ScanNoLimit {
			value, err := tx.db.getValueByRecord(record)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
	}

	return values, nil
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

func (tx *Tx) update(bucket string, key []byte, getNewValue func([]byte) ([]byte, error)) error {
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

		return tx.put(bucket, key, newValue, record.TTL, DataSetFlag, uint64(time.Now().Unix()), DataStructureBTree)
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
func (tx *Tx) GetTTL(bucket string, key []byte) (int64, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return 0, err
	}

	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return 0, err
	}
	bucketId := b.Id

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

	now := time.UnixMilli(time.Now().UnixMilli())
	expireTime := time.UnixMilli(int64(record.Timestamp))
	expireTime = expireTime.Add(time.Duration(record.TTL) * time.Second)

	remTTL := expireTime.Sub(now).Seconds()
	if remTTL >= 0 {
		return int64(remTTL), nil
	} else {
		return 0, ErrKeyNotFound
	}
}

// Persist updates record's TTL as Persistent if the record exits.
func (tx *Tx) Persist(bucket string, key []byte) error {
	return tx.updateIfExists(bucket, key, []byte{}, Persistent, true)
}
