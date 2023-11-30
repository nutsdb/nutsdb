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
	"time"
)

func (tx *Tx) PutWithTimestamp(bucket string, key, value []byte, ttl uint32, timestamp uint64) error {
	return tx.put(bucket, key, value, ttl, DataSetFlag, timestamp, DataStructureBTree)
}

// Put sets the value for a key in the bucket.
// a wrapper of the function put.
func (tx *Tx) Put(bucket string, key, value []byte, ttl uint32) error {
	return tx.put(bucket, key, value, ttl, DataSetFlag, uint64(time.Now().UnixMilli()), DataStructureBTree)
}

// Get retrieves the value for a key in the bucket.
// The returned value is only valid for the life of the transaction.
func (tx *Tx) Get(bucket string, key []byte) (value []byte, err error) {
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
