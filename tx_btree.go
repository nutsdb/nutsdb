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
func (tx *Tx) Get(bucket string, key []byte) (e *Entry, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	idxMode := tx.db.opt.EntryIdxMode

	if idx, ok := tx.db.Index.bTree.exist(bucket); ok {
		r, found := idx.Find(key)
		if !found {
			return nil, ErrKeyNotFound
		}

		if r.IsExpired() {
			tx.putDeleteLog(bucket, key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
			return nil, ErrNotFoundKey
		}

		if idxMode == HintKeyValAndRAMIdxMode {
			return NewEntry().WithBucket([]byte(r.Bucket)).WithKey(r.H.Key).WithValue(r.V).WithMeta(r.H.Meta), nil
		}

		if idxMode == HintKeyAndRAMIdxMode {
			e, err = tx.db.getEntryByHint(r.H)
			if err != nil {
				return nil, err
			}
			return e, nil
		}
	} else {
		return nil, ErrNotFoundBucket
	}

	return nil, ErrBucketAndKey(bucket, key)
}

// GetAll returns all keys and values of the bucket stored at given bucket.
func (tx *Tx) GetAll(bucket string) (entries Entries, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	entries = Entries{}

	if index, ok := tx.db.Index.bTree.exist(bucket); ok {
		records := index.All()
		if len(records) == 0 {
			return nil, ErrBucketEmpty
		}

		entries, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, entries)
		if err != nil {
			return nil, ErrBucketEmpty
		}
	}

	if len(entries) == 0 {
		return nil, ErrBucketEmpty
	}

	return
}

// RangeScan query a range at given bucket, start and end slice.
func (tx *Tx) RangeScan(bucket string, start, end []byte) (es Entries, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if index, ok := tx.db.Index.bTree.exist(bucket); ok {
		records := index.Range(start, end)
		if err != nil {
			return nil, ErrRangeScan
		}

		es, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, es)
		if err != nil {
			return nil, ErrRangeScan
		}
	}

	if len(es) == 0 {
		return nil, ErrRangeScan
	}

	return
}

// PrefixScan iterates over a key prefix at given bucket, prefix and limitNum.
// LimitNum will limit the number of entries return.
func (tx *Tx) PrefixScan(bucket string, prefix []byte, offsetNum int, limitNum int) (es Entries, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if idx, ok := tx.db.Index.bTree.exist(bucket); ok {
		records := idx.PrefixScan(prefix, offsetNum, limitNum)
		es, err = tx.getHintIdxDataItemsWrapper(records, limitNum, es)
		if err != nil {
			return nil, ErrPrefixScan
		}
	}

	if len(es) == 0 {
		return nil, ErrPrefixScan
	}

	return
}

// PrefixSearchScan iterates over a key prefix at given bucket, prefix, match regular expression and limitNum.
// LimitNum will limit the number of entries return.
func (tx *Tx) PrefixSearchScan(bucket string, prefix []byte, reg string, offsetNum int, limitNum int) (es Entries, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if idx, ok := tx.db.Index.bTree.exist(bucket); ok {
		records := idx.PrefixSearchScan(prefix, reg, offsetNum, limitNum)
		es, err = tx.getHintIdxDataItemsWrapper(records, limitNum, es)
		if err != nil {
			return nil, ErrPrefixSearchScan
		}
	}

	if len(es) == 0 {
		return nil, ErrPrefixSearchScan
	}

	return
}

// Delete removes a key from the bucket at given bucket and key.
func (tx *Tx) Delete(bucket string, key []byte) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	if idx, ok := tx.db.Index.bTree.exist(bucket); ok {
		if _, found := idx.Find(key); !found {
			return ErrKeyNotFound
		}
	} else {
		return ErrNotFoundBucket
	}

	return tx.put(bucket, key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
}

// getHintIdxDataItemsWrapper returns wrapped entries when prefix scanning or range scanning.
func (tx *Tx) getHintIdxDataItemsWrapper(records []*Record, limitNum int, es Entries) (Entries, error) {
	for _, r := range records {
		if r.IsExpired() {
			tx.putDeleteLog(r.Bucket, r.H.Key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
			continue
		}
		if limitNum > 0 && len(es) < limitNum || limitNum == ScanNoLimit {
			var (
				e   *Entry
				err error
			)
			if tx.db.opt.EntryIdxMode == HintKeyAndRAMIdxMode {
				e, err = tx.db.getEntryByHint(r.H)
				if err != nil {
					return nil, err
				}
			} else {
				e = NewEntry().WithBucket([]byte(r.Bucket)).WithKey(r.H.Key).WithValue(r.V).WithMeta(r.H.Meta)
			}
			es = append(es, e)
		}
	}

	return es, nil
}
