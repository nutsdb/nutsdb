// Copyright 2021 The nutsdb Author. All rights reserved.
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

package inmemory

import (
	"github.com/xujiajun/nutsdb/consts"
	"github.com/xujiajun/nutsdb/errs"
	"github.com/xujiajun/nutsdb/model"
	"time"

	"github.com/xujiajun/nutsdb"
)

func (db *DB) Get(bucket string, key []byte) (*model.Entry, error) {
	var (
		err   error
		entry *model.Entry
	)
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if idx, ok := shardDB.BPTreeIdx[bucket]; ok {
			r, err := idx.Find(key)
			if err != nil {
				return err
			}

			if r.H.Meta.Flag == consts.DataDeleteFlag || r.IsExpired() {
				return errs.ErrNotFoundKey
			}

			entry = r.E
		} else if !ok {
			return errs.ErrBucket
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	if entry != nil {
		return entry, nil
	}

	return nil, errs.ErrKeyNotFound
}

func (db *DB) Put(bucket string, key, value []byte, ttl uint32) (err error) {
	err = db.Managed(bucket, true, func(shardDB *ShardDB) error {
		return put(shardDB, bucket, key, value, ttl, consts.DataSetFlag)
	})

	return
}

// Delete removes a key from the bucket at given bucket and key.
func (db *DB) Delete(bucket string, key []byte) (err error) {
	err = db.Managed(bucket, true, func(shardDB *ShardDB) error {
		return put(shardDB, bucket, key, nil, consts.Persistent, consts.DataDeleteFlag)
	})
	return
}

// Range query a range at given bucket, start and end slice.
func (db *DB) Range(bucket string, start, end []byte, f func(key, value []byte) bool) (err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if index, ok := shardDB.BPTreeIdx[bucket]; ok {
			index.FindRange(start, end, func(key []byte, pointer interface{}) bool {
				record := pointer.(*model.Record)
				if record.E.Meta.Flag != consts.DataDeleteFlag && !record.IsExpired() {
					return f(key, record.E.Value)
				}
				return true
			})
		}
		return nil
	})
	return
}

// AllKeys list all key of bucket.
func (db *DB) AllKeys(bucket string) (keys [][]byte, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if index, ok := shardDB.BPTreeIdx[bucket]; ok {
			index.FindRange(index.FirstKey, index.LastKey, func(key []byte, pointer interface{}) bool {
				record := pointer.(*model.Record)
				if record.E.Meta.Flag != consts.DataDeleteFlag && !record.IsExpired() {
					keys = append(keys, key)
				}
				return true
			})
		}
		return nil
	})
	return
}

// PrefixScan iterates over a key prefix at given bucket, prefix and limitNum.
// LimitNum will limit the number of entries return.
func (db *DB) PrefixScan(bucket string, prefix []byte, offsetNum int, limitNum int) (es nutsdb.Entries, off int, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if idx, ok := shardDB.BPTreeIdx[bucket]; ok {
			records, voff, err := idx.PrefixScan(prefix, offsetNum, limitNum)
			if err != nil {
				off = voff
				return errs.ErrPrefixScan
			}
			for _, r := range records {
				if r.E.Meta.Flag == consts.DataDeleteFlag || r.IsExpired() {
					continue
				}
				es = append(es, r.E)
			}
			return nil
		}
		return nil
	})
	if len(es) == 0 {
		return nil, off, errs.ErrPrefixScan
	}
	return
}

func put(shardDB *ShardDB, bucket string, key, value []byte, ttl uint32, flag consts.DataFlag) (err error) {
	if _, ok := shardDB.BPTreeIdx[bucket]; !ok {
		shardDB.BPTreeIdx[bucket] = nutsdb.NewTree()
	}
	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	timestamp := uint64(time.Now().Unix())
	bucketSize := uint32(len(bucket))
	err = shardDB.BPTreeIdx[bucket].Insert(key, &model.Entry{
		Key:   key,
		Value: value,
		Meta: &model.MetaData{
			KeySize:    keySize,
			ValueSize:  valueSize,
			Timestamp:  timestamp,
			Flag:       flag,
			TTL:        ttl,
			Bucket:     []byte(bucket),
			BucketSize: bucketSize,
			Status:     consts.Committed,
			Ds:         consts.DataStructureBPTree,
		},
	}, &model.Hint{
		Key: key,
		Meta: &model.MetaData{
			KeySize:    keySize,
			ValueSize:  valueSize,
			Timestamp:  timestamp,
			Flag:       flag,
			TTL:        ttl,
			Bucket:     []byte(bucket),
			BucketSize: bucketSize,
			Status:     consts.Committed,
			Ds:         consts.DataStructureBPTree,
		},
	}, consts.CountFlagEnabled)
	return
}
