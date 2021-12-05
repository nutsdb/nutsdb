package inmemory

import (
	"fmt"
	"time"

	"github.com/xujiajun/nutsdb"
)

func (db *DB) Get(bucket string, key []byte) (*nutsdb.Entry, error) {
	var (
		err   error
		entry *nutsdb.Entry
	)
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if idx, ok := shardDB.BPTreeIdx[bucket]; ok {
			r, err := idx.Find(key)
			if err != nil {
				return err
			}

			if r.H.Meta.Flag == nutsdb.DataDeleteFlag || r.IsExpired() {
				return nutsdb.ErrNotFoundKey
			}

			entry = r.E
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	if entry != nil {
		return entry, nil
	}

	return nil, fmt.Errorf("not found bucket %s, key %s", bucket, key)
}

func (db *DB) Put(bucket string, key, value []byte, ttl uint32) (err error) {
	err = db.Managed(bucket, true, func(shardDB *ShardDB) error {
		return put(shardDB, bucket, key, value, ttl, nutsdb.DataSetFlag)
	})

	return
}

// Delete removes a key from the bucket at given bucket and key.
func (db *DB) Delete(bucket string, key []byte) (err error) {
	err = db.Managed(bucket, true, func(shardDB *ShardDB) error {
		return put(shardDB, bucket, key, nil, nutsdb.Persistent, nutsdb.DataDeleteFlag)
	})
	return
}

// Range query a range at given bucket, start and end slice.
func (db *DB) Range(bucket string, start, end []byte, f func(key, value []byte) bool) (err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if index, ok := shardDB.BPTreeIdx[bucket]; ok {
			index.FindRange(start, end, f)
		}
		return nil
	})

	return
}

func put(shardDB *ShardDB, bucket string, key, value []byte, ttl uint32, flag uint16) (err error) {
	if _, ok := shardDB.BPTreeIdx[bucket]; !ok {
		shardDB.BPTreeIdx[bucket] = nutsdb.NewTree()
	}
	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	timestamp := uint64(time.Now().Unix())
	bucketSize := uint32(len(bucket))
	err = shardDB.BPTreeIdx[bucket].Insert(key, &nutsdb.Entry{
		Key:   key,
		Value: value,
		Meta: &nutsdb.MetaData{
			KeySize:    keySize,
			ValueSize:  valueSize,
			Timestamp:  timestamp,
			Flag:       flag,
			TTL:        ttl,
			Bucket:     []byte(bucket),
			BucketSize: bucketSize,
			Status:     nutsdb.Committed,
			Ds:         nutsdb.DataStructureBPTree,
		},
	}, &nutsdb.Hint{
		Key: key,
		Meta: &nutsdb.MetaData{
			KeySize:    keySize,
			ValueSize:  valueSize,
			Timestamp:  timestamp,
			Flag:       flag,
			TTL:        ttl,
			Bucket:     []byte(bucket),
			BucketSize: bucketSize,
			Status:     nutsdb.Committed,
			Ds:         nutsdb.DataStructureBPTree,
		},
	}, nutsdb.CountFlagEnabled)
	return
}
