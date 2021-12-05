package inmemory

import (
	"github.com/xujiajun/nutsdb"
	"github.com/xujiajun/nutsdb/ds/list"
)

// RPop removes and returns the last element of the list stored in the bucket at given bucket and key.
func (db *DB) RPop(bucket, key string) (item []byte, err error) {
	err = db.Managed(bucket, true, func(shardDB *ShardDB) error {
		if _, ok := shardDB.ListIdx[bucket]; !ok {
			shardDB.ListIdx[bucket] = list.New()
		}
		item, err = shardDB.ListIdx[bucket].RPop(key)
		return err
	})

	return
}

// RPeek returns the last element of the list stored in the bucket at given bucket and key.
func (db *DB) RPeek(bucket, key string) (item []byte, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if idx, ok := shardDB.ListIdx[bucket]; ok {
			item, _, err = idx.RPeek(key)
		}
		return nil
	})

	return
}

// RPush inserts the values at the tail of the list stored in the bucket at given bucket,key and values.
func (db *DB) RPush(bucket string, key string, values ...[]byte) error {
	err := db.Managed(bucket, true, func(shardDB *ShardDB) error {
		if _, ok := shardDB.ListIdx[bucket]; !ok {
			shardDB.ListIdx[bucket] = list.New()
		}

		_, err := shardDB.ListIdx[bucket].RPush(key, values...)
		return err
	})
	return err
}

// LPush inserts the values at the head of the list stored in the bucket at given bucket,key and values.
func (db *DB) LPush(bucket string, key string, values ...[]byte) error {
	err := db.Managed(bucket, true, func(shardDB *ShardDB) error {
		if _, ok := shardDB.ListIdx[bucket]; !ok {
			shardDB.ListIdx[bucket] = list.New()
		}
		_, err := shardDB.ListIdx[bucket].LPush(key, values...)
		return err
	})

	return err
}

// LPop removes and returns the first element of the list stored in the bucket at given bucket and key.
func (db *DB) LPop(bucket, key string) (item []byte, err error) {
	err = db.Managed(bucket, true, func(shardDB *ShardDB) error {
		if _, ok := shardDB.ListIdx[bucket]; !ok {
			shardDB.ListIdx[bucket] = list.New()
		}

		item, err = shardDB.ListIdx[bucket].LPop(key)
		return err
	})

	return
}

// LPeek returns the first element of the list stored in the bucket at given bucket and key.
func (db *DB) LPeek(bucket string, key string) (item []byte, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.ListIdx[bucket]; !ok {
			shardDB.ListIdx[bucket] = list.New()
		}
		item, err = shardDB.ListIdx[bucket].LPeek(key)
		return err
	})

	return
}

// LSize returns the size of key in the bucket in the bucket at given bucket and key.
func (db *DB) LSize(bucket, key string) (size int, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.ListIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		size, err = shardDB.ListIdx[bucket].Size(key)
		return err
	})

	return
}

// LRange returns the specified elements of the list stored in the bucket at given bucket,key, start and end.
// The offsets start and stop are zero-based indexes 0 being the first element of the list (the head of the list),
// 1 being the next element and so on.
// Start and end can also be negative numbers indicating offsets from the end of the list,
// where -1 is the last element of the list, -2 the penultimate element and so on.
func (db *DB) LRange(bucket string, key string, start, end int) (list [][]byte, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.ListIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		list, err = shardDB.ListIdx[bucket].LRange(key, start, end)
		return err
	})
	return
}

// LRem removes the first count occurrences of elements equal to value from the list stored in the bucket at given bucket,key,count.
// The count argument influences the operation in the following ways:
// count > 0: Remove elements equal to value moving from head to tail.
// count < 0: Remove elements equal to value moving from tail to head.
// count = 0: Remove all elements equal to value.
func (db *DB) LRem(bucket string, key string, count int, value []byte) (removedNum int, err error) {
	err = db.Managed(bucket, true, func(shardDB *ShardDB) error {
		if _, ok := shardDB.ListIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		var size int
		size, err = shardDB.ListIdx[bucket].Size(key)
		if err != nil {
			return err
		}

		if count > size || -count > size {
			return list.ErrCount
		}
		removedNum, err = shardDB.ListIdx[bucket].LRem(key, count, value)
		return err
	})

	return
}

// LSet sets the list element at index to value.
func (db *DB) LSet(bucket string, key string, index int, value []byte) error {
	err := db.Managed(bucket, true, func(shardDB *ShardDB) error {
		if _, ok := shardDB.ListIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		if _, ok := shardDB.ListIdx[bucket].Items[key]; !ok {
			return nutsdb.ErrKeyNotFound
		}
		size, _ := shardDB.ListIdx[bucket].Size(key)
		if index < 0 || index >= size {
			return list.ErrIndexOutOfRange
		}
		err := shardDB.ListIdx[bucket].LSet(key, index, value)
		return err
	})
	return err
}

// LTrim trims an existing list so that it will contain only the specified range of elements specified.
// the offsets start and stop are zero-based indexes 0 being the first element of the list (the head of the list),
// 1 being the next element and so on.
// start and end can also be negative numbers indicating offsets from the end of the list,
// where -1 is the last element of the list, -2 the penultimate element and so on.
func (db *DB) LTrim(bucket string, key string, start, end int) error {
	err := db.Managed(bucket, true, func(shardDB *ShardDB) error {
		if _, ok := shardDB.ListIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}

		err := shardDB.ListIdx[bucket].Ltrim(key, start, end)
		return err
	})
	return err
}
