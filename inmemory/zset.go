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
	"github.com/xujiajun/nutsdb"
	"github.com/xujiajun/nutsdb/ds/zset"
)

// ZAdd adds the specified member key with the specified score and specified val to the sorted set stored at bucket.
func (db *DB) ZAdd(bucket string, key string, score float64, val []byte) error {
	err := db.Managed(bucket, true, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SortedSetIdx[bucket]; !ok {
			shardDB.SortedSetIdx[bucket] = zset.New()
		}
		return shardDB.SortedSetIdx[bucket].Put(key, zset.SCORE(score), val)
	})
	return err
}

// ZMembers returns all the members of the set value stored at bucket.
func (db *DB) ZMembers(bucket string) (dict map[string]*zset.SortedSetNode, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SortedSetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		dict = shardDB.SortedSetIdx[bucket].Dict
		return nil
	})
	return
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at bucket.
func (db *DB) ZCard(bucket string) (int, error) {
	dict, err := db.ZMembers(bucket)
	if err != nil {
		return 0, err
	}

	return len(dict), nil
}

// ZCount returns the number of elements in the sorted set at bucket with a score between min and max and opts.
// opts includes the following parameters:
// Limit        int  // limit the max nodes to return
// ExcludeStart bool // exclude start value, so it search in interval (start, end] or (start, end)
// ExcludeEnd   bool // exclude end value, so it search in interval [start, end) or (start, end)
func (db *DB) ZCount(bucket string, start, end float64, opts *zset.GetByScoreRangeOptions) (int, error) {
	nodes, err := db.ZRangeByScore(bucket, start, end, opts)
	if err != nil {
		return 0, err
	}

	return len(nodes), nil
}

// ZRangeByScore returns all the elements in the sorted set at bucket with a score between min and max.
func (db *DB) ZRangeByScore(bucket string, start, end float64, opts *zset.GetByScoreRangeOptions) (nodes []*zset.SortedSetNode, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SortedSetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		nodes = shardDB.SortedSetIdx[bucket].GetByScoreRange(zset.SCORE(start), zset.SCORE(end), opts)
		return nil
	})
	return
}

// ZRangeByRank returns all the elements in the sorted set in one bucket and key
// with a rank between start and end (including elements with rank equal to start or end).
func (db *DB) ZRangeByRank(bucket string, start, end int) (nodes []*zset.SortedSetNode, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SortedSetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		nodes = shardDB.SortedSetIdx[bucket].GetByRankRange(start, end, false)
		return nil
	})
	return
}

// ZRem removes the specified members from the sorted set stored in one bucket at given bucket and key.
func (db *DB) ZRem(bucket, key string) error {
	err := db.Managed(bucket, true, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SortedSetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		_ = shardDB.SortedSetIdx[bucket].Remove(key)
		return nil
	})
	return err
}

// ZRemRangeByRank removes all elements in the sorted set stored in one bucket at given bucket with rank between start and end.
// the rank is 1-based integer. Rank 1 means the first node; Rank -1 means the last node.
func (db *DB) ZRemRangeByRank(bucket string, start, end int) error {
	err := db.Managed(bucket, true, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SortedSetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}

		_ = shardDB.SortedSetIdx[bucket].GetByRankRange(start, end, true)
		return nil
	})
	return err
}

// ZRank returns the rank of member in the sorted set stored in the bucket at given bucket and key,
// with the scores ordered from low to high.
func (db *DB) ZRank(bucket string, key string) (rank int, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SortedSetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}

		rank = shardDB.SortedSetIdx[bucket].FindRank(key)
		return nil
	})
	return
}

// ZRevRank returns the rank of member in the sorted set stored in the bucket at given bucket and key,
// with the scores ordered from high to low.
func (db *DB) ZRevRank(bucket string, key string) (rank int, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SortedSetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}

		rank = shardDB.SortedSetIdx[bucket].FindRevRank(key)
		return nil
	})
	return
}

// ZScore returns the score of member in the sorted set in the bucket at given bucket and key.
func (db *DB) ZScore(bucket string, key string) (score float64, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SortedSetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}

		score = float64(shardDB.SortedSetIdx[bucket].GetByKey(key).Score())
		return nil
	})
	return
}

// ZGetByKey returns node in the bucket at given bucket and key.
func (db *DB) ZGetByKey(bucket string, key string) (node *zset.SortedSetNode, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SortedSetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}

		node = shardDB.SortedSetIdx[bucket].GetByKey(key)
		return nil
	})
	return
}
