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
	"github.com/nutsdb/nutsdb"
	"github.com/nutsdb/nutsdb/ds/set"

	"github.com/pkg/errors"
)

// SAdd adds the specified members to the set stored int the bucket at given bucket,key and items.
func (db *DB) SAdd(bucket string, key string, items ...[]byte) error {
	err := db.Managed(bucket, true, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SetIdx[bucket]; !ok {
			shardDB.SetIdx[bucket] = set.New()
		}
		return shardDB.SetIdx[bucket].SAdd(key, items...)
	})
	return err
}

// SRem removes the specified members from the set stored int the bucket at given bucket,key and items.
func (db *DB) SRem(bucket string, key string, items ...[]byte) error {
	err := db.Managed(bucket, true, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		return shardDB.SetIdx[bucket].SRem(key, items...)
	})
	return err
}

// SAreMembers returns if the specified members are the member of the set int the bucket at given bucket,key and items.
func (db *DB) SAreMembers(bucket string, key string, items ...[]byte) (areMembers bool, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		areMembers, err = shardDB.SetIdx[bucket].SAreMembers(key, items...)
		return err
	})
	return
}

// SIsMember returns if member is a member of the set stored int the bucket at given bucket,key and item.
func (db *DB) SIsMember(bucket string, key string, item []byte) (isMember bool, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		isMember = shardDB.SetIdx[bucket].SIsMember(key, item)
		return nil
	})
	return
}

// SMembers returns all the members of the set value stored int the bucket at given bucket and key.
func (db *DB) SMembers(bucket string, key string) (list [][]byte, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		list, err = shardDB.SetIdx[bucket].SMembers(key)
		return err
	})
	return
}

// SHasKey returns if the set in the bucket at given bucket and key.
func (db *DB) SHasKey(bucket string, key string) (hasKey bool, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		hasKey = shardDB.SetIdx[bucket].SHasKey(key)
		return nil
	})
	return
}

// SPop removes and returns one or more random elements from the set value store in the bucket at given bucket and key.
func (db *DB) SPop(bucket string, key string) (item []byte, err error) {
	err = db.Managed(bucket, true, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		item = shardDB.SetIdx[bucket].SPop(key)
		return nil
	})
	return
}

// SCard returns the set cardinality (number of elements) of the set stored in the bucket at given bucket and key.
func (db *DB) SCard(bucket string, key string) (itemNumber int, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		itemNumber = shardDB.SetIdx[bucket].SCard(key)
		return nil
	})
	return
}

// SDiffByOneBucket returns the members of the set resulting from the difference
// between the first set and all the successive sets in one bucket.
func (db *DB) SDiffByOneBucket(bucket string, key1, key2 string) (list [][]byte, err error) {
	err = db.Managed(bucket, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		list, err = shardDB.SetIdx[bucket].SDiff(key1, key2)
		return nil
	})
	return
}

// SDiffByTwoBuckets returns the members of the set resulting from the difference
// between the first set and all the successive sets in two buckets.
func (db *DB) SDiffByTwoBuckets(bucket1 string, key1 string, bucket2 string, key2 string) (list [][]byte, err error) {
	var set1, set2 *set.Set

	err = db.Managed(bucket1, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SetIdx[bucket1]; !ok {
			return nutsdb.ErrBucket
		}
		set1 = shardDB.SetIdx[bucket1]
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = db.Managed(bucket2, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SetIdx[bucket2]; !ok {
			return nutsdb.ErrBucket
		}
		set2 = shardDB.SetIdx[bucket2]
		return nil
	})
	if err != nil {
		return nil, err
	}
	for item1 := range set1.M[key1] {
		if _, ok := set2.M[key2][item1]; !ok {
			list = append(list, []byte(item1))
		}
	}
	return
}

// SMoveByOneBucket moves member from the set at source to the set at destination in one bucket.
func (db *DB) SMoveByOneBucket(bucket, key1, key2 string, item []byte) (isOk bool, err error) {
	err = db.Managed(bucket, true, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		isOk, err = shardDB.SetIdx[bucket].SMove(key1, key2, item)
		return err
	})
	return
}

// SMoveByTwoBuckets moves member from the set at source to the set at destination in two buckets.
func (db *DB) SMoveByTwoBuckets(bucket1 string, key1 string, bucket2 string, key2 string, item []byte) (isOK bool, err error) {
	var set1, set2 *set.Set
	set1, set2, err = db.getTwoSetsByBuckets(bucket1, bucket2)
	if err != nil {
		return false, err
	}
	err = db.checkTwoSets(set1, set2, key1, key2, bucket1, bucket2)
	if err != nil {
		return false, err
	}

	if _, ok := set2.M[key2][string(item)]; !ok {
		set2.SAdd(key2, item)
	}

	err = set1.SRem(key1, item)

	return true, err
}

// SUnionByOneBucket the members of the set resulting from the union of all the given sets in one bucket.
func (db *DB) SUnionByOneBucket(bucket string, key1, key2 string) (list [][]byte, err error) {
	err = db.Managed(bucket, true, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SetIdx[bucket]; !ok {
			return nutsdb.ErrBucket
		}
		list, err = shardDB.SetIdx[bucket].SUnion(key1, key2)
		return err
	})
	return
}

// SUnionByTwoBuckets the members of the set resulting from the union of all the given sets in two buckets.
func (db *DB) SUnionByTwoBuckets(bucket1 string, key1 string, bucket2 string, key2 string) (list [][]byte, err error) {
	var set1, set2 *set.Set
	set1, set2, err = db.getTwoSetsByBuckets(bucket1, bucket2)
	if err != nil {
		return nil, err
	}
	err = db.checkTwoSets(set1, set2, key1, key2, bucket1, bucket2)
	if err != nil {
		return nil, err
	}
	for item1 := range set1.M[key1] {
		list = append(list, []byte(item1))
	}

	for item2 := range set2.M[key2] {
		if _, ok := set1.M[key1][item2]; !ok {
			list = append(list, []byte(item2))
		}
	}
	return
}

func (db *DB) checkTwoSets(set1, set2 *set.Set, key1, key2 string, bucket1, bucket2 string) error {
	if !set1.SHasKey(key1) {
		return errors.Wrapf(nutsdb.ErrKeyNotFound, "key %s is not in the bucket %s", key1, bucket1)
	}

	if !set2.SHasKey(key2) {
		return errors.Wrapf(nutsdb.ErrKeyNotFound, "key %s is not in the bucket %s", key2, bucket2)
	}
	return nil
}

func (db *DB) getTwoSetsByBuckets(bucket1, bucket2 string) (set1 *set.Set, set2 *set.Set, err error) {
	err = db.Managed(bucket1, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SetIdx[bucket1]; !ok {
			return nutsdb.ErrBucket
		}
		set1 = shardDB.SetIdx[bucket1]
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	err = db.Managed(bucket2, false, func(shardDB *ShardDB) error {
		if _, ok := shardDB.SetIdx[bucket2]; !ok {
			return nutsdb.ErrBucket
		}
		set2 = shardDB.SetIdx[bucket2]
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return
}
