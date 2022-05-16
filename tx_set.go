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

	"github.com/pkg/errors"
	"github.com/xujiajun/nutsdb/ds/set"
)

func (tx *Tx) sPut(bucket string, key []byte, dataFlag uint16, items ...[]byte) error {

	if dataFlag == DataSetFlag {

		filter := make(map[string]struct{})

		if set, ok := tx.db.SetIdx[bucket]; ok {

			if _, ok := set.M[string(key)]; ok {
				for item := range set.M[string(key)] {
					filter[item] = struct{}{}
				}
			}

		}

		for _, item := range items {
			if _, ok := filter[string(item)]; !ok {
				filter[string(item)] = struct{}{}
				err := tx.put(bucket, key, item, Persistent, dataFlag, uint64(time.Now().Unix()), DataStructureSet)
				if err != nil {
					return err
				}
			}
		}

	} else {
		for _, item := range items {

			err := tx.put(bucket, key, item, Persistent, dataFlag, uint64(time.Now().Unix()), DataStructureSet)
			if err != nil {
				return err
			}

		}
	}

	return nil
}

// SAdd adds the specified members to the set stored int the bucket at given bucket,key and items.
func (tx *Tx) SAdd(bucket string, key []byte, items ...[]byte) error {
	return tx.sPut(bucket, key, DataSetFlag, items...)
}

// SRem removes the specified members from the set stored int the bucket at given bucket,key and items.
func (tx *Tx) SRem(bucket string, key []byte, items ...[]byte) error {
	return tx.sPut(bucket, key, DataDeleteFlag, items...)
}

// SAreMembers returns if the specified members are the member of the set int the bucket at given bucket,key and items.
func (tx *Tx) SAreMembers(bucket string, key []byte, items ...[]byte) (bool, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return false, err
	}

	if sets, ok := tx.db.SetIdx[bucket]; ok {
		return sets.SAreMembers(string(key), items...)
	}

	return false, ErrBucketNotFound
}

// SIsMember returns if member is a member of the set stored int the bucket at given bucket,key and item.
func (tx *Tx) SIsMember(bucket string, key, item []byte) (bool, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return false, err
	}

	if set, ok := tx.db.SetIdx[bucket]; ok {
		if !set.SIsMember(string(key), item) {
			return false, ErrBucketNotFound
		}
		return true, nil
	}

	return false, ErrBucketNotFound
}

// SMembers returns all the members of the set value stored int the bucket at given bucket and key.
func (tx *Tx) SMembers(bucket string, key []byte) (list [][]byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if set, ok := tx.db.SetIdx[bucket]; ok {
		return set.SMembers(string(key))
	}

	return nil, ErrBucketNotFound

}

// SHasKey returns if the set in the bucket at given bucket and key.
func (tx *Tx) SHasKey(bucket string, key []byte) (bool, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return false, err
	}

	if set, ok := tx.db.SetIdx[bucket]; ok {
		return set.SHasKey(string(key)), nil
	}

	return false, ErrBucketNotFound
}

// SPop removes and returns one or more random elements from the set value store in the bucket at given bucket and key.
func (tx *Tx) SPop(bucket string, key []byte) ([]byte, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if _, ok := tx.db.SetIdx[bucket]; ok {
		for item := range tx.db.SetIdx[bucket].M[string(key)] {
			return []byte(item), tx.sPut(bucket, key, DataDeleteFlag, []byte(item))
		}
	}

	return nil, ErrBucketNotFound
}

// SCard returns the set cardinality (number of elements) of the set stored in the bucket at given bucket and key.
func (tx *Tx) SCard(bucket string, key []byte) (int, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return 0, err
	}

	if set, ok := tx.db.SetIdx[bucket]; ok {
		return set.SCard(string(key)), nil
	}

	return 0, ErrBucketNotFound
}

// SDiffByOneBucket returns the members of the set resulting from the difference
// between the first set and all the successive sets in one bucket.
func (tx *Tx) SDiffByOneBucket(bucket string, key1, key2 []byte) (list [][]byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if set, ok := tx.db.SetIdx[bucket]; ok {
		return set.SDiff(string(key1), string(key2))
	}

	return nil, ErrBucketNotFound
}

// SDiffByTwoBuckets returns the members of the set resulting from the difference
// between the first set and all the successive sets in two buckets.
func (tx *Tx) SDiffByTwoBuckets(bucket1 string, key1 []byte, bucket2 string, key2 []byte) (list [][]byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	var (
		set1, set2 *set.Set
		ok         bool
	)

	if set1, ok = tx.db.SetIdx[bucket1]; !ok {
		return nil, ErrBucketAndKey(bucket1, key1)
	}

	if set2, ok = tx.db.SetIdx[bucket2]; !ok {
		return nil, ErrBucketAndKey(bucket2, key2)
	}

	for item1 := range set1.M[string(key1)] {
		if _, ok := set2.M[string(key2)][item1]; !ok {
			list = append(list, []byte(item1))
		}
	}

	return
}

// SMoveByOneBucket moves member from the set at source to the set at destination in one bucket.
func (tx *Tx) SMoveByOneBucket(bucket string, key1, key2, item []byte) (bool, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return false, err
	}

	if set, ok := tx.db.SetIdx[bucket]; ok {
		return set.SMove(string(key1), string(key2), item)
	}

	return false, ErrBucket
}

// SMoveByTwoBuckets moves member from the set at source to the set at destination in two buckets.
func (tx *Tx) SMoveByTwoBuckets(bucket1 string, key1 []byte, bucket2 string, key2, item []byte) (bool, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return false, err
	}

	var (
		set1, set2 *set.Set
		ok         bool
	)

	if set1, ok = tx.db.SetIdx[bucket1]; !ok {
		return false, ErrBucketAndKey(bucket1, key1)
	}

	if set2, ok = tx.db.SetIdx[bucket2]; !ok {
		return false, ErrBucketAndKey(bucket2, key1)
	}

	if !set1.SHasKey(string(key1)) {
		return false, ErrNotFoundKeyInBucket(bucket1, key1)
	}

	if !set2.SHasKey(string(key2)) {
		return false, ErrNotFoundKeyInBucket(bucket2, key2)
	}

	if _, ok := set2.M[string(key2)][string(item)]; !ok {
		set2.SAdd(string(key2), item)
	}

	set1.SRem(string(key1), item)

	return true, nil
}

// SUnionByOneBucket the members of the set resulting from the union of all the given sets in one bucket.
func (tx *Tx) SUnionByOneBucket(bucket string, key1, key2 []byte) (list [][]byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if set, ok := tx.db.SetIdx[bucket]; ok {
		return set.SUnion(string(key1), string(key2))
	}

	return nil, ErrBucket
}

// SUnionByTwoBuckets the members of the set resulting from the union of all the given sets in two buckets.
func (tx *Tx) SUnionByTwoBuckets(bucket1 string, key1 []byte, bucket2 string, key2 []byte) (list [][]byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	var (
		set1, set2 *set.Set
		ok         bool
	)

	if set1, ok = tx.db.SetIdx[bucket1]; !ok {
		return nil, ErrBucketAndKey(bucket1, key1)
	}

	if set2, ok = tx.db.SetIdx[bucket2]; !ok {
		return nil, ErrBucketAndKey(bucket2, key1)
	}

	if !set1.SHasKey(string(key1)) {
		return nil, ErrNotFoundKeyInBucket(bucket1, key1)
	}

	if !set2.SHasKey(string(key2)) {
		return nil, ErrNotFoundKeyInBucket(bucket2, key2)
	}

	for item1 := range set1.M[string(key1)] {
		list = append(list, []byte(item1))
	}

	for item2 := range set2.M[string(key2)] {
		if _, ok := set1.M[string(key1)][item2]; !ok {
			list = append(list, []byte(item2))
		}
	}

	return
}

// ErrBucketAndKey returns when bucket or key not found.
func ErrBucketAndKey(bucket string, key []byte) error {
	return errors.Wrapf(ErrBucketNotFound, "bucket:%s, key:%s", bucket, key)
}

// ErrNotFoundKeyInBucket returns when key not in the bucket.
func ErrNotFoundKeyInBucket(bucket string, key []byte) error {
	return errors.Wrapf(ErrKeyNotFound, "%s is not found in %s", key, bucket)
}
