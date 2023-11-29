// Copyright 2023 The nutsdb Authors. All rights reserved.
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

var tx *Tx

func TestTx_ZCheck(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		err := db.View(func(tx *Tx) error {
			require.Equal(t, ErrBucketNotExist, tx.ZCheck("fake bucket"))
			return nil
		})
		require.NoError(t, err)
	})
}

func TestTx_ZAdd(t *testing.T) {

	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)
		for i := 0; i < 10; i++ {
			txZAdd(t, db, bucket, GetTestBytes(0), GetTestBytes(i), float64(i), nil, nil)
		}

		txZCard(t, db, bucket, GetTestBytes(0), 10, nil)
	})
}

func TestTx_ZScore(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)
		for i := 0; i < 10; i++ {
			txZAdd(t, db, bucket, GetTestBytes(0), GetTestBytes(i), float64(i), nil, nil)
		}

		for i := 0; i < 10; i++ {
			txZScore(t, db, bucket, GetTestBytes(0), GetTestBytes(i), float64(i), nil)
		}

		txZScore(t, db, bucket, GetTestBytes(0), GetTestBytes(10), float64(10), ErrSortedSetMemberNotExist)
		txZScore(t, db, bucket, GetTestBytes(1), GetTestBytes(0), float64(0), ErrSortedSetNotFound)

		// update the score of member
		txZAdd(t, db, bucket, GetTestBytes(0), GetTestBytes(5), float64(999), nil, nil)
		txZScore(t, db, bucket, GetTestBytes(0), GetTestBytes(5), 999, nil)
	})
}

func TestTx_ZRem(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

		for i := 0; i < 10; i++ {
			txZAdd(t, db, bucket, GetTestBytes(0), GetTestBytes(i), float64(i), nil, nil)
		}

		txZScore(t, db, bucket, GetTestBytes(0), GetTestBytes(3), float64(3), nil)

		// normal remove
		txZRem(t, db, bucket, GetTestBytes(0), GetTestBytes(3), nil)
		txZScore(t, db, bucket, GetTestBytes(0), GetTestBytes(3), float64(3), ErrSortedSetMemberNotExist)

		txZCard(t, db, bucket, GetTestBytes(0), 9, nil)

		// remove a fake member
		txZRem(t, db, bucket, GetTestBytes(0), GetTestBytes(10), ErrSortedSetMemberNotExist)

		// remove from a fake zset
		txZRem(t, db, bucket, GetTestBytes(1), GetTestBytes(0), ErrSortedSetNotFound)
	})
}

func TestTx_ZMembers(t *testing.T) {

	bucket := "bucket"
	key := GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

		for i := 0; i < 10; i++ {
			txZAdd(t, db, bucket, key, GetTestBytes(i), float64(i), nil, nil)
		}

		err := db.View(func(tx *Tx) error {
			members, err := tx.ZMembers(bucket, key)
			require.NoError(t, err)

			require.Len(t, members, 10)

			for member := range members {
				require.Equal(t, GetTestBytes(int(member.Score)), member.Value)
			}

			return nil
		})
		require.NoError(t, err)
	})
}

func TestTx_ZCount(t *testing.T) {

	bucket := "bucket"
	key := GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

		for i := 0; i < 30; i++ {
			txZAdd(t, db, bucket, key, GetRandomBytes(24), float64(i), nil, nil)
		}

		err := db.View(func(tx *Tx) error {

			count, err := tx.ZCount(bucket, key, 10, 20, nil)
			require.NoError(t, err)
			require.Equal(t, 11, count)

			return nil
		})
		require.NoError(t, err)
	})
}

func TestTx_ZPop(t *testing.T) {
	bucket := "bucket"
	key := GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

		txZPop(t, db, bucket, key, true, nil, 0, ErrBucket)
		txZPop(t, db, bucket, key, false, nil, 0, ErrBucket)

		txZAdd(t, db, bucket, key, GetTestBytes(0), float64(0), nil, nil)
		txZRem(t, db, bucket, key, GetTestBytes(0), nil)

		txZPop(t, db, bucket, key, true, nil, 0, ErrSortedSetIsEmpty)
		txZPop(t, db, bucket, key, false, nil, 0, ErrSortedSetIsEmpty)

		for i := 0; i < 30; i++ {
			txZAdd(t, db, bucket, key, GetTestBytes(i), float64(i), nil, nil)
		}

		txZPop(t, db, bucket, key, true, GetTestBytes(29), float64(29), nil)
		txZPop(t, db, bucket, key, false, GetTestBytes(0), 0, nil)

		txZPop(t, db, bucket, key, true, GetTestBytes(28), float64(28), nil)
		txZPop(t, db, bucket, key, false, GetTestBytes(1), 1, nil)
	})
}

func TestTx_ZRangeByRank(t *testing.T) {
	bucket := "bucket"
	key := GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

		err := db.View(func(tx *Tx) error {
			_, err := tx.ZRangeByRank(bucket, key, 1, 10)
			require.Error(t, err)
			return nil
		})
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			txZAdd(t, db, bucket, key, GetTestBytes(i), float64(i), nil, nil)
		}

		err = db.View(func(tx *Tx) error {
			members, err := tx.ZRangeByRank(bucket, key, 1, 10)
			require.NoError(t, err)
			require.Len(t, members, 10)
			return nil
		})
		require.NoError(t, err)

		err = db.View(func(tx *Tx) error {
			members, err := tx.ZRangeByRank(bucket, key, 3, 6)
			require.NoError(t, err)
			require.Len(t, members, 4)
			return nil
		})
		require.NoError(t, err)

		err = db.View(func(tx *Tx) error {
			members, err := tx.ZRangeByRank(bucket, key, -1, 11)
			require.NoError(t, err)
			require.Len(t, members, 1)
			return nil
		})
		require.NoError(t, err)

		err = db.View(func(tx *Tx) error {
			members, err := tx.ZRangeByRank(bucket, key, 8, 4)
			require.NoError(t, err)
			require.Len(t, members, 5)

			for i, member := range members {
				require.Equal(t, member.Score, float64(7-i))
				require.Equal(t, member.Value, GetTestBytes(7-i))
			}

			return nil
		})
		require.NoError(t, err)
	})
}

func TestTx_ZRemRangeByRank(t *testing.T) {
	bucket := "bucket"
	key := GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

		err := db.Update(func(tx *Tx) error {
			err := tx.ZRemRangeByRank(bucket, key, 1, 10)
			assert.Error(t, err)
			return nil
		})
		assert.NoError(t, err)

		for i := 0; i < 10; i++ {
			txZAdd(t, db, bucket, key, GetTestBytes(i), float64(i), nil, nil)
		}

		err = db.Update(func(tx *Tx) error {
			err := tx.ZRemRangeByRank(bucket, key, 1, 10)
			assert.NoError(t, err)
			return nil
		})
		assert.NoError(t, err)

		txZCard(t, db, bucket, key, 0, nil)

		for i := 0; i < 10; i++ {
			txZAdd(t, db, bucket, key, GetTestBytes(i), float64(i), nil, nil)
		}

		err = db.Update(func(tx *Tx) error {
			err := tx.ZRemRangeByRank(bucket, key, 1, 2)
			assert.NoError(t, err)
			return nil
		})

		for i := 0; i < 2; i++ {
			txZScore(t, db, bucket, key, GetTestBytes(0), 0, ErrSortedSetMemberNotExist)
		}

		err = db.Update(func(tx *Tx) error {
			card, err := tx.ZCard(bucket, key)
			assert.NoError(t, err)
			assert.Equal(t, 8, card)

			err = tx.ZRemRangeByRank(bucket, key, 6, 8)
			assert.NoError(t, err)
			return nil
		})

		for i := 5; i < 8; i++ {
			txZScore(t, db, bucket, key, GetTestBytes(0), 0, ErrSortedSetMemberNotExist)
		}

		err = db.Update(func(tx *Tx) error {
			card, err := tx.ZCard(bucket, key)
			assert.NoError(t, err)
			assert.Equal(t, 5, card)

			err = tx.ZRemRangeByRank(bucket, key, 4, 3)
			assert.NoError(t, err)
			return nil
		})

		for i := 2; i < 4; i++ {
			txZScore(t, db, bucket, key, GetTestBytes(0), 0, ErrSortedSetMemberNotExist)
		}

		assert.NoError(t, err)
	})
}

func TestTx_ZRank(t *testing.T) {
	bucket := "bucket"
	key := GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

		txZRank(t, db, bucket, key, GetTestBytes(0), true, 0, ErrBucket)
		txZRank(t, db, bucket, key, GetTestBytes(0), false, 0, ErrBucket)

		for i := 0; i < 10; i++ {
			txZAdd(t, db, bucket, key, GetTestBytes(i), float64(i), nil, nil)
		}

		txZRank(t, db, bucket, key, GetTestBytes(0), true, 10, nil)
		txZRank(t, db, bucket, key, GetTestBytes(0), false, 1, nil)

		txZRem(t, db, bucket, key, GetTestBytes(0), nil)

		txZRank(t, db, bucket, key, GetTestBytes(0), true, 10, ErrSortedSetMemberNotExist)
		txZRank(t, db, bucket, key, GetTestBytes(0), false, 1, ErrSortedSetMemberNotExist)

		txZRem(t, db, bucket, key, GetTestBytes(3), nil)

		txZRank(t, db, bucket, key, GetTestBytes(4), true, 6, nil)
		txZRank(t, db, bucket, key, GetTestBytes(4), false, 3, nil)
	})
}

func TestTx_ZSetEntryIdxMode_HintKeyValAndRAMIdxMode(t *testing.T) {
	bucket := "bucket"
	key := GetTestBytes(0)
	value := GetRandomBytes(24)

	opts := DefaultOptions
	opts.EntryIdxMode = HintKeyValAndRAMIdxMode

	// HintKeyValAndRAMIdxMode
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

		err := db.Update(func(tx *Tx) error {
			err := tx.ZAdd(bucket, key, float64(0), value)
			require.NoError(t, err)

			return nil
		})
		require.NoError(t, err)

		zset := db.Index.sortedSet.getWithDefault(1, db).M[string(key)]
		hash, _ := getFnv32(value)
		node := zset.dict[hash]

		require.NotNil(t, node.record.Value)
		require.Equal(t, value, node.record.Value)
	})
}

func TestTx_ZSetEntryIdxMode_HintKeyAndRAMIdxMode(t *testing.T) {
	bucket := "bucket"
	key := GetTestBytes(0)
	value := GetRandomBytes(24)

	opts := DefaultOptions
	opts.EntryIdxMode = HintKeyAndRAMIdxMode

	// HintKeyValAndRAMIdxMode
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)
		err := db.Update(func(tx *Tx) error {
			err := tx.ZAdd(bucket, key, float64(0), value)
			require.NoError(t, err)

			return nil
		})
		require.NoError(t, err)

		zset := db.Index.sortedSet.getWithDefault(1, db).M[string(key)]
		hash, _ := getFnv32(value)
		node := zset.dict[hash]

		require.Nil(t, node.record.Value)

		v, err := db.getValueByRecord(node.record)
		require.NoError(t, err)
		require.Equal(t, value, v)
	})
}
