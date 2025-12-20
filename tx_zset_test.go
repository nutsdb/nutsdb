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
	"fmt"
	"testing"

	"github.com/nutsdb/nutsdb/internal/testutils"
	"github.com/nutsdb/nutsdb/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			txZAdd(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(i), float64(i), nil, nil)
		}

		txZCard(t, db, bucket, testutils.GetTestBytes(0), 10, nil)
	})
}

func TestTx_ZScore(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)
		for i := 0; i < 10; i++ {
			txZAdd(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(i), float64(i), nil, nil)
		}

		for i := 0; i < 10; i++ {
			txZScore(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(i), float64(i), nil)
		}

		txZScore(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(10), float64(10), ErrSortedSetMemberNotExist)
		txZScore(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(0), float64(0), ErrSortedSetNotFound)

		// update the score of member
		txZAdd(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(5), float64(999), nil, nil)
		txZScore(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(5), 999, nil)
	})
}

func TestTx_ZRem(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

		for i := 0; i < 10; i++ {
			txZAdd(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(i), float64(i), nil, nil)
		}

		txZScore(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(3), float64(3), nil)

		// normal remove
		txZRem(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(3), nil)
		txZScore(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(3), float64(3), ErrSortedSetMemberNotExist)

		txZCard(t, db, bucket, testutils.GetTestBytes(0), 9, nil)

		// remove a fake member
		txZRem(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(10), ErrSortedSetMemberNotExist)

		// remove from a fake zset
		txZRem(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(0), ErrSortedSetNotFound)
	})
}

func TestTx_ZMembers(t *testing.T) {

	bucket := "bucket"
	key := testutils.GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

		for i := 0; i < 10; i++ {
			txZAdd(t, db, bucket, key, testutils.GetTestBytes(i), float64(i), nil, nil)
		}

		err := db.View(func(tx *Tx) error {
			members, err := tx.ZMembers(bucket, key)
			require.NoError(t, err)

			require.Len(t, members, 10)

			for member := range members {
				require.Equal(t, testutils.GetTestBytes(int(member.Score)), member.Value)
			}

			return nil
		})
		require.NoError(t, err)
	})
}

func TestTx_ZCount(t *testing.T) {

	bucket := "bucket"
	key := testutils.GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

		for i := 0; i < 30; i++ {
			txZAdd(t, db, bucket, key, testutils.GetRandomBytes(24), float64(i), nil, nil)
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
	key := testutils.GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

		txZPop(t, db, bucket, key, true, nil, 0, ErrSortedSetNotFound)
		txZPop(t, db, bucket, key, false, nil, 0, ErrSortedSetNotFound)

		txZAdd(t, db, bucket, key, testutils.GetTestBytes(0), float64(0), nil, nil)
		txZRem(t, db, bucket, key, testutils.GetTestBytes(0), nil)

		txZPop(t, db, bucket, key, true, nil, 0, ErrSortedSetIsEmpty)
		txZPop(t, db, bucket, key, false, nil, 0, ErrSortedSetIsEmpty)

		for i := 0; i < 30; i++ {
			txZAdd(t, db, bucket, key, testutils.GetTestBytes(i), float64(i), nil, nil)
		}

		txZPop(t, db, bucket, key, true, testutils.GetTestBytes(29), float64(29), nil)
		txZPop(t, db, bucket, key, false, testutils.GetTestBytes(0), 0, nil)

		txZPop(t, db, bucket, key, true, testutils.GetTestBytes(28), float64(28), nil)
		txZPop(t, db, bucket, key, false, testutils.GetTestBytes(1), 1, nil)
	})
}

func TestTx_ZRangeByScore(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)
		err := db.View((func(tx *Tx) error {
			_, err := tx.ZRangeByScore(bucket, key, 1, 10, nil)
			require.Error(t, err)
			return nil
		}))
		require.NoError(t, err)
		for i := 0; i < 10; i++ {
			txZAdd(t, db, bucket, key, testutils.GetTestBytes(i), float64(i), nil, nil)
		}

		err = db.View(func(tx *Tx) error {
			members, err := tx.ZRangeByScore(bucket, key, 0, 11, nil)
			require.NoError(t, err)
			require.Len(t, members, 10)
			return nil
		})
		require.NoError(t, err)
		err = db.View(func(tx *Tx) error {
			members, err := tx.ZRangeByScore(bucket, key, -1, 2, nil)
			require.NoError(t, err)
			require.Len(t, members, 3)
			return nil
		})
		require.NoError(t, err)
		err = db.View(func(tx *Tx) error {
			members, err := tx.ZRangeByScore(bucket, key, 8, 12, nil)
			require.NoError(t, err)
			require.Len(t, members, 2)
			return nil
		})
		require.NoError(t, err)
		err = db.View(func(tx *Tx) error {
			members, err := tx.ZRangeByScore(bucket, key, 5, 2, nil)
			require.NoError(t, err)
			require.Len(t, members, 4)
			return nil
		})
		require.NoError(t, err)
	})
}
func TestTx_ZPeekMin(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

		for i := 0; i < 30; i++ {
			txZAdd(t, db, bucket, key, testutils.GetTestBytes(i), float64(i), nil, nil)
		}

		// get minimum node
		txZPeekMin(t, db, bucket, key, testutils.GetTestBytes(0), float64(0), nil, nil)

		//  bucket not exists
		txZPeekMin(t, db, "non-exists-bucket", key, []byte{}, float64(0), ErrBucketNotExist, ErrBucketNotExist)

		// key not exists
		txZPeekMin(t, db, bucket, []byte("non-exists-key"), []byte{}, float64(0), ErrSortedSetNotFound, ErrSortedSetNotFound)

		// add nodes

		// add node that will not affect the minimum node
		txZAdd(t, db, bucket, key, []byte("new-mem"), float64(3), nil, nil)
		txZPeekMin(t, db, bucket, key, testutils.GetTestBytes(0), float64(0), nil, nil)
		// add a new minimum value
		txZAdd(t, db, bucket, key, []byte("new-min"), float64(0), nil, nil)
		txZPeekMin(t, db, bucket, key, []byte("new-min"), float64(0), nil, nil)

		// remove nodes

		// remove minimum node
		txZRem(t, db, bucket, key, []byte("new-min"), nil)
		txZPeekMin(t, db, bucket, key, testutils.GetTestBytes(0), float64(0), nil, nil)

		// remove non-minimum node
		txZRem(t, db, bucket, key, testutils.GetTestBytes(5), nil)
		txZPeekMin(t, db, bucket, key, testutils.GetTestBytes(0), float64(0), nil, nil)

		// remove range by rank
		err := db.Update(func(tx *Tx) error {
			err := tx.ZRemRangeByRank(bucket, key, 1, 10)
			assert.NoError(t, err)
			return nil
		})
		assert.NoError(t, err)
		txZPeekMin(t, db, bucket, key, testutils.GetTestBytes(10), float64(10), nil, nil)

		// pop

		// pop min
		txZPop(t, db, bucket, key, false, testutils.GetTestBytes(10), float64(10), nil)
		txZPeekMin(t, db, bucket, key, testutils.GetTestBytes(11), float64(11), nil, nil)

		// pop max
		txZPop(t, db, bucket, key, true, testutils.GetTestBytes(29), float64(29), nil)
		txZPeekMin(t, db, bucket, key, testutils.GetTestBytes(11), float64(11), nil, nil)
	})
}

func TestTx_ZRangeByRank(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

		err := db.View(func(tx *Tx) error {
			_, err := tx.ZRangeByRank(bucket, key, 1, 10)
			require.Error(t, err)
			return nil
		})
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			txZAdd(t, db, bucket, key, testutils.GetTestBytes(i), float64(i), nil, nil)
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
				require.Equal(t, member.Value, testutils.GetTestBytes(7-i))
			}

			return nil
		})
		require.NoError(t, err)
	})
}

func TestTx_ZRemRangeByRank(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

		err := db.Update(func(tx *Tx) error {
			err := tx.ZRemRangeByRank(bucket, key, 1, 10)
			assert.NoError(t, err)
			return nil
		})
		assert.NoError(t, err)

		for i := 0; i < 10; i++ {
			txZAdd(t, db, bucket, key, testutils.GetTestBytes(i), float64(i), nil, nil)
		}

		err = db.Update(func(tx *Tx) error {
			err := tx.ZRemRangeByRank(bucket, key, 1, 10)
			assert.NoError(t, err)
			return nil
		})
		assert.NoError(t, err)

		txZCard(t, db, bucket, key, 0, nil)

		for i := 0; i < 10; i++ {
			txZAdd(t, db, bucket, key, testutils.GetTestBytes(i), float64(i), nil, nil)
		}

		err = db.Update(func(tx *Tx) error {
			err := tx.ZRemRangeByRank(bucket, key, 1, 2)
			assert.NoError(t, err)
			return nil
		})
		assert.NoError(t, err)

		for i := 0; i < 2; i++ {
			txZScore(t, db, bucket, key, testutils.GetTestBytes(0), 0, ErrSortedSetMemberNotExist)
		}

		err = db.Update(func(tx *Tx) error {
			card, err := tx.ZCard(bucket, key)
			assert.NoError(t, err)
			assert.Equal(t, 8, card)

			err = tx.ZRemRangeByRank(bucket, key, 6, 8)
			assert.NoError(t, err)
			return nil
		})
		assert.NoError(t, err)

		for i := 5; i < 8; i++ {
			txZScore(t, db, bucket, key, testutils.GetTestBytes(0), 0, ErrSortedSetMemberNotExist)
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
			txZScore(t, db, bucket, key, testutils.GetTestBytes(0), 0, ErrSortedSetMemberNotExist)
		}

		assert.NoError(t, err)
	})
}

func TestTx_ZRank(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

		txZRank(t, db, bucket, key, testutils.GetTestBytes(0), true, 0, ErrSortedSetNotFound)
		txZRank(t, db, bucket, key, testutils.GetTestBytes(0), false, 0, ErrSortedSetNotFound)

		for i := 0; i < 10; i++ {
			txZAdd(t, db, bucket, key, testutils.GetTestBytes(i), float64(i), nil, nil)
		}

		txZRank(t, db, bucket, key, testutils.GetTestBytes(0), true, 10, nil)
		txZRank(t, db, bucket, key, testutils.GetTestBytes(0), false, 1, nil)

		txZRem(t, db, bucket, key, testutils.GetTestBytes(0), nil)

		txZRank(t, db, bucket, key, testutils.GetTestBytes(0), true, 10, ErrSortedSetMemberNotExist)
		txZRank(t, db, bucket, key, testutils.GetTestBytes(0), false, 1, ErrSortedSetMemberNotExist)

		txZRem(t, db, bucket, key, testutils.GetTestBytes(3), nil)

		txZRank(t, db, bucket, key, testutils.GetTestBytes(4), true, 6, nil)
		txZRank(t, db, bucket, key, testutils.GetTestBytes(4), false, 3, nil)
	})
}

func TestTx_ZKeys(t *testing.T) {
	bucket := "bucket"
	key := "key_%d"
	val := testutils.GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)
		for i := 0; i < 3; i++ {
			txZAdd(t, db, bucket, []byte(fmt.Sprintf(key, i)), val, float64(i), nil, nil)
		}
		txZAdd(t, db, bucket, []byte("foo"), val, 1, nil, nil)

		tests := []struct {
			pattern         string
			expectedMatches int
			expectedError   error
		}{
			{"*", 4, nil},         // find all keys
			{"key_*", 3, nil},     // find keys with 'key_' prefix
			{"fake_key*", 0, nil}, // find non-existing keys
		}

		for _, test := range tests {
			txZKeys(t, db, bucket, test.pattern, func(key string) bool { return true }, test.expectedMatches, test.expectedError)
		}

		// stop after finding the expected number of keys.
		expectNum := 2
		var foundKeys []string
		txZKeys(t, db, bucket, "*", func(key string) bool {
			foundKeys = append(foundKeys, key)
			return len(foundKeys) < expectNum
		}, expectNum, nil)
		assert.Equal(t, expectNum, len(foundKeys))
	})
}

func TestTx_ZSetEntryIdxMode_HintKeyValAndRAMIdxMode(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)
	value := testutils.GetRandomBytes(24)

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

		zset := db.Index.SortedSet.GetWithDefault(1).M[string(key)]
		hash, _ := utils.GetFnv32(value)
		node := zset.dict[hash]

		require.NotNil(t, node.record.Value)
		require.Equal(t, value, node.record.Value)
	})
}

func TestTx_ZSetEntryIdxMode_HintKeyAndRAMIdxMode(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)
	value := testutils.GetRandomBytes(24)

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

		zset := db.Index.SortedSet.GetWithDefault(1).M[string(key)]
		hash, _ := utils.GetFnv32(value)
		node := zset.dict[hash]

		require.Nil(t, node.record.Value)

		v, err := db.getValueByRecord(node.record)
		require.NoError(t, err)
		require.Equal(t, value, v)
	})
}
