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
	"os"
	"path"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/testutils"
	"github.com/nutsdb/nutsdb/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pushDataByStartEnd(t *testing.T, db *DB, bucket string, key int, start, end int, isLeft bool) {
	for i := start; i <= end; i++ {
		txPush(t, db, bucket, testutils.GetTestBytes(key), testutils.GetTestBytes(i), isLeft, nil, nil)
	}
}

func pushDataByValues(t *testing.T, db *DB, bucket string, key int, isLeft bool, values ...int) {
	for _, v := range values {
		txPush(t, db, bucket, testutils.GetTestBytes(key), testutils.GetTestBytes(v), isLeft, nil, nil)
	}
}

func TestTx_RPush(t *testing.T) {
	bucket := "bucket"

	// 1. Insert values for some keys by using RPush
	// 2. Validate values for these keys by using RPop
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		pushDataByStartEnd(t, db, bucket, 0, 0, 9, false)
		pushDataByStartEnd(t, db, bucket, 1, 10, 19, false)
		pushDataByStartEnd(t, db, bucket, 2, 20, 29, false)

		for i := 0; i < 10; i++ {
			txPop(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(9-i), nil, false)
		}
		for i := 10; i < 20; i++ {
			txPop(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(29-i), nil, false)
		}
		for i := 20; i < 30; i++ {
			txPop(t, db, bucket, testutils.GetTestBytes(2), testutils.GetTestBytes(49-i), nil, false)
		}
	})
}

func TestTx_MPush(t *testing.T) {
	bucket := "bucket"
	t.Run("Test Multiple LPush ", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			bbs := make([][]byte, 0)
			bbs = append(bbs, testutils.GetTestBytes(2))
			bbs = append(bbs, testutils.GetTestBytes(3))
			bbs = append(bbs, testutils.GetTestBytes(4))

			txCreateBucket(t, db, DataStructureList, bucket, nil)
			txMPush(t, db, bucket, testutils.GetTestBytes(1), bbs, true, nil, nil)

			expect := make([][]byte, 0)
			for i := len(bbs) - 1; i >= 0; i-- {
				expect = append(expect, bbs[i])
			}

			txLRange(t, db, bucket, testutils.GetTestBytes(1), 0, 2, 3, expect, nil)
		})
	})

	t.Run("Test Error LPush ", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			bbs := make([][]byte, 0)
			bbs = append(bbs, testutils.GetTestBytes(2))
			bbs = append(bbs, testutils.GetTestBytes(3))
			bbs = append(bbs, testutils.GetTestBytes(4))

			txCreateBucket(t, db, DataStructureList, bucket, nil)
			txMPush(t, db, "test1", testutils.GetTestBytes(1), bbs, true, ErrNotFoundBucket, nil)
		})
	})

	t.Run("Test Multiple RPush ", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			bbs := make([][]byte, 0)
			bbs = append(bbs, testutils.GetTestBytes(2))
			bbs = append(bbs, testutils.GetTestBytes(3))
			bbs = append(bbs, testutils.GetTestBytes(4))
			txCreateBucket(t, db, DataStructureList, bucket, nil)
			txMPush(t, db, bucket, testutils.GetTestBytes(1), bbs, false, nil, nil)
			txLRange(t, db, bucket, testutils.GetTestBytes(1), 0, 2, 3, bbs, nil)
		})
	})

	t.Run("Test Error RPush ", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			bbs := make([][]byte, 0)
			bbs = append(bbs, testutils.GetTestBytes(2))
			bbs = append(bbs, testutils.GetTestBytes(3))
			bbs = append(bbs, testutils.GetTestBytes(4))

			txCreateBucket(t, db, DataStructureList, bucket, nil)
			txMPush(t, db, "test1", testutils.GetTestBytes(1), bbs, false, ErrNotFoundBucket, nil)
		})
	})
}

func TestTx_LPush(t *testing.T) {
	bucket := "bucket"

	// 1. Insert values for some keys by using LPush
	// 2. Validate values for these keys by using LPop
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)

		pushDataByStartEnd(t, db, bucket, 0, 0, 9, true)
		pushDataByStartEnd(t, db, bucket, 1, 10, 19, true)
		pushDataByStartEnd(t, db, bucket, 2, 20, 29, true)

		txPush(t, db, bucket, []byte("012|sas"), testutils.GetTestBytes(0), true, ErrSeparatorForListKey, nil)

		for i := 0; i < 10; i++ {
			txPop(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(9-i), nil, true)
		}
		for i := 10; i < 20; i++ {
			txPop(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(29-i), nil, true)
		}
		for i := 20; i < 30; i++ {
			txPop(t, db, bucket, testutils.GetTestBytes(2), testutils.GetTestBytes(49-i), nil, true)
		}
	})
}

func TestTx_LPushRaw(t *testing.T) {
	bucket := "bucket"
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)

		seq := uint64(100000)
		for i := 0; i <= 100; i++ {
			key := utils.EncodeListKey([]byte("0"), seq)
			seq--
			txPushRaw(t, db, bucket, key, testutils.GetTestBytes(i), true, nil, nil)
		}

		for i := 0; i <= 100; i++ {
			v := testutils.GetTestBytes(100 - i)
			txPop(t, db, bucket, []byte("0"), v, nil, true)
		}
	})
}

func TestTx_RPushRaw(t *testing.T) {
	bucket := "bucket"
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		seq := uint64(100000)
		for i := 0; i <= 100; i++ {
			key := utils.EncodeListKey([]byte("0"), seq)
			seq++
			txPushRaw(t, db, bucket, key, testutils.GetTestBytes(i), false, nil, nil)
		}

		txPush(t, db, bucket, []byte("012|sas"), testutils.GetTestBytes(0), false, ErrSeparatorForListKey, nil)

		for i := 0; i <= 100; i++ {
			v := testutils.GetTestBytes(100 - i)
			txPop(t, db, bucket, []byte("0"), v, nil, false)
		}
	})
}

func TestTx_LPop(t *testing.T) {
	bucket := "bucket"

	// Calling LPop on a non-existent list
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		txPop(t, db, bucket, testutils.GetTestBytes(0), nil, ErrListNotFound, true)
	})

	// Insert some values for a key and validate them by using LPop
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		pushDataByStartEnd(t, db, bucket, 0, 0, 2, true)
		for i := 0; i < 3; i++ {
			txPop(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(2-i), nil, true)
		}
	})
}

func TestTx_RPop(t *testing.T) {
	bucket := "bucket"

	// Calling RPop on a non-existent list
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		txPop(t, db, bucket, testutils.GetTestBytes(0), nil, ErrListNotFound, false)
	})

	// Calling RPop on a list with added data
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		pushDataByStartEnd(t, db, bucket, 0, 0, 2, false)

		txPop(t, db, "fake_bucket", testutils.GetTestBytes(0), nil, ErrBucketNotExist, false)

		for i := 0; i < 3; i++ {
			txPop(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(2-i), nil, false)
		}

		txPop(t, db, bucket, testutils.GetTestBytes(0), nil, ErrEmptyList, false)
	})
}

func TestTx_LRange(t *testing.T) {
	bucket := "bucket"

	// Calling LRange on a non-existent list
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)

		txLRange(t, db, bucket, testutils.GetTestBytes(0), 0, -1, 0, nil, ErrListNotFound)
	})

	// Calling LRange on a list with added data
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)

		pushDataByStartEnd(t, db, bucket, 0, 0, 2, true)

		txLRange(t, db, bucket, testutils.GetTestBytes(0), 0, -1, 3, [][]byte{
			testutils.GetTestBytes(2), testutils.GetTestBytes(1), testutils.GetTestBytes(0),
		}, nil)

		for i := 0; i < 3; i++ {
			txPop(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(2-i), nil, true)
		}

		txLRange(t, db, bucket, testutils.GetTestBytes(0), 0, -1, 0, nil, nil)
	})
}

func TestTx_LRem(t *testing.T) {
	bucket := "bucket"

	// Calling LRem on a non-existent list
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		txLRem(t, db, bucket, testutils.GetTestBytes(0), 1, testutils.GetTestBytes(0), ErrListNotFound)
	})

	// A basic calling for LRem
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)

		pushDataByStartEnd(t, db, bucket, 0, 0, 3, true)

		txLRem(t, db, bucket, testutils.GetTestBytes(0), 1, testutils.GetTestBytes(0), nil)
		txLRange(t, db, bucket, testutils.GetTestBytes(0), 0, -1, 3, [][]byte{
			testutils.GetTestBytes(3), testutils.GetTestBytes(2), testutils.GetTestBytes(1),
		}, nil)
		txLRem(t, db, bucket, testutils.GetTestBytes(0), 4, testutils.GetTestBytes(0), ErrCount)
		txLRem(t, db, bucket, testutils.GetTestBytes(0), 1, testutils.GetTestBytes(1), nil)
	})

	// Calling LRem with count > 0
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)

		count := 3

		pushDataByValues(t, db, bucket, 1, true, 0, 1, 0, 1, 0, 1, 0, 1)

		txLRange(t, db, bucket, testutils.GetTestBytes(1), 0, -1, 8, [][]byte{
			testutils.GetTestBytes(1), testutils.GetTestBytes(0), testutils.GetTestBytes(1), testutils.GetTestBytes(0),
			testutils.GetTestBytes(1), testutils.GetTestBytes(0), testutils.GetTestBytes(1), testutils.GetTestBytes(0),
		}, nil)
		txLRem(t, db, bucket, testutils.GetTestBytes(1), count, testutils.GetTestBytes(0), nil)
		txLRange(t, db, bucket, testutils.GetTestBytes(1), 0, -1, 5, [][]byte{
			testutils.GetTestBytes(1), testutils.GetTestBytes(1), testutils.GetTestBytes(1), testutils.GetTestBytes(1), testutils.GetTestBytes(0),
		}, nil)
	})

	// Calling LRem with count == 0
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		count := 0

		pushDataByValues(t, db, bucket, 1, true, 0, 1, 0, 1, 0, 1, 0, 1)

		txLRange(t, db, bucket, testutils.GetTestBytes(1), 0, -1, 8, [][]byte{
			testutils.GetTestBytes(1), testutils.GetTestBytes(0), testutils.GetTestBytes(1), testutils.GetTestBytes(0),
			testutils.GetTestBytes(1), testutils.GetTestBytes(0), testutils.GetTestBytes(1), testutils.GetTestBytes(0),
		}, nil)
		txLRem(t, db, bucket, testutils.GetTestBytes(1), count, testutils.GetTestBytes(0), nil)
		txLRange(t, db, bucket, testutils.GetTestBytes(1), 0, -1, 4, [][]byte{
			testutils.GetTestBytes(1), testutils.GetTestBytes(1), testutils.GetTestBytes(1), testutils.GetTestBytes(1),
		}, nil)
	})

	// Calling LRem with count < 0
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)

		count := -3

		pushDataByValues(t, db, bucket, 1, true, 0, 1, 0, 1, 0, 1, 0, 1)

		txLRange(t, db, bucket, testutils.GetTestBytes(1), 0, -1, 8, [][]byte{
			testutils.GetTestBytes(1), testutils.GetTestBytes(0), testutils.GetTestBytes(1), testutils.GetTestBytes(0),
			testutils.GetTestBytes(1), testutils.GetTestBytes(0), testutils.GetTestBytes(1), testutils.GetTestBytes(0),
		}, nil)
		txLRem(t, db, bucket, testutils.GetTestBytes(1), count, testutils.GetTestBytes(0), nil)
		txLRange(t, db, bucket, testutils.GetTestBytes(1), 0, -1, 5, [][]byte{
			testutils.GetTestBytes(1), testutils.GetTestBytes(0), testutils.GetTestBytes(1), testutils.GetTestBytes(1), testutils.GetTestBytes(1),
		}, nil)
	})
}

func TestTx_LTrim(t *testing.T) {
	bucket := "bucket"

	// Calling LTrim on a non-existent list
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		txLTrim(t, db, bucket, testutils.GetTestBytes(0), 0, 1, ErrListNotFound)
	})

	// test LTrim to clear list data
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		n := 20
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		pushDataByStartEnd(t, db, bucket, n, 0, 6, true)
		txLRange(
			t, db, bucket,
			testutils.GetTestBytes(n),
			0, 8, 7,
			[][]byte{
				testutils.GetTestBytes(6),
				testutils.GetTestBytes(5),
				testutils.GetTestBytes(4),
				testutils.GetTestBytes(3),
				testutils.GetTestBytes(2),
				testutils.GetTestBytes(1),
				testutils.GetTestBytes(0),
			}, nil)
		txLTrim(t, db, bucket, testutils.GetTestBytes(n), 2, 5, nil)
		txLRange(
			t, db, bucket,
			testutils.GetTestBytes(n),
			0, 8, 4,
			[][]byte{
				testutils.GetTestBytes(4),
				testutils.GetTestBytes(3),
				testutils.GetTestBytes(2),
				testutils.GetTestBytes(1),
			}, nil)

		err = db.View(func(tx *Tx) error {
			l, err := tx.LSize(bucket, testutils.GetTestBytes(n))
			assert.NoError(t, err)
			assert.Equal(t, 4, l)

			item, err := tx.LPeek(bucket, testutils.GetTestBytes(n))
			assert.NoError(t, err)
			assert.Equal(t, testutils.GetTestBytes(4), item)
			return nil
		})
		assert.NoError(t, err)
	})

	// Calling LTrim on a list with added data and use LRange to validate it
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		pushDataByStartEnd(t, db, bucket, 0, 0, 2, true)
		txLTrim(t, db, bucket, testutils.GetTestBytes(0), 0, 1, nil)

		txLRange(t, db, bucket, testutils.GetTestBytes(0), 0, -1, 2, [][]byte{
			testutils.GetTestBytes(2), testutils.GetTestBytes(1),
		}, nil)
	})

	// Calling LTrim with incorrect start and end
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)

		for i := 0; i < 3; i++ {
			txPush(t, db, bucket, testutils.GetTestBytes(2), testutils.GetTestBytes(i), true, nil, nil)
		}
		txLTrim(t, db, bucket, testutils.GetTestBytes(2), 0, -10, ErrStartOrEnd)
	})
}

func TestTx_LSize(t *testing.T) {
	bucket := "bucket"

	// Calling LSize on a non-existent list
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		txLSize(t, db, bucket, testutils.GetTestBytes(0), 0, ErrListNotFound)
	})

	// Calling LSize after adding some values
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		pushDataByStartEnd(t, db, bucket, 0, 0, 2, false)
		txLSize(t, db, bucket, testutils.GetTestBytes(0), 3, nil)
	})
}

func TestTx_LRemByIndex(t *testing.T) {
	bucket := "bucket"

	// Calling LRemByIndex on a newly created empty list
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		txLRemByIndex(t, db, bucket, testutils.GetTestBytes(0), nil)
	})

	// Calling LRemByIndex with len(indexes) == 0
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		pushDataByValues(t, db, bucket, 0, true, 0)
		txLRemByIndex(t, db, bucket, testutils.GetTestBytes(0), nil)
	})

	// Calling LRemByIndex with a expired bucket name
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		pushDataByValues(t, db, bucket, 0, true, 0)
		txExpireList(t, db, bucket, testutils.GetTestBytes(0), 1, nil)
		time.Sleep(3 * time.Second)
		txLRemByIndex(t, db, bucket, testutils.GetTestBytes(0), ErrListNotFound)
	})

	// Calling LRemByIndex on a list with added data and use LRange to validate it
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		pushDataByStartEnd(t, db, bucket, 0, 0, 2, false)
		txLRemByIndex(t, db, bucket, testutils.GetTestBytes(0), nil, 1, 0, 8, -8, 88, -88)
		txLRange(t, db, bucket, testutils.GetTestBytes(0), 0, -1, 1, [][]byte{
			testutils.GetTestBytes(2),
		}, nil)
	})
}

func TestTx_ExpireList(t *testing.T) {
	bucket := "bucket"

	// Verify that the list with expiration time expires normally
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		pushDataByStartEnd(t, db, bucket, 0, 0, 3, false)
		txLRange(t, db, bucket, testutils.GetTestBytes(0), 0, -1, 4, [][]byte{
			testutils.GetTestBytes(0), testutils.GetTestBytes(1), testutils.GetTestBytes(2), testutils.GetTestBytes(3),
		}, nil)

		txExpireList(t, db, bucket, testutils.GetTestBytes(0), 1, nil)
		time.Sleep(time.Second)
		txLRange(t, db, bucket, testutils.GetTestBytes(0), 0, -1, 0, nil, ErrListNotFound)
	})

	// Verify that the list with persistent time
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		pushDataByStartEnd(t, db, bucket, 0, 0, 3, false)
		txExpireList(t, db, bucket, testutils.GetTestBytes(0), Persistent, nil)
		time.Sleep(time.Second)
		txLRange(t, db, bucket, testutils.GetTestBytes(0), 0, -1, 4, [][]byte{
			testutils.GetTestBytes(0), testutils.GetTestBytes(1), testutils.GetTestBytes(2), testutils.GetTestBytes(3),
		}, nil)
	})
}

func TestTx_LKeys(t *testing.T) {
	bucket := "bucket"

	// Calling LKeys after adding some keys
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		pushDataByValues(t, db, bucket, 10, false, 0)
		pushDataByValues(t, db, bucket, 11, false, 1)
		pushDataByValues(t, db, bucket, 12, false, 2)
		pushDataByValues(t, db, bucket, 23, false, 3)

		txLKeys(t, db, bucket, "*", 4, nil, func(keys []string) bool {
			return true
		})

		txLKeys(t, db, bucket, "*", 2, nil, func(keys []string) bool {
			return len(keys) != 2
		})

		txLKeys(t, db, bucket, "nutsdb-00000001*", 3, nil, func(keys []string) bool {
			return true
		})
	})
}

func TestTx_GetListTTL(t *testing.T) {
	bucket := "bucket"

	// Verify TTL of list
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		pushDataByStartEnd(t, db, bucket, 0, 0, 3, false)

		txGetListTTL(t, db, bucket, testutils.GetTestBytes(0), uint32(0), nil)
		txExpireList(t, db, bucket, testutils.GetTestBytes(0), uint32(1), nil)
		txGetListTTL(t, db, bucket, testutils.GetTestBytes(0), uint32(1), nil)

		time.Sleep(3 * time.Second)
		txGetListTTL(t, db, bucket, testutils.GetTestBytes(0), uint32(0), ErrListNotFound)
	})
}

func TestTx_ListEntryIdxMode_HintKeyValAndRAMIdxMode(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)

	opts := DefaultOptions
	opts.EntryIdxMode = HintKeyValAndRAMIdxMode

	// HintKeyValAndRAMIdxMode
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		err := db.Update(func(tx *Tx) error {
			err := tx.LPush(bucket, key, []byte("d"), []byte("c"), []byte("b"), []byte("a"))
			require.NoError(t, err)

			return nil
		})
		require.NoError(t, err)

		listIdx := db.Index.List.GetWithDefault(1)
		item, ok := listIdx.Items[string(key)].PopMin()
		r := item.Record
		require.True(t, ok)
		require.NotNil(t, r.Value)
		require.Equal(t, []byte("a"), r.Value)
	})
}

func TestTx_ListEntryIdxMode_HintKeyAndRAMIdxMode(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)

	opts := &DefaultOptions
	opts.EntryIdxMode = HintKeyAndRAMIdxMode

	// HintKeyAndRAMIdxMode
	runNutsDBTest(t, opts, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		err := db.Update(func(tx *Tx) error {
			err := tx.LPush(bucket, key, []byte("d"), []byte("c"), []byte("b"), []byte("a"))
			require.NoError(t, err)

			return nil
		})
		require.NoError(t, err)

		listIdx := db.Index.List.GetWithDefault(1)
		item, ok := listIdx.Items[string(key)].PopMin()
		r := item.Record
		require.True(t, ok)
		require.Nil(t, r.Value)

		val, err := db.getValueByRecord(r)
		require.NoError(t, err)
		require.Equal(t, []byte("a"), val)
	})
}

// TestTx_PushPopPushSequence tests Push->Pop->Push sequence numbers
func TestTx_PushPopPushSequence(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureList, bucket, nil)

		// Push 3 elements
		txPush(t, db, bucket, key, testutils.GetTestBytes(0), false, nil, nil)
		txPush(t, db, bucket, key, testutils.GetTestBytes(1), false, nil, nil)
		txPush(t, db, bucket, key, testutils.GetTestBytes(2), false, nil, nil)

		// Pop one from left
		txPop(t, db, bucket, key, testutils.GetTestBytes(0), nil, true)

		// Push another to right
		txPush(t, db, bucket, key, testutils.GetTestBytes(3), false, nil, nil)

		// Pop from right
		txPop(t, db, bucket, key, testutils.GetTestBytes(3), nil, false)

		// Push to left
		txPush(t, db, bucket, key, testutils.GetTestBytes(99), true, nil, nil)

		// Verify final order: [99, 1, 2]
		txLRange(t, db, bucket, key, 0, -1, 3, [][]byte{
			testutils.GetTestBytes(99), testutils.GetTestBytes(1), testutils.GetTestBytes(2),
		}, nil)
	})
}

// TestTx_ListRecoveryAfterRestart tests that list data is correctly recovered after DB restart
func TestTx_ListRecoveryAfterRestart(t *testing.T) {
	bucket := "list_bucket"
	key := testutils.GetTestBytes(0)

	dir := path.Join(t.TempDir(), "test_nutsdb_list_recovery")
	defer os.RemoveAll(dir)

	// Step 1: Create DB and insert data
	opts := DefaultOptions
	opts.Dir = dir
	db, err := Open(opts)
	require.NoError(t, err)

	// Create bucket and insert data
	txCreateBucket(t, db, DataStructureList, bucket, nil)

	// Insert 10 elements using RPush
	for i := 0; i < 10; i++ {
		txPush(t, db, bucket, key, testutils.GetTestBytes(i), false, nil, nil)
	}

	// Verify data before closing
	txLSize(t, db, bucket, key, 10, nil)
	txLRange(t, db, bucket, key, 0, -1, 10, [][]byte{
		testutils.GetTestBytes(0), testutils.GetTestBytes(1), testutils.GetTestBytes(2), testutils.GetTestBytes(3), testutils.GetTestBytes(4),
		testutils.GetTestBytes(5), testutils.GetTestBytes(6), testutils.GetTestBytes(7), testutils.GetTestBytes(8), testutils.GetTestBytes(9),
	}, nil)

	// Step 2: Close DB
	err = db.Close()
	require.NoError(t, err)

	// Step 3: Reopen DB
	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()

	// Step 4: Verify data after recovery
	txLSize(t, db, bucket, key, 10, nil)
	txLRange(t, db, bucket, key, 0, -1, 10, [][]byte{
		testutils.GetTestBytes(0), testutils.GetTestBytes(1), testutils.GetTestBytes(2), testutils.GetTestBytes(3), testutils.GetTestBytes(4),
		testutils.GetTestBytes(5), testutils.GetTestBytes(6), testutils.GetTestBytes(7), testutils.GetTestBytes(8), testutils.GetTestBytes(9),
	}, nil)

	// Step 5: Test continued operations after recovery
	txPush(t, db, bucket, key, testutils.GetTestBytes(10), false, nil, nil)
	txPush(t, db, bucket, key, testutils.GetTestBytes(99), true, nil, nil)

	// Verify final state
	txLSize(t, db, bucket, key, 12, nil)
	txLRange(t, db, bucket, key, 0, 0, 1, [][]byte{testutils.GetTestBytes(99)}, nil)
	txLRange(t, db, bucket, key, 11, 11, 1, [][]byte{testutils.GetTestBytes(10)}, nil)
}

// TestTx_ListRecoveryWithMixedOperations tests recovery after complex operations
func TestTx_ListRecoveryWithMixedOperations(t *testing.T) {
	bucket := "list_bucket"
	key := testutils.GetTestBytes(0)

	dir := path.Join(t.TempDir(), "test_nutsdb_list_recovery_mixed")
	defer os.RemoveAll(dir)

	opts := DefaultOptions
	opts.Dir = dir
	db, err := Open(opts)
	require.NoError(t, err)

	// Create bucket
	txCreateBucket(t, db, DataStructureList, bucket, nil)

	// RPush 5 elements: [0,1,2,3,4]
	for i := 0; i < 5; i++ {
		txPush(t, db, bucket, key, testutils.GetTestBytes(i), false, nil, nil)
	}

	// LPush 2 elements: [98,99,0,1,2,3,4]
	txPush(t, db, bucket, key, testutils.GetTestBytes(99), true, nil, nil)
	txPush(t, db, bucket, key, testutils.GetTestBytes(98), true, nil, nil)

	// Close and reopen
	err = db.Close()
	require.NoError(t, err)

	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()

	// Verify recovered state - should have 7 elements
	txLSize(t, db, bucket, key, 7, nil)
	txLRange(t, db, bucket, key, 0, -1, 7, [][]byte{
		testutils.GetTestBytes(98), testutils.GetTestBytes(99), testutils.GetTestBytes(0), testutils.GetTestBytes(1),
		testutils.GetTestBytes(2), testutils.GetTestBytes(3), testutils.GetTestBytes(4),
	}, nil)
}

// TestTx_ListRecoveryMultipleLists tests recovery of multiple lists
func TestTx_ListRecoveryMultipleLists(t *testing.T) {
	bucket := "list_bucket"

	dir := path.Join(t.TempDir(), "test_nutsdb_list_recovery_multiple")
	defer os.RemoveAll(dir)

	opts := DefaultOptions
	opts.Dir = dir
	db, err := Open(opts)
	require.NoError(t, err)

	// Create bucket
	txCreateBucket(t, db, DataStructureList, bucket, nil)

	// Create multiple lists
	for listIdx := 0; listIdx < 3; listIdx++ {
		key := testutils.GetTestBytes(listIdx)
		for i := 0; i < 5; i++ {
			txPush(t, db, bucket, key, testutils.GetTestBytes(listIdx*100+i), false, nil, nil)
		}
	}

	// Close and reopen
	err = db.Close()
	require.NoError(t, err)

	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()

	// Verify all lists recovered correctly
	for listIdx := 0; listIdx < 3; listIdx++ {
		key := testutils.GetTestBytes(listIdx)
		txLSize(t, db, bucket, key, 5, nil)

		expected := make([][]byte, 5)
		for i := 0; i < 5; i++ {
			expected[i] = testutils.GetTestBytes(listIdx*100 + i)
		}
		txLRange(t, db, bucket, key, 0, -1, 5, expected, nil)
	}
}
