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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func pushDataByStartEnd(t *testing.T, db *DB, bucket string, key int, start, end int, isLeft bool) {
	for i := start; i <= end; i++ {
		txPush(t, db, bucket, GetTestBytes(key), GetTestBytes(i), isLeft, nil, nil)
	}
}

func pushDataByValues(t *testing.T, db *DB, bucket string, key int, isLeft bool, values ...int) {
	for _, v := range values {
		txPush(t, db, bucket, GetTestBytes(key), GetTestBytes(v), isLeft, nil, nil)
	}
}

func TestTx_RPush(t *testing.T) {
	bucket := "bucket"

	// 1. Insert values for some keys by using RPush
	// 2. Validate values for these keys by using RPop
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		pushDataByStartEnd(t, db, bucket, 0, 0, 9, false)
		pushDataByStartEnd(t, db, bucket, 1, 10, 19, false)
		pushDataByStartEnd(t, db, bucket, 2, 20, 29, false)

		for i := 0; i < 10; i++ {
			txPop(t, db, bucket, GetTestBytes(0), GetTestBytes(9-i), nil, false)
		}
		for i := 10; i < 20; i++ {
			txPop(t, db, bucket, GetTestBytes(1), GetTestBytes(29-i), nil, false)
		}
		for i := 20; i < 30; i++ {
			txPop(t, db, bucket, GetTestBytes(2), GetTestBytes(49-i), nil, false)
		}
	})
}

func TestTx_LPush(t *testing.T) {
	bucket := "bucket"

	// 1. Insert values for some keys by using LPush
	// 2. Validate values for these keys by using LPop
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		pushDataByStartEnd(t, db, bucket, 0, 0, 9, true)
		pushDataByStartEnd(t, db, bucket, 1, 10, 19, true)
		pushDataByStartEnd(t, db, bucket, 2, 20, 29, true)

		for i := 0; i < 10; i++ {
			txPop(t, db, bucket, GetTestBytes(0), GetTestBytes(9-i), nil, true)
		}
		for i := 10; i < 20; i++ {
			txPop(t, db, bucket, GetTestBytes(1), GetTestBytes(29-i), nil, true)
		}
		for i := 20; i < 30; i++ {
			txPop(t, db, bucket, GetTestBytes(2), GetTestBytes(49-i), nil, true)
		}
	})
}

func TestTx_LPushRaw(t *testing.T) {
	bucket := "bucket"
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		seq := uint64(100000)
		for i := 0; i <= 100; i++ {
			key := encodeListKey([]byte("0"), seq)
			seq--
			txPushRaw(t, db, bucket, key, GetTestBytes(i), true, nil, nil)
		}

		for i := 0; i <= 100; i++ {
			v := GetTestBytes(100 - i)
			txPop(t, db, bucket, []byte("0"), v, nil, true)
		}
	})
}

func TestTx_RPushRaw(t *testing.T) {
	bucket := "bucket"
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		seq := uint64(100000)
		for i := 0; i <= 100; i++ {
			key := encodeListKey([]byte("0"), seq)
			seq++
			txPushRaw(t, db, bucket, key, GetTestBytes(i), false, nil, nil)
		}

		for i := 0; i <= 100; i++ {
			v := GetTestBytes(100 - i)
			txPop(t, db, bucket, []byte("0"), v, nil, false)
		}
	})
}

func TestTx_LPop(t *testing.T) {
	bucket := "bucket"

	// Calling LPop on a non-existent list
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txPop(t, db, bucket, GetTestBytes(0), nil, ErrListNotFound, true)
	})

	// Insert some values for a key and validate them by using LPop
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		pushDataByStartEnd(t, db, bucket, 0, 0, 2, true)
		for i := 0; i < 3; i++ {
			txPop(t, db, bucket, GetTestBytes(0), GetTestBytes(2-i), nil, true)
		}
	})
}

func TestTx_RPop(t *testing.T) {
	bucket := "bucket"

	// Calling RPop on a non-existent list
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txPop(t, db, bucket, GetTestBytes(0), nil, ErrListNotFound, false)
	})

	// Calling RPop on a list with added data
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		pushDataByStartEnd(t, db, bucket, 0, 0, 2, false)

		txPop(t, db, "fake_bucket", GetTestBytes(0), nil, ErrListNotFound, false)

		for i := 0; i < 3; i++ {
			txPop(t, db, bucket, GetTestBytes(0), GetTestBytes(2-i), nil, false)
		}

		txPop(t, db, bucket, GetTestBytes(0), nil, ErrEmptyList, false)
	})
}

func TestTx_LRange(t *testing.T) {
	bucket := "bucket"

	// Calling LRange on a non-existent list
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txLRange(t, db, bucket, GetTestBytes(0), 0, -1, 0, nil, ErrListNotFound)
	})

	// Calling LRange on a list with added data
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		pushDataByStartEnd(t, db, bucket, 0, 0, 2, true)

		txLRange(t, db, bucket, GetTestBytes(0), 0, -1, 3, [][]byte{
			GetTestBytes(2), GetTestBytes(1), GetTestBytes(0),
		}, nil)

		for i := 0; i < 3; i++ {
			txPop(t, db, bucket, GetTestBytes(0), GetTestBytes(2-i), nil, true)
		}

		txLRange(t, db, bucket, GetTestBytes(0), 0, -1, 0, nil, nil)
	})
}

func TestTx_LRem(t *testing.T) {
	bucket := "bucket"

	// Calling LRem on a non-existent list
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txLRem(t, db, bucket, GetTestBytes(0), 1, GetTestBytes(0), ErrListNotFound)
	})

	// A basic calling for LRem
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		pushDataByStartEnd(t, db, bucket, 0, 0, 3, true)

		txLRem(t, db, bucket, GetTestBytes(0), 1, GetTestBytes(0), nil)
		txLRange(t, db, bucket, GetTestBytes(0), 0, -1, 3, [][]byte{
			GetTestBytes(3), GetTestBytes(2), GetTestBytes(1),
		}, nil)
		txLRem(t, db, bucket, GetTestBytes(0), 4, GetTestBytes(0), ErrCount)
		txLRem(t, db, bucket, GetTestBytes(0), 1, GetTestBytes(1), nil)
	})

	// Calling LRem with count > 0
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		count := 3

		pushDataByValues(t, db, bucket, 1, true, 0, 1, 0, 1, 0, 1, 0, 1)

		txLRange(t, db, bucket, GetTestBytes(1), 0, -1, 8, [][]byte{
			GetTestBytes(1), GetTestBytes(0), GetTestBytes(1), GetTestBytes(0),
			GetTestBytes(1), GetTestBytes(0), GetTestBytes(1), GetTestBytes(0),
		}, nil)
		txLRem(t, db, bucket, GetTestBytes(1), count, GetTestBytes(0), nil)
		txLRange(t, db, bucket, GetTestBytes(1), 0, -1, 5, [][]byte{
			GetTestBytes(1), GetTestBytes(1), GetTestBytes(1), GetTestBytes(1), GetTestBytes(0),
		}, nil)
	})

	// Calling LRem with count == 0
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		count := 0

		pushDataByValues(t, db, bucket, 1, true, 0, 1, 0, 1, 0, 1, 0, 1)

		txLRange(t, db, bucket, GetTestBytes(1), 0, -1, 8, [][]byte{
			GetTestBytes(1), GetTestBytes(0), GetTestBytes(1), GetTestBytes(0),
			GetTestBytes(1), GetTestBytes(0), GetTestBytes(1), GetTestBytes(0),
		}, nil)
		txLRem(t, db, bucket, GetTestBytes(1), count, GetTestBytes(0), nil)
		txLRange(t, db, bucket, GetTestBytes(1), 0, -1, 4, [][]byte{
			GetTestBytes(1), GetTestBytes(1), GetTestBytes(1), GetTestBytes(1),
		}, nil)
	})

	// Calling LRem with count < 0
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		count := -3

		pushDataByValues(t, db, bucket, 1, true, 0, 1, 0, 1, 0, 1, 0, 1)

		txLRange(t, db, bucket, GetTestBytes(1), 0, -1, 8, [][]byte{
			GetTestBytes(1), GetTestBytes(0), GetTestBytes(1), GetTestBytes(0),
			GetTestBytes(1), GetTestBytes(0), GetTestBytes(1), GetTestBytes(0),
		}, nil)
		txLRem(t, db, bucket, GetTestBytes(1), count, GetTestBytes(0), nil)
		txLRange(t, db, bucket, GetTestBytes(1), 0, -1, 5, [][]byte{
			GetTestBytes(1), GetTestBytes(0), GetTestBytes(1), GetTestBytes(1), GetTestBytes(1),
		}, nil)
	})
}

func TestTx_LTrim(t *testing.T) {
	bucket := "bucket"

	// Calling LTrim on a non-existent list
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txLTrim(t, db, bucket, GetTestBytes(0), 0, 1, ErrListNotFound)
	})

	// Calling LTrim on a list with added data and use LRange to validate it
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		pushDataByStartEnd(t, db, bucket, 0, 0, 2, true)
		txLTrim(t, db, bucket, GetTestBytes(0), 0, 1, nil)

		txLRange(t, db, bucket, GetTestBytes(0), 0, -1, 2, [][]byte{
			GetTestBytes(2), GetTestBytes(1),
		}, nil)
	})

	// Calling LTrim with incorrect start and end
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 3; i++ {
			txPush(t, db, bucket, GetTestBytes(2), GetTestBytes(i), true, nil, nil)
		}
		txLTrim(t, db, bucket, GetTestBytes(2), 0, -10, ErrStartOrEnd)
	})
}

func TestTx_LSize(t *testing.T) {
	bucket := "bucket"

	// Calling LSize on a non-existent list
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txLSize(t, db, bucket, GetTestBytes(0), 0, ErrListNotFound)
	})

	// Calling LSize after adding some values
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		pushDataByStartEnd(t, db, bucket, 0, 0, 2, false)
		txLSize(t, db, bucket, GetTestBytes(0), 3, nil)
	})
}

func TestTx_LRemByIndex(t *testing.T) {
	bucket := "bucket"

	// Calling LRemByIndex on a non-existent list
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txLRemByIndex(t, db, bucket, GetTestBytes(0), ErrListNotFound)
	})

	// Calling LRemByIndex with len(indexes) == 0
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		pushDataByValues(t, db, bucket, 0, true, 0)
		txLRemByIndex(t, db, bucket, GetTestBytes(0), nil)
	})

	// Calling LRemByIndex with a expired bucket name
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		pushDataByValues(t, db, bucket, 0, true, 0)
		txExpireList(t, db, bucket, GetTestBytes(0), 1, nil)
		time.Sleep(3 * time.Second)
		txLRemByIndex(t, db, bucket, GetTestBytes(0), ErrListNotFound)
	})

	// Calling LRemByIndex on a list with added data and use LRange to validate it
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		pushDataByStartEnd(t, db, bucket, 0, 0, 2, false)
		txLRemByIndex(t, db, bucket, GetTestBytes(0), nil, 1, 0, 8, -8, 88, -88)
		txLRange(t, db, bucket, GetTestBytes(0), 0, -1, 1, [][]byte{
			GetTestBytes(2),
		}, nil)
	})
}

func TestTx_ExpireList(t *testing.T) {
	bucket := "bucket"

	// Verify that the list with expiration time expires normally
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		pushDataByStartEnd(t, db, bucket, 0, 0, 3, false)
		txLRange(t, db, bucket, GetTestBytes(0), 0, -1, 4, [][]byte{
			GetTestBytes(0), GetTestBytes(1), GetTestBytes(2), GetTestBytes(3),
		}, nil)

		txExpireList(t, db, bucket, GetTestBytes(0), 1, nil)
		time.Sleep(time.Second)
		txLRange(t, db, bucket, GetTestBytes(0), 0, -1, 0, nil, ErrListNotFound)
	})

	// Verify that the list with persistent time
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		pushDataByStartEnd(t, db, bucket, 0, 0, 3, false)
		txExpireList(t, db, bucket, GetTestBytes(0), Persistent, nil)
		time.Sleep(time.Second)
		txLRange(t, db, bucket, GetTestBytes(0), 0, -1, 4, [][]byte{
			GetTestBytes(0), GetTestBytes(1), GetTestBytes(2), GetTestBytes(3),
		}, nil)
	})
}

func TestTx_LKeys(t *testing.T) {
	bucket := "bucket"

	// Calling LKeys after adding some keys
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
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
		pushDataByStartEnd(t, db, bucket, 0, 0, 3, false)

		txGetListTTL(t, db, bucket, GetTestBytes(0), uint32(0), nil)
		txExpireList(t, db, bucket, GetTestBytes(0), uint32(1), nil)
		txGetListTTL(t, db, bucket, GetTestBytes(0), uint32(1), nil)

		time.Sleep(3 * time.Second)
		txGetListTTL(t, db, bucket, GetTestBytes(0), uint32(0), ErrListNotFound)
	})
}

func TestTx_ListEntryIdxMode_HintKeyValAndRAMIdxMode(t *testing.T) {
	bucket := "bucket"
	key := GetTestBytes(0)

	opts := DefaultOptions
	opts.EntryIdxMode = HintKeyValAndRAMIdxMode

	// HintKeyValAndRAMIdxMode
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		err := db.Update(func(tx *Tx) error {
			err := tx.LPush(bucket, key, []byte("d"), []byte("c"), []byte("b"), []byte("a"))
			require.NoError(t, err)

			return nil
		})
		require.NoError(t, err)

		listIdx := db.Index.list.getWithDefault(bucket)
		item, ok := listIdx.Items[string(key)].PopMin()
		r := item.r
		require.True(t, ok)
		require.NotNil(t, r.V)
		require.Equal(t, []byte("a"), r.V)
	})
}

func TestTx_ListEntryIdxMode_HintKeyAndRAMIdxMode(t *testing.T) {
	bucket := "bucket"
	key := GetTestBytes(0)

	opts := &DefaultOptions
	opts.EntryIdxMode = HintKeyAndRAMIdxMode

	// HintKeyAndRAMIdxMode
	runNutsDBTest(t, opts, func(t *testing.T, db *DB) {
		err := db.Update(func(tx *Tx) error {
			err := tx.LPush(bucket, key, []byte("d"), []byte("c"), []byte("b"), []byte("a"))
			require.NoError(t, err)

			return nil
		})
		require.NoError(t, err)

		listIdx := db.Index.list.getWithDefault(bucket)
		item, ok := listIdx.Items[string(key)].PopMin()
		r := item.r
		require.True(t, ok)
		require.Nil(t, r.V)

		val, err := db.getValueByRecord(r)
		require.NoError(t, err)
		require.Equal(t, []byte("a"), val)
	})
}
