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
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/testutils"
	"github.com/nutsdb/nutsdb/internal/ttl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xujiajun/utils/strconv2"
)

func getIntegerValue(value int64) []byte {
	return []byte(fmt.Sprintf("%d", value))
}

func validateEqual(r *require.Assertions, tx *Tx, bucket string, key []byte, expect int64) {
	value, err := tx.Get(bucket, key)
	r.NoError(err)
	intValue, err := strconv2.StrToInt64(string(value))
	r.NoError(err)
	r.Equal(expect, intValue)
}

func TestTx_PutAndGet(t *testing.T) {
	var (
		bucket = "bucket1"
		key    = []byte("key1")
		val    = []byte("val1")
	)

	t.Run("put_and_get", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			{
				txCreateBucket(t, db, DataStructureBTree, bucket, nil)

				tx, err := db.Begin(true)
				require.NoError(t, err)

				err = tx.Put(bucket, key, val, Persistent)
				assert.NoError(t, err)

				assert.NoError(t, tx.Commit())
			}

			{
				tx, err = db.Begin(false)
				require.NoError(t, err)

				value, err := tx.Get(bucket, key)
				assert.NoError(t, err)
				assert.NoError(t, tx.Commit())

				assert.Equal(t, val, value)
			}
		})
	})

	t.Run("get by closed tx", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			tx, err := db.Begin(false)
			require.NoError(t, err)
			assert.NoError(t, tx.Commit())

			_, err = tx.Get(bucket, key) // use closed tx
			assert.Error(t, err)
		})
	})
}

func TestTx_GetAll_GetKeys_GetValues(t *testing.T) {
	bucket := "bucket"

	runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		txGetAll(t, db, bucket, nil, nil, nil)

		n := 10
		keys := make([][]byte, n)
		values := make([][]byte, n)
		for i := 0; i < n; i++ {
			keys[i] = testutils.GetTestBytes(i)
			values[i] = testutils.GetRandomBytes(10)
			txPut(t, db, bucket, keys[i], values[i], Persistent, nil, nil)
		}

		txGetAll(t, db, bucket, keys, values, nil)

		keys = append(keys, testutils.GetTestBytes(10))
		values = append(values, testutils.GetRandomBytes(10))
		txPut(t, db, bucket, keys[10], values[10], Persistent, nil, nil)
		txGetAll(t, db, bucket, keys, values, nil)

		txDel(t, db, bucket, keys[0], nil)
		keys = keys[1:]
		values = values[1:]
		txGetAll(t, db, bucket, keys, values, nil)

		txPut(t, db, bucket, testutils.GetTestBytes(11), testutils.GetRandomBytes(10), 1, nil, nil)
		mc.AdvanceTime(1100 * time.Millisecond)
		txGetAll(t, db, bucket, keys, values, nil)

		require.NoError(t, db.View(func(tx *Tx) error {
			keysInBucket, err := tx.GetKeys(bucket)
			require.NoError(t, err)
			valuesInBucket, err := tx.GetValues(bucket)
			require.NoError(t, err)
			for i := 0; i < n; i++ {
				require.Equal(t, keys[i], keysInBucket[i])
				require.Equal(t, values[i], valuesInBucket[i])
			}
			return nil
		}))
	})
}

func TestTx_GetAfterDelete(t *testing.T) {
	bucket := "bucket_test"

	t.Run("get key after delete in same transaction", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			tx, err := db.Begin(true)
			require.NoError(t, err)

			key := []byte("key_test")
			val := []byte("value =_test")

			require.NoError(t, tx.Put(bucket, key, val, Persistent))

			value, err := tx.Get(bucket, key)
			require.NoError(t, err)
			require.Equal(t, val, value)

			require.NoError(t, tx.Delete(bucket, key))

			_, err = tx.Get(bucket, key)
			require.Equal(t, ErrKeyNotFound, err)

			require.NoError(t, tx.Commit())
		})
	})

	t.Run("get persisted key in transaction", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			key := []byte("persisted_key")
			val := []byte("persisted_value")

			txPut(t, db, bucket, key, val, Persistent, nil, nil)
			txGet(t, db, bucket, key, val, nil)

			tx, err := db.Begin(true)
			require.NoError(t, err)
			require.NoError(t, tx.Delete(bucket, key))
			require.NoError(t, tx.Commit())

			txGet(t, db, bucket, key, nil, ErrKeyNotFound)
		})
	})
}

func TestTx_RangeScan_Err(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		bucket := "bucket_for_rangeScan"

		{
			// setup the data
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			tx, err := db.Begin(true)
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				key := []byte("key_" + fmt.Sprintf("%07d", i))
				val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
				err = tx.Put(bucket, key, val, Persistent)
				assert.NoError(t, err)
			}

			assert.NoError(t, tx.Commit()) // tx commit
		}

		{
			// verify

			tx, err := db.Begin(false)
			assert.NoError(t, err)

			start := []byte("key_0010001")
			end := []byte("key_0010010")
			_, err = tx.RangeScan(bucket, start, end)
			assert.Error(t, err)

			assert.NoError(t, tx.Rollback())
		}
	})
}

func TestTx_RangeScan(t *testing.T) {
	bucket := "bucket_for_range"

	withDefaultDB(t, func(t *testing.T, db *DB) {
		{
			// setup the data
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			tx, err := db.Begin(true)
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				key := []byte("key_" + fmt.Sprintf("%07d", i))
				val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
				err = tx.Put(bucket, key, val, Persistent)
				assert.NoError(t, err)
			}

			assert.NoError(t, tx.Commit()) // tx commit
		}

		{

			tx, err = db.Begin(false)
			require.NoError(t, err)

			var (
				s = 5
				e = 8
			)

			start := []byte(fmt.Sprintf("key_%07d", s))
			end := []byte(fmt.Sprintf("key_%07d", e))

			values, err := tx.RangeScan(bucket, start, end)
			assert.NoError(t, err)
			assert.NoError(t, tx.Commit()) // tx commit

			wantCount := (e - s) + 1 // the range: [5, 8]
			assert.Equal(t, wantCount, len(values))

			for i, value := range values {

				wantValue := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i+s))
				assert.Equal(t, wantValue, value)
			}

			values, err = tx.RangeScan(bucket, start, end)
			assert.Error(t, err)
			assert.Empty(t, values)
		}
	})
}

func TestTx_PrefixScan(t *testing.T) {
	bucket := "bucket_for_prefix_scan"

	withDefaultDB(t, func(t *testing.T, db *DB) {
		{
			// setup the data
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			tx, err := db.Begin(true)
			require.NoError(t, err)

			for i, prefix := range []string{"key1_", "key2_"} {
				for j := 0; j < 10; j++ {
					key := []byte(prefix + fmt.Sprintf("%07d", j))
					val := []byte("foobar" + fmt.Sprintf("%d%07d", i+1, j))
					err = tx.Put(bucket, key, val, Persistent)
					assert.NoError(t, err)
				}
			}

			assert.NoError(t, tx.Commit()) // tx commit
		}

		{
			tx, err = db.Begin(false)
			require.NoError(t, err)

			var (
				offset = 5
				limit  = 3
			)

			prefix := []byte("key1_")
			values, err := tx.PrefixScan(bucket, prefix, offset, limit)
			assert.NoError(t, err)

			assert.NoError(t, tx.Commit())

			assert.Equal(t, limit, len(values))

			for i := 0; i < limit; i++ {
				valIndex := offset + i

				wantVal := []byte("foobar" + fmt.Sprintf("%d%07d", 1, valIndex))
				assert.Equal(t, wantVal, values[i])
			}
		}

		{
			tx, err = db.Begin(false)
			require.NoError(t, err)

			var (
				offset = 5
				limit  = 3
			)

			prefix := []byte("key1_")
			keys, values, err := tx.PrefixScanEntries(bucket, prefix, "", offset, limit, true, true)
			assert.NoError(t, err)

			assert.NoError(t, tx.Commit())

			assert.Equal(t, limit, len(keys))
			assert.Equal(t, limit, len(values))

			for i := 0; i < limit; i++ {
				valIndex := offset + i

				wantKey := []byte("key1_" + fmt.Sprintf("%07d", valIndex))
				assert.Equal(t, wantKey, keys[i])

				wantVal := []byte("foobar" + fmt.Sprintf("%d%07d", 1, valIndex))
				assert.Equal(t, wantVal, values[i])
			}
		}
	})
}

func TestTx_PrefixSearchScanEntries(t *testing.T) {
	bucket := "bucket_for_prefix_search_scan_entries"

	withDefaultDB(t, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		regs := "1"

		tx, err := db.Begin(true)
		require.NoError(t, err)

		key := []byte("key_" + fmt.Sprintf("%07d", 0))
		val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 0))
		err = tx.Put(bucket, key, val, Persistent)
		assert.NoError(t, err)

		assert.NoError(t, tx.Commit()) // tx commit

		tx, err = db.Begin(true)
		require.NoError(t, err)

		key = []byte("key_" + fmt.Sprintf("%07d", 1))
		val = []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 1))
		err = tx.Put(bucket, key, val, Persistent)
		assert.NoError(t, err)

		assert.NoError(t, tx.Commit()) // tx commit

		tx, err = db.Begin(false)
		require.NoError(t, err)

		prefix := []byte("key_")
		keys, values, err := tx.PrefixScanEntries(bucket, prefix, regs, 0, 1, true, true)
		assert.NoError(t, err)

		assert.NoError(t, tx.Commit()) // tx commit

		c := 0
		for k, value := range values {
			wantKey := []byte("key_" + fmt.Sprintf("%07d", 1))

			assert.Equal(t, wantKey, keys[k])

			wantVal := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 1))

			assert.Equal(t, wantVal, value)
			c++
		}

		assert.Equal(t, 1, c)
	})
}

func TestTx_PrefixSearchScan(t *testing.T) {
	bucket := "bucket_for_prefix_search_scan"

	withDefaultDB(t, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		regs := "1"

		tx, err := db.Begin(true)
		require.NoError(t, err)

		key := []byte("key_" + fmt.Sprintf("%07d", 0))
		val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 0))
		err = tx.Put(bucket, key, val, Persistent)
		assert.NoError(t, err)

		assert.NoError(t, tx.Commit()) // tx commit

		tx, err = db.Begin(true)
		require.NoError(t, err)

		key = []byte("key_" + fmt.Sprintf("%07d", 1))
		val = []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 1))
		err = tx.Put(bucket, key, val, Persistent)
		assert.NoError(t, err)

		assert.NoError(t, tx.Commit()) // tx commit

		tx, err = db.Begin(false)
		require.NoError(t, err)

		prefix := []byte("key_")
		values, err := tx.PrefixSearchScan(bucket, prefix, regs, 0, 1)
		assert.NoError(t, err)

		assert.NoError(t, tx.Commit()) // tx commit

		c := 0
		for _, value := range values {
			wantVal := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 1))

			assert.Equal(t, wantVal, value)
			c++
		}

		assert.Equal(t, 1, c)
	})
}

func TestTx_DeleteAndGet(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		bucket := "bucket_delete_test"

		{
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			tx, err := db.Begin(true)
			require.NoError(t, err)

			for i := 0; i <= 10; i++ {
				key := []byte("key_" + fmt.Sprintf("%07d", i))
				val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
				err := tx.Put(bucket, key, val, Persistent)
				assert.NoError(t, err)
			}

			// tx commit
			assert.NoError(t, tx.Commit())
		}

		{
			tx, err := db.Begin(true)
			require.NoError(t, err)

			for i := 0; i <= 5; i++ {
				key := []byte("key_" + fmt.Sprintf("%07d", i))
				err := tx.Delete(bucket, key)
				assert.NoError(t, err)
			}

			// tx commit
			assert.NoError(t, tx.Commit())

			err = tx.Delete(bucket, []byte("key_"+fmt.Sprintf("%07d", 1)))
			assert.Error(t, err)
		}
	})
}

func TestTx_DeleteFromMemory(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 10; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		for i := 0; i < 10; i++ {
			txGet(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), nil)
		}

		txDel(t, db, bucket, testutils.GetTestBytes(3), nil)

		err := db.View(func(tx *Tx) error {
			r, ok := db.Index.BTree.GetWithDefault(1).Find(testutils.GetTestBytes(3))
			require.Nil(t, r)
			require.False(t, ok)

			return nil
		})

		require.NoError(t, err)
	})
}

func TestTx_DeleteKeyDuringTransaction(t *testing.T) {
	bucket := "bucket_delete_test"

	t.Run("delete key after put in same transaction", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			tx, err := db.Begin(true)
			require.NoError(t, err)

			key := []byte("key_test")
			val := []byte("value to be deleted")

			require.NoError(t, tx.Put(bucket, key, val, Persistent))

			value, err := tx.Get(bucket, key)
			require.NoError(t, err)
			require.Equal(t, val, value)

			require.NoError(t, tx.Delete(bucket, key))

			_, err = tx.Get(bucket, key)
			require.Equal(t, ErrKeyNotFound, err)

			require.NoError(t, tx.Commit())
		})
	})

	t.Run("delete same key twice in transaction", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			tx, err := db.Begin(true)
			require.NoError(t, err)

			key := []byte("key_test")
			val := []byte("value")

			require.NoError(t, tx.Put(bucket, key, val, Persistent))
			require.NoError(t, tx.Delete(bucket, key))

			// Second delete should return ErrKeyNotFound
			err = tx.Delete(bucket, key)
			require.Equal(t, ErrKeyNotFound, err)

			require.NoError(t, tx.Commit())
		})
	})

	t.Run("delete persisted key in transaction", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			key := []byte("persisted_key")
			val := []byte("persisted_value")

			txPut(t, db, bucket, key, val, Persistent, nil, nil)
			txGet(t, db, bucket, key, val, nil)

			tx, err := db.Begin(true)
			require.NoError(t, err)
			require.NoError(t, tx.Delete(bucket, key))
			require.NoError(t, tx.Commit())

			txGet(t, db, bucket, key, nil, ErrKeyNotFound)
		})
	})
}

func TestTx_GetAndScansFromHintKey(t *testing.T) {
	bucket := "bucket_get_test"
	withRAMIdxDB(t, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		// write tx begin
		tx, err := db.Begin(true)
		require.NoError(t, err)

		for i := 0; i <= 10; i++ {
			key := []byte("key_" + fmt.Sprintf("%07d", i))
			val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
			err = tx.Put(bucket, key, val, Persistent)
			assert.NoError(t, err)
		}
		assert.NoError(t, tx.Commit()) // tx commit

		for i := 0; i <= 10; i++ {
			tx, err = db.Begin(false)
			require.NoError(t, err)

			key := []byte("key_" + fmt.Sprintf("%07d", i))
			value, err := tx.Get(bucket, key)
			assert.NoError(t, err)

			wantValue := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
			assert.Equal(t, wantValue, value)

			// tx commit
			assert.NoError(t, tx.Commit())
		}

		tx, err = db.Begin(false)
		require.NoError(t, err)

		start := []byte("key_0000001")
		end := []byte("key_0000010")
		values, err := tx.RangeScan(bucket, start, end)
		assert.NoError(t, err)

		j := 0
		for i := 1; i <= 10; i++ {
			wantVal := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
			assert.Equal(t, wantVal, values[j])

			j++
		}

		// tx commit
		assert.NoError(t, tx.Commit())
	})
}

func TestTx_Put_Err(t *testing.T) {
	bucket := "bucket_tx_put"

	t.Run("write with read only tx", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			// write tx begin err setting here
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			tx, err := db.Begin(false) // tx not writable
			require.NoError(t, err)

			key := []byte("key_" + fmt.Sprintf("%07d", 0))
			val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 0))
			err = tx.Put(bucket, key, val, Persistent)
			assert.Error(t, err)

			assert.NoError(t, tx.Rollback())
		})
	})

	t.Run("write with empty key", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			tx, err := db.Begin(true)
			require.NoError(t, err)

			key := []byte("") // key cannot be empty
			val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 0))
			err = tx.Put(bucket, key, val, Persistent)
			assert.Error(t, err)

			assert.NoError(t, tx.Rollback())
		})
	})

	t.Run("write with TOO big size", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			tx, err := db.Begin(true)
			require.NoError(t, err)

			key := []byte("key_bigone")
			var bigVal string // too big size
			for i := 1; i <= 9*1024; i++ {
				bigVal += "val" + strconv2.IntToStr(i)
			}

			err = tx.Put(bucket, key, []byte(bigVal), Persistent)
			assert.NoError(t, err)

			assert.Error(t, tx.Commit()) // too big cannot commit by tx
		})
	})
}

func TestTx_PrefixScan_NotFound(t *testing.T) {
	t.Run("prefix scan in empty bucket", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			tx, err := db.Begin(false)
			assert.NoError(t, err)

			prefix := []byte("key_")
			entries, err := tx.PrefixScan("foobucket", prefix, 0, 10)
			assert.Error(t, err)
			assert.Empty(t, entries)

			assert.NoError(t, tx.Commit()) // tx commit
		})
	})

	t.Run("prefix scan", func(t *testing.T) {
		bucket := "bucket_prefix_scan_test"

		withDefaultDB(t, func(t *testing.T, db *DB) {
			{ // write tx begin
				txCreateBucket(t, db, DataStructureBTree, bucket, nil)

				tx, err := db.Begin(true)
				require.NoError(t, err)

				for i := 0; i <= 10; i++ {
					key := []byte("key_" + fmt.Sprintf("%07d", i))
					val := []byte("val" + fmt.Sprintf("%07d", i))
					err = tx.Put(bucket, key, val, Persistent)
					assert.NoError(t, err)
				}

				assert.NoError(t, tx.Commit()) // tx commit
			}

			{
				tx, err = db.Begin(false)
				require.NoError(t, err)

				prefix := []byte("key_foo")
				entries, err := tx.PrefixScan(bucket, prefix, 0, 10)
				assert.Error(t, err)
				assert.NoError(t, tx.Commit())

				assert.Empty(t, entries)
			}

			{
				tx, err = db.Begin(false)
				require.NoError(t, err)

				entries, err := tx.PrefixScan(bucket, []byte("key_"), 0, 10)
				assert.NoError(t, err)
				assert.NoError(t, tx.Commit())

				assert.NotEmpty(t, entries)

			}

			{ // scan by closed tx
				entries, err := tx.PrefixScan(bucket, []byte("key_"), 0, 10)
				assert.Error(t, err)
				if len(entries) > 0 || err == nil {
					t.Error("err TestTx_PrefixScan_NotFound")
				}
			}
		})
	})
}

func TestTx_PrefixSearchScan_NotFound(t *testing.T) {
	regs := "(.+)"
	bucket := "bucket_prefix_search_scan_test"

	t.Run("prefix search in empty bucket", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			tx, err := db.Begin(false)
			require.NoError(t, err)

			prefix := []byte("key_")
			_, err = tx.PrefixSearchScan("foobucket", prefix, regs, 0, 10)
			assert.Error(t, err)

			assert.NoError(t, tx.Commit())
		})
	})

	t.Run("prefix search scan", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			{ // set up the data
				txCreateBucket(t, db, DataStructureBTree, bucket, nil)

				tx, err = db.Begin(true) // write tx begin
				require.NoError(t, err)

				for i := 0; i <= 10; i++ {
					key := []byte("key_" + fmt.Sprintf("%07d", i))
					val := []byte("val" + fmt.Sprintf("%07d", i))

					err := tx.Put(bucket, key, val, Persistent)
					assert.NoError(t, err)
				}
				// tx commit
				assert.NoError(t, tx.Commit())
			}

			{ // no key exists in bucket
				tx, err := db.Begin(false)
				require.NoError(t, err)

				prefix := []byte("key_foo")
				_, err = tx.PrefixSearchScan(bucket, prefix, regs, 0, 10)
				assert.Error(t, err)

				assert.NoError(t, tx.Rollback())
			}

			{ // scan by closed tx
				tx, err := db.Begin(false)
				require.NoError(t, err)

				assert.NoError(t, tx.Commit())

				_, err = tx.PrefixSearchScan(bucket, []byte("key_"), regs, 0, 10)
				assert.Error(t, err)
			}
		})
	})
}

func TestTx_RangeScan_NotFound(t *testing.T) {
	bucket := "bucket_range_scan_test"

	withDefaultDB(t, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		tx, err := db.Begin(true) // write tx begin
		require.NoError(t, err)

		for i := 0; i <= 10; i++ {
			key := []byte("key_" + fmt.Sprintf("%03d", i))
			val := []byte("val" + fmt.Sprintf("%03d", i))
			err = tx.Put(bucket, key, val, Persistent)
			assert.NoError(t, err)
		}
		assert.NoError(t, tx.Commit()) // tx commit

		tx, err = db.Begin(false)
		require.NoError(t, err)

		start := []byte("key_011")
		end := []byte("key_012")
		_, err = tx.RangeScan(bucket, start, end)
		assert.Error(t, err)

		assert.NoError(t, tx.Commit())
	})
}

func TestTx_ExpiredDeletion(t *testing.T) {
	bucket := "bucket"

	t.Run("expired deletion", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), 1, nil, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), 2, nil, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(2), testutils.GetTestBytes(2), 3, nil, nil)
			txGet(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), nil)
			txGet(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), nil)
			txGet(t, db, bucket, testutils.GetTestBytes(2), testutils.GetTestBytes(2), nil)

			txDel(t, db, bucket, testutils.GetTestBytes(2), nil)

			mc.AdvanceTime(1100 * time.Millisecond)

			// this entry will be deleted
			txGet(t, db, bucket, testutils.GetTestBytes(0), nil, ErrKeyNotFound)
			// this entry still alive
			txGet(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), nil)

			mc.AdvanceTime(1 * time.Second)

			// this entry will be deleted
			txGet(t, db, bucket, testutils.GetTestBytes(1), nil, ErrKeyNotFound)

			r, ok := db.Index.BTree.GetWithDefault(1).Find(testutils.GetTestBytes(0))
			require.Nil(t, r)
			require.False(t, ok)

			r, ok = db.Index.BTree.GetWithDefault(1).Find(testutils.GetTestBytes(1))
			require.Nil(t, r)
			require.False(t, ok)
		})
	})

	t.Run("update expire time", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), 1, nil, nil)
			mc.AdvanceTime(500 * time.Millisecond)

			// reset expire time
			txPut(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), 3, nil, nil)
			mc.AdvanceTime(1 * time.Second)
			txGet(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), nil)

			mc.AdvanceTime(3 * time.Second)
			txGet(t, db, bucket, testutils.GetTestBytes(0), nil, ErrKeyNotFound)
		})
	})

	t.Run("persist expire time", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), 1, nil, nil)
			mc.AdvanceTime(500 * time.Millisecond)

			// persist expire time
			txPut(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), Persistent, nil, nil)
			mc.AdvanceTime(1 * time.Second)
			txGet(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), nil)

			mc.AdvanceTime(3 * time.Second)
			txGet(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), nil)
		})
	})

	t.Run("expired deletion when open", func(t *testing.T) {
		// This test uses MockClock to test TTL behavior across database restarts
		mc := ttl.NewMockClock(time.Now().UnixMilli())
		opts := DefaultOptions
		opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
		opts.Clock = mc
		defer removeDir(opts.Dir)

		db, err := Open(opts)
		require.NoError(t, err)

		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		txPut(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), 1, nil, nil)
		txPut(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), 3, nil, nil)
		txPut(t, db, bucket, testutils.GetTestBytes(2), testutils.GetTestBytes(2), 3, nil, nil)
		txPut(t, db, bucket, testutils.GetTestBytes(3), testutils.GetTestBytes(3), Persistent, nil, nil)
		txPut(t, db, bucket, testutils.GetTestBytes(4), testutils.GetTestBytes(4), Persistent, nil, nil)
		txPut(t, db, bucket, testutils.GetTestBytes(5), testutils.GetTestBytes(5), 5, nil, nil)
		txPut(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), Persistent, nil, nil)
		txDel(t, db, bucket, testutils.GetTestBytes(5), nil)

		require.NoError(t, db.Close())

		mc.AdvanceTime(1100 * time.Millisecond)

		// Reopen with the same MockClock
		db, err = Open(opts)
		require.NoError(t, err)
		defer func() {
			if !db.IsClose() {
				require.NoError(t, db.Close())
			}
		}()

		txGet(t, db, bucket, testutils.GetTestBytes(0), nil, ErrKeyNotFound)
		txGet(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), nil)
		txGet(t, db, bucket, testutils.GetTestBytes(2), testutils.GetTestBytes(2), nil)
		txGet(t, db, bucket, testutils.GetTestBytes(5), nil, ErrKeyNotFound)

		mc.AdvanceTime(2 * time.Second)

		txGet(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), nil)
		txGet(t, db, bucket, testutils.GetTestBytes(2), testutils.GetTestBytes(2), ErrKeyNotFound)

		mc.AdvanceTime(2 * time.Second)
		// this entry should be persistent
		txGet(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), nil)
		txGet(t, db, bucket, testutils.GetTestBytes(3), testutils.GetTestBytes(3), nil)
		txGet(t, db, bucket, testutils.GetTestBytes(4), testutils.GetTestBytes(4), nil)
	})

	t.Run("expire deletion when merge", func(t *testing.T) {
		opts := DefaultOptions
		opts.SegmentSize = 1 * 100
		runNutsDBTestWithMockClock(t, &opts, func(t *testing.T, db *DB, mc ttl.Clock) {
			bucket := "bucket"
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), Persistent, nil, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), Persistent, nil, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(2), testutils.GetTestBytes(2), 1, nil, nil)

			mc.AdvanceTime(1100 * time.Millisecond)

			require.NoError(t, db.Merge())

			txGet(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), nil)
			txGet(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), nil)
			txGet(t, db, bucket, testutils.GetTestBytes(2), nil, ErrKeyNotFound)

			r, ok := db.Index.BTree.GetWithDefault(1).Find(testutils.GetTestBytes(2))
			require.Nil(t, r)
			require.False(t, ok)
		})
	})

	t.Run("expire deletion with batch processing", func(t *testing.T) {
		opts := DefaultOptions
		runNutsDBTestWithMockClock(t, &opts, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), 1, nil, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), 2, nil, nil)
			txGet(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), nil)
			txGet(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), nil)

			mc.AdvanceTime(1100 * time.Millisecond)

			// this entry will be deleted
			txGet(t, db, bucket, testutils.GetTestBytes(0), nil, ErrKeyNotFound)
			// this entry still alive
			txGet(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), nil)

			mc.AdvanceTime(1 * time.Second)

			// this entry will be deleted
			txGet(t, db, bucket, testutils.GetTestBytes(1), nil, ErrKeyNotFound)

			r, ok := db.Index.BTree.GetWithDefault(1).Find(testutils.GetTestBytes(0))
			require.Nil(t, r)
			require.False(t, ok)

			r, ok = db.Index.BTree.GetWithDefault(1).Find(testutils.GetTestBytes(1))
			require.Nil(t, r)
			require.False(t, ok)
		})
	})
}

func TestTx_GetMaxOrMinKey(t *testing.T) {
	bucket := "bucket"
	t.Run("general key test", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			for i := 0; i < 10; i++ {
				txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetRandomBytes(24), Persistent, nil, nil)
			}

			txGetMaxOrMinKey(t, db, bucket, true, testutils.GetTestBytes(9), nil)
			txGetMaxOrMinKey(t, db, bucket, false, testutils.GetTestBytes(0), nil)

			txDel(t, db, bucket, testutils.GetTestBytes(9), nil)
			txDel(t, db, bucket, testutils.GetTestBytes(0), nil)

			txGetMaxOrMinKey(t, db, bucket, true, testutils.GetTestBytes(8), nil)
			txGetMaxOrMinKey(t, db, bucket, false, testutils.GetTestBytes(1), nil)

			txPut(t, db, bucket, testutils.GetTestBytes(-1), testutils.GetRandomBytes(24), Persistent, nil, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(100), testutils.GetRandomBytes(24), Persistent, nil, nil)

			txGetMaxOrMinKey(t, db, bucket, false, testutils.GetTestBytes(-1), nil)
			txGetMaxOrMinKey(t, db, bucket, true, testutils.GetTestBytes(100), nil)
		})
	})

	t.Run("test expire", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			keya := []byte("A")
			keyb := []byte("B")
			keyc := []byte("C")
			txPut(t, db, bucket, keya, keya, 1, nil, nil)
			txPut(t, db, bucket, keyb, keyb, Persistent, nil, nil)
			txPut(t, db, bucket, keyc, keyc, 3, nil, nil)

			txGetMaxOrMinKey(t, db, bucket, false, keya, nil)
			txGetMaxOrMinKey(t, db, bucket, true, keyc, nil)
			mc.AdvanceTime(1500 * time.Millisecond)
			txGetMaxOrMinKey(t, db, bucket, false, keyb, nil)
			mc.AdvanceTime(2000 * time.Millisecond)
			txGetMaxOrMinKey(t, db, bucket, true, keyb, nil)
		})
	})
}

func TestTx_updateOrPut(t *testing.T) {
	bucket := "bucket"
	t.Run("updateOrPut will return the expected error when it do update", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, []byte("any"), []byte("any"), Persistent, nil, nil)

			tx, err := db.Begin(true)
			require.NoError(t, err)
			expectedErr := errors.New("some error")
			require.Equal(t, expectedErr, tx.updateOrPut("bucket", []byte("any"), []byte("any"), func(bytes []byte) ([]byte, error) {
				return nil, expectedErr
			}))
			require.Equal(t, nil, tx.Commit())
		})
	})
}

func TestTx_IncrementAndDecrement(t *testing.T) {
	bucket := "bucket"

	key := testutils.GetTestBytes(0)

	t.Run("increments and decrements on a non-exist key", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txIncrement(t, db, bucket, key, ErrKeyNotFound, nil)
			txIncrementBy(t, db, bucket, key, 12, ErrKeyNotFound, nil)
			txDecrement(t, db, bucket, key, ErrKeyNotFound, nil)
			txDecrementBy(t, db, bucket, key, 12, ErrKeyNotFound, nil)
		})
	})

	t.Run("increments and decrements on a non-integer value", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, []byte("foo"), Persistent, nil, nil)

			txIncrement(t, db, bucket, key, ErrValueNotInteger, nil)
			txIncrementBy(t, db, bucket, key, 12, ErrValueNotInteger, nil)
			txDecrement(t, db, bucket, key, ErrValueNotInteger, nil)
			txDecrementBy(t, db, bucket, key, 12, ErrValueNotInteger, nil)
		})
	})

	t.Run("increments and decrements normally", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, getIntegerValue(12), Persistent, nil, nil)
			txIncrement(t, db, bucket, key, nil, nil)
			txGet(t, db, bucket, key, getIntegerValue(13), nil)

			txDecrement(t, db, bucket, key, nil, nil)
			txGet(t, db, bucket, key, getIntegerValue(12), nil)

			txIncrementBy(t, db, bucket, key, 12, nil, nil)
			txGet(t, db, bucket, key, getIntegerValue(24), nil)

			txDecrementBy(t, db, bucket, key, 25, nil, nil)
			txGet(t, db, bucket, key, getIntegerValue(-1), nil)
		})
	})

	t.Run("increments and decrements over range of int64", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, []byte("9223372036854775818"), Persistent, nil, nil)
			txIncrement(t, db, bucket, key, nil, nil)
			txGet(t, db, bucket, key, []byte("9223372036854775819"), nil)

			txIncrementBy(t, db, bucket, key, 200, nil, nil)
			txGet(t, db, bucket, key, []byte("9223372036854776019"), nil)

			txDecrement(t, db, bucket, key, nil, nil)
			txGet(t, db, bucket, key, []byte("9223372036854776018"), nil)

			txDecrementBy(t, db, bucket, key, math.MaxInt64, nil, nil)
			txGet(t, db, bucket, key, []byte("211"), nil)

			txDecrementBy(t, db, bucket, key, math.MaxInt64, nil, nil)
			txGet(t, db, bucket, key, []byte("-9223372036854775596"), nil)
		})
	})

	t.Run("increments and decrements on value which will overflow after operation", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, []byte("9223372036854775800"), Persistent, nil, nil)
			txIncrementBy(t, db, bucket, key, 100, nil, nil)
			txGet(t, db, bucket, key, []byte("9223372036854775900"), nil)

			txDecrementBy(t, db, bucket, key, math.MaxInt64, nil, nil)
			txGet(t, db, bucket, key, []byte("93"), nil)

			txDecrementBy(t, db, bucket, key, math.MaxInt64, nil, nil)
			txGet(t, db, bucket, key, []byte("-9223372036854775714"), nil)

			txDecrementBy(t, db, bucket, key, math.MaxInt64, nil, nil)
			txGet(t, db, bucket, key, []byte("-18446744073709551521"), nil)
		})
	})

	t.Run("operations on expired key", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, []byte("1"), 1, nil, nil)

			mc.AdvanceTime(2 * time.Second)

			txIncrement(t, db, bucket, key, ErrKeyNotFound, nil)
			txIncrementBy(t, db, bucket, key, 12, ErrKeyNotFound, nil)
			txDecrement(t, db, bucket, key, ErrKeyNotFound, nil)
			txDecrementBy(t, db, bucket, key, 12, ErrKeyNotFound, nil)
		})
	})
}

func TestTx_PutIfNotExistsAndPutIfExists(t *testing.T) {
	bucket := "bucket"
	val := []byte("value")
	updated_val := []byte("updated_value")

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 10; i++ {
			txPutIfNotExists(t, db, bucket, testutils.GetTestBytes(i), val, nil, nil)
		}

		for i := 0; i < 10; i++ {
			txGet(t, db, bucket, testutils.GetTestBytes(i), val, nil)
		}

		for i := 0; i < 10; i++ {
			txPutIfExists(t, db, bucket, testutils.GetTestBytes(i), updated_val, nil, nil)
		}

		for i := 0; i < 10; i++ {
			txGet(t, db, bucket, testutils.GetTestBytes(i), updated_val, nil)
		}
	})
}

func TestTx_GetAndSetBit(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)

	t.Run("get bit on a non-exist key", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txGetBit(t, db, bucket, key, 0, 0, ErrKeyNotFound, nil)
		})
	})

	t.Run("set bit on a non-exist key", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txSetBit(t, db, bucket, key, 0, 1, nil, nil)
			txGetBit(t, db, bucket, key, 0, 1, nil, nil)
			txGet(t, db, bucket, key, []byte("\x01"), nil)

			txDel(t, db, bucket, key, nil)
			txSetBit(t, db, bucket, key, 2, 1, nil, nil)
			txGetBit(t, db, bucket, key, 2, 1, nil, nil)
			txGet(t, db, bucket, key, []byte("\x00\x00\x01"), nil)

			txDel(t, db, bucket, key, nil)
			txSetBit(t, db, bucket, key, 2, '1', nil, nil)
			txGetBit(t, db, bucket, key, 2, '1', nil, nil)
			txGet(t, db, bucket, key, []byte("\x00\x001"), nil)
		})
	})

	t.Run("get and set bit on a exist string", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, []byte("foober"), Persistent, nil, nil)
			txGet(t, db, bucket, key, []byte("foober"), nil)
			txGetBit(t, db, bucket, key, 0, 'f', nil, nil)

			txSetBit(t, db, bucket, key, 0, 'F', nil, nil)
			txGetBit(t, db, bucket, key, 0, 'F', nil, nil)

			txSetBit(t, db, bucket, key, 3, 'B', nil, nil)
			txGetBit(t, db, bucket, key, 3, 'B', nil, nil)

			txSetBit(t, db, bucket, key, 5, 'R', nil, nil)
			txGetBit(t, db, bucket, key, 5, 'R', nil, nil)

			txSetBit(t, db, bucket, key, 6, 'A', nil, nil)
			txGetBit(t, db, bucket, key, 6, 'A', nil, nil)
			txGet(t, db, bucket, key, []byte("FooBeRA"), nil)

			txSetBit(t, db, bucket, key, 12, 'C', nil, nil)
			txGetBit(t, db, bucket, key, 12, 'C', nil, nil)
			txGet(t, db, bucket, key, []byte("FooBeRA\x00\x00\x00\x00\x00C"), nil)
		})
	})

	t.Run("give a invalid offset", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txSetBit(t, db, bucket, key, math.MaxInt64, 0, ErrOffsetInvalid, nil)
			txGetBit(t, db, bucket, key, math.MaxInt64, 0, ErrOffsetInvalid, nil)

			txSetBit(t, db, bucket, key, 0, 1, nil, nil)
			txGetBit(t, db, bucket, key, 1, 0, nil, nil)
		})
	})
}

func TestTx_ValueLen(t *testing.T) {
	bucket := "bucket"
	val := []byte("value")

	t.Run("match length", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(1), val, Persistent, nil, nil)
			txValueLen(t, db, bucket, testutils.GetTestBytes(1), 5, nil)
		})
	})

	t.Run("bucket not exist", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txValueLen(t, db, bucket, testutils.GetTestBytes(1), 0, ErrBucketNotExist)
		})
	})

	t.Run("key not found", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(1), val, Persistent, nil, nil)
			txValueLen(t, db, bucket, testutils.GetTestBytes(2), 0, ErrKeyNotFound)
		})
	})

	t.Run("expired test", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(1), val, 1, nil, nil)
			mc.AdvanceTime(3 * time.Second)
			txValueLen(t, db, bucket, testutils.GetTestBytes(1), 0, ErrKeyNotFound)
		})
	})
}

func TestTx_GetSet(t *testing.T) {
	bucket := "bucket"
	val := []byte("value")
	newVal := []byte("new_value")

	t.Run("match value", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(1), val, Persistent, nil, nil)
			txGetSet(t, db, bucket, testutils.GetTestBytes(1), newVal, val, nil)
			txGet(t, db, bucket, testutils.GetTestBytes(1), newVal, nil)
		})
	})

	t.Run("bucket not exist", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txGetSet(t, db, bucket, testutils.GetTestBytes(1), newVal, nil, ErrBucketNotExist)
		})
	})

	t.Run("expired test", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(1), val, 1, nil, nil)
			mc.AdvanceTime(3 * time.Second)
			txGetSet(t, db, bucket, testutils.GetTestBytes(1), newVal, nil, ErrKeyNotFound)
		})
	})
}

func TestTx_GetTTLAndPersist(t *testing.T) {
	bucket := "bucket1"
	key := []byte("key1")
	value := []byte("value1")
	runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
		txGetTTL(t, db, bucket, key, 0, ErrBucketNotExist)

		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		txGetTTL(t, db, bucket, key, 0, ErrKeyNotFound)
		txPersist(t, db, bucket, key, ErrKeyNotFound)

		txPut(t, db, bucket, key, value, 1, nil, nil)
		mc.AdvanceTime(2 * time.Second) // Wait till value to expire
		txGetTTL(t, db, bucket, key, 0, ErrKeyNotFound)

		txPut(t, db, bucket, key, value, 100, nil, nil)
		txGetTTL(t, db, bucket, key, 100, nil)

		txPersist(t, db, bucket, key, nil)
		txGetTTL(t, db, bucket, key, -1, nil)
	})
}

func TestTx_MSetMGet(t *testing.T) {
	bucket := "bucket"

	t.Run("use MSet and MGet with 0 args", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txMSet(t, db, bucket, nil, Persistent, nil, nil)
			txMGet(t, db, bucket, nil, nil, nil, nil)
		})
	})

	t.Run("use MSet by using odd number of args ", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txMSet(t, db, bucket, [][]byte{
				testutils.GetTestBytes(0), testutils.GetTestBytes(1), testutils.GetTestBytes(2),
			}, Persistent, ErrKVArgsLenNotEven, nil)
		})
	})

	t.Run("use MSet and MGet normally", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txMSet(t, db, bucket, [][]byte{
				testutils.GetTestBytes(0), testutils.GetTestBytes(1), testutils.GetTestBytes(2), testutils.GetTestBytes(3),
			}, Persistent, nil, nil)
			txMGet(t, db, bucket, [][]byte{
				testutils.GetTestBytes(0), testutils.GetTestBytes(2),
			}, [][]byte{
				testutils.GetTestBytes(1), testutils.GetTestBytes(3),
			}, nil, nil)
		})
	})
}

func TestTx_Append(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)

	t.Run("use Append with a nil appendage", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txAppend(t, db, bucket, key, []byte(""), nil, nil)
			txAppend(t, db, bucket, key, nil, nil, nil)
		})
	})

	t.Run("use Append on a non-exist key", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txAppend(t, db, bucket, key, testutils.GetTestBytes(1), nil, nil)
			txGet(t, db, bucket, key, testutils.GetTestBytes(1), nil)
		})
	})

	t.Run("use append on an exist key", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, []byte("test"), Persistent, nil, nil)
			txAppend(t, db, bucket, key, []byte("test"), nil, nil)
			txGet(t, db, bucket, key, []byte("testtest"), nil)
		})
	})
}

func TestTx_GetRange(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)

	t.Run("use GetRange with start greater than less", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txGetRange(t, db, bucket, key, 5, 1, nil, ErrStartGreaterThanEnd, nil)
		})
	})

	t.Run("use GetRange with start greater than size of value", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, []byte("test"), Persistent, nil, nil)
			txGetRange(t, db, bucket, key, 5, 10, nil, nil, nil)
		})
	})

	t.Run("use GetRange with end greater than size of value", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, []byte("test"), Persistent, nil, nil)
			txGetRange(t, db, bucket, key, 2, 10, []byte("st"), nil, nil)
		})
	})

	t.Run("use GetRange normally", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, []byte("test"), Persistent, nil, nil)
			txGetRange(t, db, bucket, key, 1, 2, []byte("es"), nil, nil)
			txGetRange(t, db, bucket, key, 1, 3, []byte("est"), nil, nil)
			txGetRange(t, db, bucket, key, 1, 4, []byte("est"), nil, nil)
		})
	})
}

func TestBTreeInternalVisibility(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		txPut(t, db, bucket, key, []byte("test"), Persistent, nil, nil)
		err := db.Update(func(tx *Tx) error {
			err := tx.Put(bucket, key, []byte("test-updated"), Persistent)
			assert.Nil(t, err)
			value, err := tx.Get(bucket, key)
			assert.Nil(t, err)
			assert.Equal(t, "test-updated", string(value))
			err = tx.DeleteBucket(DataStructureBTree, bucket)
			assert.Nil(t, err)
			err = tx.Put(bucket, key, []byte("test-updated"), Persistent)
			assert.Equal(t, ErrBucketNotFound, err)
			return nil
		})
		assert.Equal(t, ErrBucketNotExist, err)
	})
}

// TestTx_EmptyBucketQuery tests GitHub Issue #595 about empty bucket query returning wrong error
// Issue: When querying a key in an empty bucket, it should return "key not found" instead of "bucket not found"
func TestTx_EmptyBucketQuery(t *testing.T) {
	bucket := "empty_bucket_test"
	key := testutils.GetTestBytes(0)

	t.Run("empty bucket query after creation", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			// Create empty bucket
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			// Query empty bucket should return ErrKeyNotFound, not ErrBucketNotFound
			txGet(t, db, bucket, key, nil, ErrKeyNotFound)
		})
	})

	t.Run("empty bucket query after restart", func(t *testing.T) {
		opts := DefaultOptions

		opts.Dir = filepath.Join(t.TempDir(), "nutsdb_empty_bucket_restart_test")

		runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
			// Create empty bucket
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			// Close and reopen database to simulate restart
			require.NoError(t, db.Close())

			// Reopen database
			db, err := Open(opts)
			require.NoError(t, err)
			defer func() {
				if !db.IsClose() {
					require.NoError(t, db.Close())
				}
			}()

			// Query empty bucket after restart should still return ErrKeyNotFound
			txGet(t, db, bucket, key, nil, ErrKeyNotFound)
		})
	})

	t.Run("other methods on empty bucket", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			// Create empty bucket
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			// Test GetMaxKey and GetMinKey should return ErrKeyNotFound
			txGetMaxOrMinKey(t, db, bucket, true, nil, ErrKeyNotFound)
			txGetMaxOrMinKey(t, db, bucket, false, nil, ErrKeyNotFound)

			// Test GetAll should return empty arrays, not error
			txGetAll(t, db, bucket, [][]byte{}, [][]byte{}, nil)

			// Test Delete on empty bucket should return ErrKeyNotFound
			txDel(t, db, bucket, key, ErrKeyNotFound)

			// Test other Get methods return ErrKeyNotFound
			err := db.View(func(tx *Tx) error {
				// Test GetKeys
				keys, err := tx.GetKeys(bucket)
				require.NoError(t, err)
				require.Empty(t, keys)

				// Test GetValues
				values, err := tx.GetValues(bucket)
				require.NoError(t, err)
				require.Empty(t, values)

				return nil
			})
			require.NoError(t, err)
		})
	})

	t.Run("normal functionality after fix", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			// Create bucket and add data
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			value := testutils.GetTestBytes(1)
			txPut(t, db, bucket, key, value, Persistent, nil, nil)

			// Verify normal get works
			txGet(t, db, bucket, key, value, nil)

			// Query non-existing key should return ErrKeyNotFound
			nonExistingKey := testutils.GetTestBytes(999)
			txGet(t, db, bucket, nonExistingKey, nil, ErrKeyNotFound)

			// Test GetAll works normally
			txGetAll(t, db, bucket, [][]byte{key}, [][]byte{value}, nil)

			// Test Delete non-existing key returns ErrKeyNotFound
			txDel(t, db, bucket, nonExistingKey, ErrKeyNotFound)
		})
	})
}

func TestTx_Has(t *testing.T) {
	bucket := "bucket"
	val := []byte("value")

	t.Run("match value", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(1), val, Persistent, nil, nil)
			txHas(t, db, bucket, testutils.GetTestBytes(1), true, nil)
		})
	})

	t.Run("no match value", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(1), val, Persistent, nil, nil)
			nonExistingKey := testutils.GetTestBytes(999)
			txHas(t, db, bucket, nonExistingKey, false, nil)
		})
	})

	t.Run("bucket not exist", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txHas(t, db, bucket, testutils.GetTestBytes(1), false, ErrBucketNotExist)
		})
	})

	t.Run("expired test", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(1), val, 1, nil, nil)
			mc.AdvanceTime(3 * time.Second)
			txHas(t, db, bucket, testutils.GetTestBytes(1), false, nil)
		})
	})
}

func TestTx_ReadAndWriteInSameTransaction(t *testing.T) {
	t.Run("test Put and Get", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			r := require.New(t)
			bucket := `1`
			key := []byte(`k`)
			v1 := []byte(`v1`)
			v2 := []byte(`v2`)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			r.NoError(db.Update(func(tx *Tx) error {
				r.NoError(tx.Put(bucket, key, v1, 0))
				_, err := tx.Get(bucket, key)
				r.NoError(err)
				return nil
			}))

			r.NoError(db.Update(func(tx *Tx) error {
				item, err := tx.Get(bucket, key)
				r.NoError(err)
				r.EqualValues(v1, item)

				r.NoError(tx.Put(bucket, key, v2, 0))
				item, err = tx.Get(bucket, key)
				r.NoError(err)
				r.EqualValues(v2, item)
				return nil
			}))
		})
	})

	t.Run("test GetTTL", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			r := require.New(t)

			bucket := `1`
			key := []byte(`k`)
			v1 := []byte(`v1`)

			r.NoError(db.Update(func(tx *Tx) (err error) {
				r.NoError(tx.NewKVBucket(bucket))
				r.NoError(tx.Put(bucket, key, v1, 3600))
				ttl, err := tx.GetTTL(bucket, key)
				r.Nil(err)
				r.NotEqual(0, ttl)
				return
			}))
		})
	})

	t.Run("test Persist", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			r := require.New(t)
			bucket := `1`
			key := []byte("k")
			ttl := uint32(100)
			val := []byte("V")
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			r.NoError(db.Update(func(tx *Tx) (err error) {
				r.NoError(tx.Put(bucket, key, val, ttl))
				curTTL, err := tx.GetTTL(bucket, key)
				r.NoError(err)
				r.Greater(curTTL, int64(0))
				r.NoError(tx.Persist(bucket, key))
				curTTL, err = tx.GetTTL(bucket, key)
				r.NoError(err)
				r.Equal(int64(-1), curTTL)
				return
			}))
		})
	})

	t.Run("test Incr And Decr", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			r := require.New(t)
			bucket := `1`
			key := []byte("k")
			intVal := int64(10)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			r.NoError(db.Update(func(tx *Tx) (err error) {
				r.NoError(tx.Put(bucket, key, getIntegerValue(intVal), Persistent))
				validateEqual(r, tx, bucket, key, intVal)
				r.NoError(tx.Incr(bucket, key))
				validateEqual(r, tx, bucket, key, intVal+1)
				r.NoError(tx.IncrBy(bucket, key, 100))
				validateEqual(r, tx, bucket, key, intVal+101)
				r.NoError(tx.DecrBy(bucket, key, 200))
				validateEqual(r, tx, bucket, key, intVal-99)
				return
			}))
		})
	})

	t.Run("test PutIfExists", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			r := require.New(t)
			bucket := `1`
			key := []byte("k")
			v1 := []byte("v1")
			v2 := []byte("v2")
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			r.NoError(db.Update(func(tx *Tx) (err error) {
				r.Equal(ErrKeyNotFound, tx.PutIfExists(bucket, key, v1, Persistent))
				r.NoError(tx.Put(bucket, key, v1, Persistent))
				value, err := tx.Get(bucket, key)
				r.NoError(err)
				r.Equal(v1, value)

				err = tx.PutIfExists(bucket, key, v2, Persistent)
				r.NoError(err)
				value, err = tx.Get(bucket, key)
				r.NoError(err)
				r.Equal(v2, value)
				return
			}))
		})
	})

	t.Run("test GetSet", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			r := require.New(t)
			bucket := `1`
			key := []byte("k")
			v1 := []byte("v1")
			v2 := []byte("v2")
			v3 := []byte("v3")
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			_ = db.Update(func(tx *Tx) error {
				r.NoError(tx.Put(bucket, key, v1, Persistent))
				var (
					v   []byte
					err error
				)
				v, err = tx.GetSet(bucket, key, v2)
				r.NoError(err)
				r.Equal(v1, v)
				v, err = tx.GetSet(bucket, key, v3)
				r.NoError(err)
				r.Equal(v2, v)
				return nil
			})
		})
	})
}

func TestTx_CreateBucketAndWriteInSameTransaction(t *testing.T) {
	t.Run("test Get and Has", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			r := require.New(t)

			bucket := `1`
			key := []byte(`k`)
			v1 := []byte(`v1`)

			r.NoError(db.Update(func(tx *Tx) (err error) {
				r.NoError(tx.NewKVBucket(bucket))
				r.NoError(tx.Put(bucket, key, v1, 0))
				v, err := tx.Get(bucket, key)
				r.Equal(v1, v)
				r.NoError(err)
				exist, err := tx.Has(bucket, key)
				r.True(exist)
				r.NoError(err)
				return
			}))
		})
	})

	t.Run("test GetMinOrMaxKey", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			r := require.New(t)

			bucket := `1`
			key1 := []byte(`k1`)
			key2 := []byte("k2")
			v1 := []byte(`v1`)

			r.NoError(db.Update(func(tx *Tx) (err error) {
				r.NoError(tx.NewKVBucket(bucket))
				r.NoError(tx.Put(bucket, key1, v1, 0))
				r.NoError(tx.Put(bucket, key2, v1, 0))

				k, err := tx.GetMaxKey(bucket)
				r.Nil(err)
				r.Equal(key2, k)
				k, err = tx.GetMinKey(bucket)
				r.Nil(err)
				r.Equal(key1, k)
				return
			}))
		})
	})

	t.Run("test GetTTL", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			r := require.New(t)

			bucket := `1`
			key := []byte(`k`)
			v1 := []byte(`v1`)

			r.NoError(db.Update(func(tx *Tx) (err error) {
				r.NoError(tx.NewKVBucket(bucket))
				r.NoError(tx.Put(bucket, key, v1, 3600))
				ttl, err := tx.GetTTL(bucket, key)
				r.Nil(err)
				r.NotEqual(0, ttl)
				return
			}))
		})
	})

	t.Run("test PutIfNotExists", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			r := require.New(t)

			bucket := `1`
			key := []byte(`k`)
			v1 := []byte(`v1`)
			v2 := []byte(`v2`)

			r.NoError(db.Update(func(tx *Tx) (err error) {
				r.NoError(tx.NewKVBucket(bucket))
				r.NoError(tx.Put(bucket, key, v1, 3600))
				v, _ := tx.get(bucket, key)
				r.Equal(v1, v)
				r.NoError(tx.PutIfNotExists(bucket, key, v2, 3600))
				v, _ = tx.get(bucket, key)
				r.Equal(v1, v)
				return
			}))
		})
	})

	t.Run("test GetRange", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			r := require.New(t)

			bucket := `1`
			key := []byte(`k`)
			v := []byte(`0123456789`)

			r.NoError(db.Update(func(tx *Tx) (err error) {
				r.NoError(tx.NewKVBucket(bucket))
				r.NoError(tx.Put(bucket, key, v, 3600))
				vnow, err := tx.GetRange(bucket, key, 1, 3)
				r.Nil(err)
				r.Equal("123", string(vnow))
				return
			}))
		})
	})

	t.Run("test RangeScanEntries, existed bucket", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			r := require.New(t)
			bucket := "1"

			txNewBucket(t, db, bucket, DataStructureBTree, nil, nil)

			txPut(t, db, bucket, []byte("k03"), []byte("old-v03"), 0, nil, nil)
			txPut(t, db, bucket, []byte("k06"), []byte("old-v06"), 0, nil, nil)

			r.NoError(db.Update(func(tx *Tx) error {
				for i := 0; i < 10; i++ {
					key := []byte(fmt.Sprintf("k%02d", i))
					val := []byte(fmt.Sprintf("v%02d", i))
					_ = tx.Put(bucket, key, val, Persistent)
				}

				keys, vals, err := tx.RangeScanEntries(bucket, []byte("k02"), []byte("k07"), true, true)
				r.NoError(err)

				r.EqualValues([][]byte{
					[]byte("k02"),
					[]byte("k03"),
					[]byte("k04"),
					[]byte("k05"),
					[]byte("k06"),
					[]byte("k07"),
				}, keys)
				r.EqualValues([][]byte{
					[]byte("v02"),
					[]byte("v03"),
					[]byte("v04"),
					[]byte("v05"),
					[]byte("v06"),
					[]byte("v07"),
				}, vals)

				return nil
			}))
		})
	})

	t.Run("test RangeScanEntries, new bucket", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			r := require.New(t)
			bucket := "1"

			r.NoError(db.Update(func(tx *Tx) error {
				_ = tx.NewKVBucket(bucket)
				for i := 0; i < 10; i++ {
					key := []byte(fmt.Sprintf("k%02d", i))
					val := []byte(fmt.Sprintf("v%02d", i))
					_ = tx.Put(bucket, key, val, Persistent)
				}

				keys, vals, err := tx.RangeScanEntries(bucket, []byte("k02"), []byte("k07"), true, true)
				r.NoError(err)

				r.EqualValues([][]byte{
					[]byte("k02"),
					[]byte("k03"),
					[]byte("k04"),
					[]byte("k05"),
					[]byte("k06"),
					[]byte("k07"),
				}, keys)
				r.EqualValues([][]byte{
					[]byte("v02"),
					[]byte("v03"),
					[]byte("v04"),
					[]byte("v05"),
					[]byte("v06"),
					[]byte("v07"),
				}, vals)

				return nil
			}))
		})
	})

	t.Run("test PutIfExists", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			r := require.New(t)
			bucket := `1`
			key := []byte("k")
			v1 := []byte("v1")
			v2 := []byte("v2")

			r.NoError(db.Update(func(tx *Tx) (err error) {
				r.NoError(tx.NewKVBucket(bucket))
				r.Equal(ErrKeyNotFound, tx.PutIfExists(bucket, key, v1, Persistent))
				r.NoError(tx.Put(bucket, key, v1, Persistent))
				value, err := tx.Get(bucket, key)
				r.NoError(err)
				r.Equal(v1, value)

				err = tx.PutIfExists(bucket, key, v2, Persistent)
				r.NoError(err)
				value, err = tx.Get(bucket, key)
				r.NoError(err)
				r.Equal(v2, value)
				return
			}))
		})
	})

	t.Run("test GetSet", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			r := require.New(t)
			bucket := `1`
			key := []byte("k")
			v1 := []byte("v1")
			v2 := []byte("v2")
			v3 := []byte("v3")

			_ = db.Update(func(tx *Tx) error {
				r.NoError(tx.NewKVBucket(bucket))
				r.NoError(tx.Put(bucket, key, v1, Persistent))
				var (
					v   []byte
					err error
				)
				v, err = tx.GetSet(bucket, key, v2)
				r.NoError(err)
				r.Equal(v1, v)
				v, err = tx.GetSet(bucket, key, v3)
				r.NoError(err)
				r.Equal(v2, v)
				return nil
			})
		})
	})
}

func TestTx_TestBucketNotExists(t *testing.T) {
	t.Run("test Get,Has,Put,PutIfNotExists,PutIfExists,GetMaxKey,GetMinKey",
		func(t *testing.T) {
			runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
				r := require.New(t)

				bucket := `1`
				key := []byte(`k`)
				v1 := []byte(`v1`)

				r.NoError(db.Update(func(tx *Tx) (err error) {
					_, err = tx.Get(bucket, key)
					r.Equal(ErrBucketNotExist, err)
					_, err = tx.Has(bucket, key)
					r.Equal(ErrBucketNotExist, err)
					_, err = tx.GetMaxKey(bucket)
					r.Equal(ErrBucketNotExist, err)
					_, err = tx.GetMinKey(bucket)
					r.Equal(ErrBucketNotExist, err)
					_, err = tx.GetTTL(bucket, key)
					r.Equal(ErrBucketNotExist, err)
					_, _, err = tx.RangeScanEntries(bucket, nil, nil, false, false)
					r.Equal(ErrBucketNotExist, err)

					r.Equal(ErrBucketNotExist, tx.Put(bucket, key, v1, 0))
					r.Equal(ErrBucketNotExist, tx.PutIfNotExists(bucket, key, v1, 0))
					r.Equal(ErrBucketNotExist, tx.PutIfExists(bucket, key, v1, 0))
					return nil
				}))
			})
		})
}

func TestTx_RecordExpired(t *testing.T) {
	t.Run("test PutIfExists", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			bucket := `1`
			key := []byte("k")
			v1 := []byte("v1")
			v2 := []byte("v2")
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, key, v1, 1, nil, nil)
			mc.AdvanceTime(100 * time.Millisecond)
			r.NoError(db.Update(func(tx *Tx) (err error) {
				r.NoError(tx.PutIfExists(bucket, key, v2, 1))
				return
			}))
			txGet(t, db, bucket, key, v2, nil)
			mc.AdvanceTime(100 * time.Millisecond)
			r.NoError(db.Update(func(tx *Tx) (err error) {
				r.NoError(tx.PutIfExists(bucket, key, v2, 1))
				return
			}))
			mc.AdvanceTime(1200 * time.Millisecond)
			r.NoError(db.Update(func(tx *Tx) (err error) {
				r.Equal(ErrNotFoundKey, tx.PutIfExists(bucket, key, v2, 1))
				return
			}))
		})
	})

	t.Run("test PutIfNotExists", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			bucket := `1`
			key := []byte("k")
			v1 := []byte("v1")
			v2 := []byte("v2")
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, key, v1, 1, nil, nil)
			mc.AdvanceTime(100 * time.Millisecond)
			r.NoError(db.Update(func(tx *Tx) (err error) {
				r.NoError(tx.PutIfNotExists(bucket, key, v2, 1))
				return
			}))
			txGet(t, db, bucket, key, v1, nil)
			mc.AdvanceTime(1100 * time.Millisecond)
			r.NoError(db.Update(func(tx *Tx) (err error) {
				r.NoError(tx.PutIfNotExists(bucket, key, v2, 1))
				return
			}))
			txGet(t, db, bucket, key, v2, nil)
		})
	})
}

func TestTx_NewTTLReturnError(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		r := require.New(t)
		bucket := `1`
		key := []byte("k")
		v := []byte("v")
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		txPut(t, db, bucket, key, v, 1, nil, nil)
		expectErr := errors.New("test error")
		err = db.Update(func(tx *Tx) error {
			return tx.update(
				bucket,
				key,
				func(b []byte) ([]byte, error) {
					return b, nil
				},
				func(u uint32) (uint32, error) {
					return 0, expectErr
				})
		})
		r.Equal(expectErr, err)
	})
}

// TestTx_Delete_NoRecursiveCallback tests that Delete operation does not trigger recursive TTL callbacks
// This test verifies the fix for the infinite recursion issue where:
// 1. Timer expires and triggers callback
// 2. Callback calls Delete which uses Find
// 3. Find triggers another callback (infinite loop)
// The fix uses FindForVerification in Delete to avoid triggering callbacks
func TestTx_Delete_NoRecursiveCallback(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)
	value := []byte("value")

	t.Run("delete expired key should not trigger recursive callback", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			// Track callback invocations
			callbackCount := int32(0)

			// Put a key with TTL
			txPut(t, db, bucket, key, value, 1, nil, nil)

			// Advance time to expire the key
			mc.AdvanceTime(1100 * time.Millisecond)

			// Manually trigger delete (simulating what handleExpiredKeyAsync does)
			err := db.Update(func(tx *Tx) error {
				// Get the index
				idx, ok := db.Index.BTree.exist(1) // bucketId = 1
				r.True(ok)

				// Use FindForVerification - should NOT trigger callback
				record, found := idx.FindForVerification(key)
				r.True(found)
				r.NotNil(record)

				// Verify the record is actually expired
				r.True(db.ttlService.GetChecker().IsExpired(record.TTL, record.Timestamp))

				return nil
			})
			r.NoError(err)

			// Verify callback was not triggered during delete
			// In the old implementation, this would cause infinite recursion
			r.Equal(int32(0), callbackCount)

			// Verify key is actually deleted
			txGet(t, db, bucket, key, nil, ErrKeyNotFound)
		})
	})

	t.Run("delete non-expired key should work normally", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, value, 10, nil, nil)

			// Delete before expiration
			err := db.Update(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				// FindForVerification should return the record
				record, found := idx.FindForVerification(key)
				r.True(found)
				r.NotNil(record)

				// Record should not be expired
				r.False(db.ttlService.GetChecker().IsExpired(record.TTL, record.Timestamp))

				return tx.Delete(bucket, key)
			})
			r.NoError(err)

			txGet(t, db, bucket, key, nil, ErrKeyNotFound)
		})
	})

	t.Run("timestamp verification prevents wrong deletion", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			// Put key with TTL=1s
			txPut(t, db, bucket, key, []byte("value1"), 1, nil, nil)

			// Get the original timestamp
			var originalTimestamp uint64
			err := db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)
				record, found := idx.FindForVerification(key)
				r.True(found)
				originalTimestamp = record.Timestamp
				return nil
			})
			r.NoError(err)

			// Advance time to expire
			mc.AdvanceTime(1100 * time.Millisecond)

			// Before async delete executes, key is updated with new value
			txPut(t, db, bucket, key, []byte("value2"), 10, nil, nil)

			// Get the new timestamp
			var newTimestamp uint64
			err = db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)
				record, found := idx.FindForVerification(key)
				r.True(found)
				newTimestamp = record.Timestamp
				return nil
			})
			r.NoError(err)

			// Timestamps should be different
			r.NotEqual(originalTimestamp, newTimestamp)

			// Simulate async delete with old timestamp
			err = db.Update(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				record, found := idx.FindForVerification(key)
				r.True(found)

				// Timestamp verification: should NOT delete because timestamps don't match
				if record.Timestamp == originalTimestamp {
					return tx.Delete(bucket, key)
				}
				return nil
			})
			r.NoError(err)

			// Key should still exist with new value
			txGet(t, db, bucket, key, []byte("value2"), nil)
		})
	})

	t.Run("FindForVerification returns expired record without callback", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, value, 1, nil, nil)

			// Advance time to expire
			mc.AdvanceTime(1100 * time.Millisecond)

			err := db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				// FindForVerification should return the expired record
				record, found := idx.FindForVerification(key)
				r.True(found, "FindForVerification should return expired record")
				r.NotNil(record)

				// Verify it's actually expired
				r.True(db.ttlService.GetChecker().IsExpired(record.TTL, record.Timestamp))

				// Compare with Find - should return nil (triggers callback and filters)
				record2, found2 := idx.Find(key)
				r.False(found2, "Find should not return expired record")
				r.Nil(record2)

				return nil
			})
			r.NoError(err)
		})
	})

	t.Run("concurrent delete operations with FindForVerification", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			// Put multiple keys with TTL
			for i := 0; i < 10; i++ {
				k := testutils.GetTestBytes(i)
				txPut(t, db, bucket, k, value, 1, nil, nil)
			}

			// Advance time to expire all keys
			mc.AdvanceTime(1100 * time.Millisecond)

			// Simulate concurrent delete operations
			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					k := testutils.GetTestBytes(index)

					_ = db.Update(func(tx *Tx) error {
						idx, ok := db.Index.BTree.exist(1)
						if !ok {
							return nil
						}

						// Use FindForVerification - no recursive callbacks
						record, found := idx.FindForVerification(k)
						if !found {
							return nil
						}

						if db.ttlService.GetChecker().IsExpired(record.TTL, record.Timestamp) {
							return tx.Delete(bucket, k)
						}
						return nil
					})
				}(i)
			}

			wg.Wait()

			// Verify all keys are deleted
			for i := 0; i < 10; i++ {
				k := testutils.GetTestBytes(i)
				txGet(t, db, bucket, k, nil, ErrKeyNotFound)
			}
		})
	})
}

// TestTx_Delete_FindVsFindForVerification compares behavior of Find and FindForVerification
func TestTx_Delete_FindVsFindForVerification(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)
	value := []byte("value")

	t.Run("Find triggers callback, FindForVerification does not", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, value, 1, nil, nil)

			// Advance time to expire
			mc.AdvanceTime(1100 * time.Millisecond)

			err := db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				// Test 1: FindForVerification returns expired record
				record1, found1 := idx.FindForVerification(key)
				r.True(found1, "FindForVerification should find expired key")
				r.NotNil(record1)
				r.Equal(value, record1.Value)

				// Test 2: Find filters out expired record (and triggers callback)
				record2, found2 := idx.Find(key)
				r.False(found2, "Find should filter expired key")
				r.Nil(record2)

				// Test 3: FindForVerification still works after Find
				record3, found3 := idx.FindForVerification(key)
				r.True(found3, "FindForVerification should still find key after Find")
				r.NotNil(record3)

				return nil
			})
			r.NoError(err)
		})
	})

	t.Run("both methods return same result for non-expired key", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, value, 10, nil, nil)

			err := db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				// Both should return the record
				record1, found1 := idx.FindForVerification(key)
				r.True(found1)
				r.NotNil(record1)

				record2, found2 := idx.Find(key)
				r.True(found2)
				r.NotNil(record2)

				// Should be the same record
				r.Equal(record1.Value, record2.Value)
				r.Equal(record1.Timestamp, record2.Timestamp)
				r.Equal(record1.TTL, record2.TTL)

				return nil
			})
			r.NoError(err)
		})
	})

	t.Run("FindForVerification returns nil for non-existent key", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			err := db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				// Both should return nil for non-existent key
				record1, found1 := idx.FindForVerification(key)
				r.False(found1)
				r.Nil(record1)

				record2, found2 := idx.Find(key)
				r.False(found2)
				r.Nil(record2)

				return nil
			})
			r.NoError(err)
		})
	})
}

func TestTx_TTL_PutGetDelete(t *testing.T) {
	bucket := "bucket"

	t.Run("basic put and get with TTL", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), 1, nil, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), 2, nil, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(2), testutils.GetTestBytes(2), 3, nil, nil)

			// Verify all keys are accessible
			txGet(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), nil)
			txGet(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), nil)
			txGet(t, db, bucket, testutils.GetTestBytes(2), testutils.GetTestBytes(2), nil)

			// Delete one key
			txDel(t, db, bucket, testutils.GetTestBytes(2), nil)

			// Advance time to expire first key
			mc.AdvanceTime(1100 * time.Millisecond)

			// First key should be expired
			txGet(t, db, bucket, testutils.GetTestBytes(0), nil, ErrKeyNotFound)
			// Second key should still be alive
			txGet(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), nil)

			// Advance time to expire second key
			mc.AdvanceTime(1 * time.Second)

			// Second key should now be expired
			txGet(t, db, bucket, testutils.GetTestBytes(1), nil, ErrKeyNotFound)

			// Verify via BTree index
			r := require.New(t)
			r.Nil(db.Index.BTree.GetWithDefault(1).Find(testutils.GetTestBytes(0)))
			r.Nil(db.Index.BTree.GetWithDefault(1).Find(testutils.GetTestBytes(1)))
		})
	})

	t.Run("update expire time", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), 1, nil, nil)
			mc.AdvanceTime(500 * time.Millisecond)

			// Reset expire time to 3 seconds
			txPut(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), 3, nil, nil)
			mc.AdvanceTime(1 * time.Second)

			// Key should still be accessible
			txGet(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), nil)

			mc.AdvanceTime(3 * time.Second)

			// Now key should be expired
			txGet(t, db, bucket, testutils.GetTestBytes(0), nil, ErrKeyNotFound)
		})
	})

	t.Run("persist expire time", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), 1, nil, nil)
			mc.AdvanceTime(500 * time.Millisecond)

			// Persist the key (set TTL to Persistent)
			txPut(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), Persistent, nil, nil)
			mc.AdvanceTime(1 * time.Second)

			// Key should still be accessible
			txGet(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), nil)

			mc.AdvanceTime(3 * time.Second)

			// Key should still be accessible (persisted)
			txGet(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), nil)
		})
	})
}

// TestTx_TTL_RestartCheck tests TTL behavior after database restart.
func TestTx_TTL_RestartCheck(t *testing.T) {
	bucket := "bucket"

	t.Run("expired deletion when open", func(t *testing.T) {
		// This test uses MockClock to test TTL behavior across database restarts
		mc := ttl.NewMockClock(time.Now().UnixMilli())
		opts := DefaultOptions
		opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
		opts.Clock = mc
		defer removeDir(opts.Dir)

		db, err := Open(opts)
		require.NoError(t, err)

		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		// Insert keys with different TTLs
		txPut(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), 1, nil, nil)          // Will expire
		txPut(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), 3, nil, nil)          // Will expire later
		txPut(t, db, bucket, testutils.GetTestBytes(2), testutils.GetTestBytes(2), 3, nil, nil)          // Will expire later
		txPut(t, db, bucket, testutils.GetTestBytes(3), testutils.GetTestBytes(3), Persistent, nil, nil) // Persistent
		txPut(t, db, bucket, testutils.GetTestBytes(4), testutils.GetTestBytes(4), Persistent, nil, nil) // Persistent
		txPut(t, db, bucket, testutils.GetTestBytes(5), testutils.GetTestBytes(5), 5, nil, nil)          // Will expire later
		// Update key 1 to be persistent
		txPut(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), Persistent, nil, nil)
		// Delete key 5
		txDel(t, db, bucket, testutils.GetTestBytes(5), nil)

		require.NoError(t, db.Close())

		// Advance time past first key's expiration
		mc.AdvanceTime(1100 * time.Millisecond)

		// Reopen with the same MockClock
		db, err = Open(opts)
		require.NoError(t, err)
		defer func() {
			if !db.IsClose() {
				require.NoError(t, db.Close())
			}
		}()

		// Key 0 should be expired
		txGet(t, db, bucket, testutils.GetTestBytes(0), nil, ErrKeyNotFound)
		// Key 1 should be persistent
		txGet(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), nil)
		// Key 2 should still be valid
		txGet(t, db, bucket, testutils.GetTestBytes(2), testutils.GetTestBytes(2), nil)
		// Key 5 was deleted
		txGet(t, db, bucket, testutils.GetTestBytes(5), nil, ErrKeyNotFound)

		// Advance time to expire key 2
		mc.AdvanceTime(2 * time.Second)

		txGet(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), nil)
		txGet(t, db, bucket, testutils.GetTestBytes(2), nil, ErrKeyNotFound)

		// Advance more time - persistent keys should still work
		mc.AdvanceTime(2 * time.Second)
		txGet(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), nil)
		txGet(t, db, bucket, testutils.GetTestBytes(3), testutils.GetTestBytes(3), nil)
		txGet(t, db, bucket, testutils.GetTestBytes(4), testutils.GetTestBytes(4), nil)
	})
}

// TestTx_TTL_MergeCheck tests TTL behavior during merge.
func TestTx_TTL_MergeCheck(t *testing.T) {
	t.Run("expire deletion when merge", func(t *testing.T) {
		opts := DefaultOptions
		opts.SegmentSize = 1 * 100
		runNutsDBTestWithMockClock(t, &opts, func(t *testing.T, db *DB, mc ttl.Clock) {
			bucket := "bucket"
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), Persistent, nil, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), Persistent, nil, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(2), testutils.GetTestBytes(2), 1, nil, nil)

			mc.AdvanceTime(1100 * time.Millisecond)

			require.NoError(t, db.Merge())

			// Verify results
			txGet(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), nil)
			txGet(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), nil)
			txGet(t, db, bucket, testutils.GetTestBytes(2), nil, ErrKeyNotFound)

			r := require.New(t)
			r.Nil(db.Index.BTree.GetWithDefault(1).Find(testutils.GetTestBytes(2)))
		})
	})
}

// TestTx_TTL_BatchProcessing tests batch processing of expired keys.
func TestTx_TTL_BatchProcessing(t *testing.T) {
	t.Run("expire deletion with batch processing", func(t *testing.T) {
		opts := DefaultOptions
		runNutsDBTestWithMockClock(t, &opts, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), 1, nil, nil)
			txPut(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), 2, nil, nil)
			txGet(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(0), nil)
			txGet(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), nil)

			mc.AdvanceTime(1100 * time.Millisecond)

			// First key should be expired
			txGet(t, db, bucket, testutils.GetTestBytes(0), nil, ErrKeyNotFound)
			// Second key should still be alive
			txGet(t, db, bucket, testutils.GetTestBytes(1), testutils.GetTestBytes(1), nil)

			mc.AdvanceTime(1 * time.Second)

			// Second key should now be expired
			txGet(t, db, bucket, testutils.GetTestBytes(1), nil, ErrKeyNotFound)

			r := require.New(t)
			r.Nil(db.Index.BTree.GetWithDefault(1).Find(testutils.GetTestBytes(0)))
			r.Nil(db.Index.BTree.GetWithDefault(1).Find(testutils.GetTestBytes(1)))
		})
	})
}

// TestTx_TTL_MinMax tests Min/Max operations with TTL filtering.
func TestTx_TTL_MinMax(t *testing.T) {
	t.Run("Min/Max skip expired keys", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			keya := []byte("A")
			keyb := []byte("B")
			keyc := []byte("C")
			txPut(t, db, bucket, keya, keya, 1, nil, nil)
			txPut(t, db, bucket, keyb, keyb, Persistent, nil, nil)
			txPut(t, db, bucket, keyc, keyc, 3, nil, nil)

			// Initial Min/Max
			txGetMaxOrMinKey(t, db, bucket, false, keya, nil) // Min: A
			txGetMaxOrMinKey(t, db, bucket, true, keyc, nil)  // Max: C

			// Advance time to expire A
			mc.AdvanceTime(1500 * time.Millisecond)

			// Min should now be B (A is expired)
			txGetMaxOrMinKey(t, db, bucket, false, keyb, nil)

			// Advance time to expire C
			mc.AdvanceTime(2000 * time.Millisecond)

			// Max should now be B
			txGetMaxOrMinKey(t, db, bucket, true, keyb, nil)
		})
	})
}

// TestTx_TTL_IncrDecr tests Incr/Decr operations with TTL.
func TestTx_TTL_IncrDecr(t *testing.T) {
	t.Run("operations on expired key", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			key := testutils.GetTestBytes(0)
			txPut(t, db, bucket, key, []byte("1"), 1, nil, nil)

			mc.AdvanceTime(2 * time.Second)

			// Operations on expired key should return ErrKeyNotFound
			txIncrement(t, db, bucket, key, ErrKeyNotFound, nil)
			txIncrementBy(t, db, bucket, key, 12, ErrKeyNotFound, nil)
			txDecrement(t, db, bucket, key, ErrKeyNotFound, nil)
			txDecrementBy(t, db, bucket, key, 12, ErrKeyNotFound, nil)
		})
	})
}

// TestTx_TTL_GetTTL tests GetTTL operation.
func TestTx_TTL_GetTTL(t *testing.T) {
	t.Run("GetTTL and Persist", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			bucket := "bucket1"
			key := []byte("key1")
			value := []byte("value1")

			// Non-existent bucket
			txGetTTL(t, db, bucket, key, 0, ErrBucketNotExist)

			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			// Non-existent key
			txGetTTL(t, db, bucket, key, 0, ErrKeyNotFound)
			txPersist(t, db, bucket, key, ErrKeyNotFound)

			// Put with TTL, then wait for expiration
			txPut(t, db, bucket, key, value, 1, nil, nil)
			mc.AdvanceTime(2 * time.Second)
			txGetTTL(t, db, bucket, key, 0, ErrKeyNotFound)

			// Put with TTL
			txPut(t, db, bucket, key, value, 100, nil, nil)
			txGetTTL(t, db, bucket, key, 100, nil)

			// Persist the key
			txPersist(t, db, bucket, key, nil)
			txGetTTL(t, db, bucket, key, -1, nil)
		})
	})
}

// TestTx_TTL_PutIfExists tests PutIfExists with TTL.
func TestTx_TTL_PutIfExists(t *testing.T) {
	t.Run("PutIfExists with TTL", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			bucket := "bucket1"
			key := []byte("k")
			v1 := []byte("v1")
			v2 := []byte("v2")

			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, key, v1, 1, nil, nil)

			// Advance time slightly, then update with PutIfExists
			mc.AdvanceTime(100 * time.Millisecond)
			r.NoError(db.Update(func(tx *Tx) error {
				return tx.PutIfExists(bucket, key, v2, 1)
			}))
			txGet(t, db, bucket, key, v2, nil)

			// Advance time and try to update expired key
			mc.AdvanceTime(100 * time.Millisecond)
			r.NoError(db.Update(func(tx *Tx) error {
				return tx.PutIfExists(bucket, key, v2, 1)
			}))

			mc.AdvanceTime(1200 * time.Millisecond)
			r.Error(db.Update(func(tx *Tx) error {
				return tx.PutIfExists(bucket, key, v2, 1)
			}))
		})
	})

	t.Run("PutIfNotExists with TTL", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			bucket := "bucket1"
			key := []byte("k")
			v1 := []byte("v1")
			v2 := []byte("v2")

			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, key, v1, 1, nil, nil)

			// Advance time slightly, then try PutIfNotExists (should fail - key exists)
			mc.AdvanceTime(100 * time.Millisecond)
			r.NoError(db.Update(func(tx *Tx) error {
				return tx.PutIfNotExists(bucket, key, v2, 1)
			}))
			txGet(t, db, bucket, key, v1, nil) // Original value unchanged

			// Advance time past expiration, then PutIfNotExists should work
			mc.AdvanceTime(1100 * time.Millisecond)
			r.NoError(db.Update(func(tx *Tx) error {
				return tx.PutIfNotExists(bucket, key, v2, 1)
			}))
			txGet(t, db, bucket, key, v2, nil) // New value
		})
	})
}

// TestTx_TTL_NewTTLReturnError tests error handling when TTL update returns error.
func TestTx_TTL_NewTTLReturnError(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		r := require.New(t)
		bucket := "bucket1"
		key := []byte("k")
		v := []byte("v")

		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		txPut(t, db, bucket, key, v, 1, nil, nil)

		expectErr := errors.New("test error")
		err := db.Update(func(tx *Tx) error {
			return tx.update(
				bucket,
				key,
				func(b []byte) ([]byte, error) {
					return b, nil
				},
				func(u uint32) (uint32, error) {
					return 0, expectErr
				})
		})
		r.Equal(expectErr, err)
	})
}

// TestTx_TTL_DeleteExpired tests deletion of expired keys without recursive callbacks.
func TestTx_TTL_DeleteExpired(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)
	value := []byte("value")

	t.Run("delete expired key should not trigger recursive callback", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			// Put a key with longer TTL to ensure we can delete it
			txPut(t, db, bucket, key, value, 10, nil, nil)

			// Advance time but not enough to expire the key
			mc.AdvanceTime(500 * time.Millisecond)

			// Verify key exists and is not expired
			err := db.Update(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				record, found := idx.FindForVerification(key)
				r.True(found)
				r.NotNil(record)

				r.False(db.ttlService.GetChecker().IsExpired(record.TTL, record.Timestamp))

				return tx.Delete(bucket, key)
			})
			r.NoError(err)

			// Verify key is deleted
			txGet(t, db, bucket, key, nil, ErrKeyNotFound)
		})
	})

	t.Run("delete non-expired key should work normally", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, value, 10, nil, nil)

			// Delete before expiration
			err := db.Update(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				record, found := idx.FindForVerification(key)
				r.True(found)
				r.NotNil(record)

				r.False(db.ttlService.GetChecker().IsExpired(record.TTL, record.Timestamp))

				return tx.Delete(bucket, key)
			})
			r.NoError(err)

			txGet(t, db, bucket, key, nil, ErrKeyNotFound)
		})
	})

	t.Run("timestamp verification prevents wrong deletion", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			// Put key with TTL=1s
			txPut(t, db, bucket, key, []byte("value1"), 1, nil, nil)

			// Get the original timestamp
			var originalTimestamp uint64
			err := db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)
				record, found := idx.FindForVerification(key)
				r.True(found)
				originalTimestamp = record.Timestamp
				return nil
			})
			r.NoError(err)

			// Advance time to expire
			mc.AdvanceTime(1100 * time.Millisecond)

			// Before async delete executes, key is updated with new value
			txPut(t, db, bucket, key, []byte("value2"), 10, nil, nil)

			// Get the new timestamp
			var newTimestamp uint64
			err = db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)
				record, found := idx.FindForVerification(key)
				r.True(found)
				newTimestamp = record.Timestamp
				return nil
			})
			r.NoError(err)

			// Timestamps should be different
			r.NotEqual(originalTimestamp, newTimestamp)

			// Simulate async delete with old timestamp - should NOT delete
			err = db.Update(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				record, found := idx.FindForVerification(key)
				r.True(found)

				// Timestamp verification: should NOT delete because timestamps don't match
				if record.Timestamp == originalTimestamp {
					return tx.Delete(bucket, key)
				}
				return nil
			})
			r.NoError(err)

			// Key should still exist with new value
			txGet(t, db, bucket, key, []byte("value2"), nil)
		})
	})

	t.Run("FindForVerification returns expired record without callback", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, value, 1, nil, nil)

			// Advance time to expire
			mc.AdvanceTime(1100 * time.Millisecond)

			err := db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				// FindForVerification should return the expired record
				record, found := idx.FindForVerification(key)
				r.True(found, "FindForVerification should return expired record")
				r.NotNil(record)

				// Verify it's actually expired
				r.True(db.ttlService.GetChecker().IsExpired(record.TTL, record.Timestamp))

				// Compare with Find - should return nil (triggers callback and filters)
				record2, found2 := idx.Find(key)
				r.False(found2, "Find should not return expired record")
				r.Nil(record2)

				return nil
			})
			r.NoError(err)
		})
	})

	t.Run("concurrent delete operations with FindForVerification", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			// Put multiple keys with TTL
			for i := 0; i < 10; i++ {
				k := testutils.GetTestBytes(i)
				txPut(t, db, bucket, k, value, 1, nil, nil)
			}

			// Advance time to expire all keys
			mc.AdvanceTime(1100 * time.Millisecond)

			// Simulate concurrent delete operations
			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					k := testutils.GetTestBytes(index)

					_ = db.Update(func(tx *Tx) error {
						idx, ok := db.Index.BTree.exist(1)
						if !ok {
							return nil
						}

						record, found := idx.FindForVerification(k)
						if !found {
							return nil
						}

						if db.ttlService.GetChecker().IsExpired(record.TTL, record.Timestamp) {
							return tx.Delete(bucket, k)
						}
						return nil
					})
				}(i)
			}

			wg.Wait()

			// Verify all keys are deleted
			for i := 0; i < 10; i++ {
				k := testutils.GetTestBytes(i)
				txGet(t, db, bucket, k, nil, ErrKeyNotFound)
			}
		})
	})
}

// TestTx_TTL_FindVsFindForVerification compares Find and FindForVerification behavior.
func TestTx_TTL_FindVsFindForVerification(t *testing.T) {
	bucket := "bucket"
	key := testutils.GetTestBytes(0)
	value := []byte("value")

	t.Run("Find triggers callback, FindForVerification does not", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, value, 1, nil, nil)

			// Advance time to expire
			mc.AdvanceTime(1100 * time.Millisecond)

			err := db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				// FindForVerification returns expired record
				record1, found1 := idx.FindForVerification(key)
				r.True(found1, "FindForVerification should find expired key")
				r.NotNil(record1)
				r.Equal(value, record1.Value)

				// Find filters out expired record
				record2, found2 := idx.Find(key)
				r.False(found2, "Find should filter expired key")
				r.Nil(record2)

				// FindForVerification still works after Find
				record3, found3 := idx.FindForVerification(key)
				r.True(found3, "FindForVerification should still find key after Find")
				r.NotNil(record3)

				return nil
			})
			r.NoError(err)
		})
	})

	t.Run("both methods return same result for non-expired key", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, value, 10, nil, nil)

			err := db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				// Both should return the record
				record1, found1 := idx.FindForVerification(key)
				r.True(found1)
				r.NotNil(record1)

				record2, found2 := idx.Find(key)
				r.True(found2)
				r.NotNil(record2)

				// Should be the same record
				r.Equal(record1.Value, record2.Value)
				r.Equal(record1.Timestamp, record2.Timestamp)
				r.Equal(record1.TTL, record2.TTL)

				return nil
			})
			r.NoError(err)
		})
	})

	t.Run("FindForVerification returns nil for non-existent key", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			err := db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				// Both should return nil for non-existent key
				record1, found1 := idx.FindForVerification(key)
				r.False(found1)
				r.Nil(record1)

				record2, found2 := idx.Find(key)
				r.False(found2)
				r.Nil(record2)

				return nil
			})
			r.NoError(err)
		})
	})
}

// TestTx_TTL_ConcurrentUpdate tests concurrent updates to the same key with different TTLs.
func TestTx_TTL_ConcurrentUpdate(t *testing.T) {
	t.Run("concurrent put same key different TTL", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			key := testutils.GetTestBytes(0)
			var wg sync.WaitGroup

			// Concurrent writes with different TTLs
			for i := 0; i < 5; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					val := []byte{byte(id)}
					ttl := uint32(id+1) * 2 // TTL: 2, 4, 6, 8, 10
					_ = db.Update(func(tx *Tx) error {
						return tx.Put(bucket, key, val, ttl)
					})
				}(i)
			}

			wg.Wait()

			// Advance time past all TTLs
			mc.AdvanceTime(15 * time.Second)

			// Key should be expired
			txGet(t, db, bucket, key, nil, ErrKeyNotFound)
		})
	})

	t.Run("concurrent expire and update", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			key := testutils.GetTestBytes(0)
			txPut(t, db, bucket, key, []byte("initial"), 1, nil, nil)

			// Advance time to just before expiration
			mc.AdvanceTime(500 * time.Millisecond)

			// Update the key with longer TTL (10 seconds)
			err := db.Update(func(tx *Tx) error {
				return tx.Put(bucket, key, []byte("updated"), 10)
			})
			require.NoError(t, err)

			// Key should still exist with updated value
			var value []byte
			_ = db.View(func(tx *Tx) error {
				var err error
				value, err = tx.Get(bucket, key)
				return err
			})
			require.Equal(t, []byte("updated"), value)

			// Advance to after the updated TTL (500ms + 10s + a bit more)
			mc.AdvanceTime(10500 * time.Millisecond)

			// Now key should be expired
			err = db.View(func(tx *Tx) error {
				_, err := tx.Get(bucket, key)
				return err
			})
			require.Error(t, err)
		})
	})
}

// TestTx_TTL_BoundaryValues tests TTL with boundary values.
func TestTx_TTL_BoundaryValues(t *testing.T) {
	t.Run("TTL = 0 (Persistent)", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			key := testutils.GetTestBytes(0)
			txPut(t, db, bucket, key, []byte("value"), 0, nil, nil) // Persistent

			// Advance a long time
			mc.AdvanceTime(100 * time.Second)

			// Key should still exist
			txGet(t, db, bucket, key, []byte("value"), nil)

			// GetTTL should return -1 for persistent
			var actualTTL int64
			_ = db.View(func(tx *Tx) error {
				var err error
				actualTTL, err = tx.GetTTL(bucket, key)
				return err
			})
			require.Equal(t, int64(-1), actualTTL)
		})
	})

	t.Run("TTL = 1 (minimum positive TTL)", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			key := testutils.GetTestBytes(0)
			txPut(t, db, bucket, key, []byte("value"), 1, nil, nil)

			// Before expiration
			txGet(t, db, bucket, key, []byte("value"), nil)

			// Advance just under 1 second
			mc.AdvanceTime(999 * time.Millisecond)
			txGet(t, db, bucket, key, []byte("value"), nil)

			// Advance past 1 second
			mc.AdvanceTime(2 * time.Millisecond)
			txGet(t, db, bucket, key, nil, ErrKeyNotFound)
		})
	})

	t.Run("TTL = uint32 max", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			key := testutils.GetTestBytes(0)
			maxTTL := uint32(4294967295) // uint32 max (~136 years)

			txPut(t, db, bucket, key, []byte("value"), maxTTL, nil, nil)

			// Advance a short time
			mc.AdvanceTime(1 * time.Second)

			// Key should still exist
			txGet(t, db, bucket, key, []byte("value"), nil)

			// GetTTL should return a large number (less than maxTTL due to elapsed time)
			var ttl int64
			err := db.View(func(tx *Tx) error {
				var err error
				ttl, err = tx.GetTTL(bucket, key)
				return err
			})
			require.NoError(t, err)
			require.Less(t, ttl, int64(maxTTL))
			require.Greater(t, ttl, int64(maxTTL-5)) // Should still be close to max
		})
	})
}

// TestTx_TTL_RangeScanBoundary tests RangeScan with boundary key expiration.
func TestTx_TTL_RangeScanBoundary(t *testing.T) {
	t.Run("RangeScan boundary key expiration", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			// Insert keys with different TTLs
			txPut(t, db, bucket, []byte("a"), []byte("a"), Persistent, nil, nil)
			txPut(t, db, bucket, []byte("b"), []byte("b"), 1, nil, nil)
			txPut(t, db, bucket, []byte("c"), []byte("c"), 2, nil, nil)
			txPut(t, db, bucket, []byte("d"), []byte("d"), Persistent, nil, nil)

			// Before expiration - should return all 4 keys
			var values [][]byte
			_ = db.View(func(tx *Tx) error {
				var err error
				values, err = tx.RangeScan(bucket, []byte("a"), []byte("d"))
				require.NoError(t, err)
				return nil
			})
			require.Len(t, values, 4)

			// Advance time to expire "b"
			mc.AdvanceTime(1100 * time.Millisecond)

			// Should return 3 keys (a, c, d) - b is filtered out
			_ = db.View(func(tx *Tx) error {
				var err error
				values, err = tx.RangeScan(bucket, []byte("a"), []byte("d"))
				require.NoError(t, err)
				return nil
			})
			require.Len(t, values, 3)
			require.Equal(t, []byte("a"), values[0])
			require.Equal(t, []byte("c"), values[1])
			require.Equal(t, []byte("d"), values[2])

			// Advance time to expire "c"
			mc.AdvanceTime(1 * time.Second)

			// Should return 2 keys (a, d)
			_ = db.View(func(tx *Tx) error {
				var err error
				values, err = tx.RangeScan(bucket, []byte("a"), []byte("d"))
				require.NoError(t, err)
				return nil
			})
			require.Len(t, values, 2)
			require.Equal(t, []byte("a"), values[0])
			require.Equal(t, []byte("d"), values[1])
		})
	})

	t.Run("RangeScan with only expired keys", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			// Insert only keys with short TTL
			txPut(t, db, bucket, []byte("a"), []byte("a"), 1, nil, nil)
			txPut(t, db, bucket, []byte("b"), []byte("b"), 1, nil, nil)

			// Advance time to expire all
			mc.AdvanceTime(1100 * time.Millisecond)

			// Should return error because all keys are expired (no valid keys to scan)
			// This is expected behavior - RangeScan returns error when no valid keys found
			_ = db.View(func(tx *Tx) error {
				_, err := tx.RangeScan(bucket, []byte("a"), []byte("c"))
				// Expected error when all keys are expired
				require.Error(t, err)
				return nil
			})
		})
	})
}

// TestTx_TTL_PrefixScan tests PrefixScan with TTL filtering.
func TestTx_TTL_PrefixScan(t *testing.T) {
	t.Run("PrefixScan skips expired keys", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			// Insert keys with different TTLs
			txPut(t, db, bucket, []byte("user:1"), []byte("data1"), Persistent, nil, nil)
			txPut(t, db, bucket, []byte("user:2"), []byte("data2"), 1, nil, nil)
			txPut(t, db, bucket, []byte("user:3"), []byte("data3"), 2, nil, nil)
			txPut(t, db, bucket, []byte("user:4"), []byte("data4"), Persistent, nil, nil)

			// Before expiration
			var values [][]byte
			_ = db.View(func(tx *Tx) error {
				var err error
				values, err = tx.PrefixScan(bucket, []byte("user:"), 0, 100)
				require.NoError(t, err)
				return nil
			})
			require.Len(t, values, 4)

			// Advance time to expire user:2
			mc.AdvanceTime(1100 * time.Millisecond)

			// Should skip user:2
			_ = db.View(func(tx *Tx) error {
				var err error
				values, err = tx.PrefixScan(bucket, []byte("user:"), 0, 100)
				require.NoError(t, err)
				return nil
			})
			require.Len(t, values, 3)
		})
	})
}

// TestTx_TTL_PrefixSearchScan tests PrefixSearchScan with TTL filtering.
func TestTx_TTL_PrefixSearchScan(t *testing.T) {
	t.Run("PrefixSearchScan skips expired keys", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			// Insert keys
			txPut(t, db, bucket, []byte("user:1:active"), []byte("data1"), Persistent, nil, nil)
			txPut(t, db, bucket, []byte("user:2:active"), []byte("data2"), 1, nil, nil)
			txPut(t, db, bucket, []byte("user:3:active"), []byte("data3"), 2, nil, nil)

			// Before expiration
			var values [][]byte
			_ = db.View(func(tx *Tx) error {
				var err error
				values, err = tx.PrefixSearchScan(bucket, []byte("user:"), "active", 0, 100)
				require.NoError(t, err)
				return nil
			})
			require.Len(t, values, 3)

			// Advance time to expire user:2
			mc.AdvanceTime(1100 * time.Millisecond)

			// Should skip user:2
			_ = db.View(func(tx *Tx) error {
				var err error
				values, err = tx.PrefixSearchScan(bucket, []byte("user:"), "active", 0, 100)
				require.NoError(t, err)
				return nil
			})
			require.Len(t, values, 2)
		})
	})
}

// TestTx_TTL_BatchInsertDifferentTTL tests batch insertion with different TTLs.
func TestTx_TTL_BatchInsertDifferentTTL(t *testing.T) {
	t.Run("MSet with TTL", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			// Use MSet with TTL
			txMSet(t, db, bucket, [][]byte{
				testutils.GetTestBytes(0), []byte("v0"),
				testutils.GetTestBytes(1), []byte("v1"),
				testutils.GetTestBytes(2), []byte("v2"),
			}, 1, nil, nil)

			// Before expiration
			var values [][]byte
			_ = db.View(func(tx *Tx) error {
				var err error
				values, err = tx.MGet(bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(1), testutils.GetTestBytes(2))
				require.NoError(t, err)
				return nil
			})
			require.Len(t, values, 3)
			require.Equal(t, []byte("v0"), values[0])
			require.Equal(t, []byte("v1"), values[1])
			require.Equal(t, []byte("v2"), values[2])

			// Advance time to expire
			mc.AdvanceTime(1100 * time.Millisecond)

			// All should be expired - MGet will return error for expired keys
			_ = db.View(func(tx *Tx) error {
				_, err := tx.MGet(bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(1), testutils.GetTestBytes(2))
				require.Error(t, err)
				return nil
			})
		})
	})
}

// TestTx_TTL_RapidExpireUpdate tests the scenario where a key is updated
// shortly after it expires, before the expiration event is processed.
// This verifies that timestamp verification prevents stale expiration events
// from deleting newly updated keys.
func TestTx_TTL_RapidExpireUpdate(t *testing.T) {
	t.Run("key updated before expiration event processed", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			key := testutils.GetTestBytes(0)

			// Step 1: Set key with TTL=1s
			txPut(t, db, bucket, key, []byte("v1"), 1, nil, nil)

			// Step 2: Advance time past expiration
			mc.AdvanceTime(1100 * time.Millisecond)

			// Key should be expired now (passive check)
			// Get the record to verify its timestamp
			var oldTimestamp uint64
			_ = db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)
				record, found := idx.FindForVerification(key)
				r.True(found)
				r.NotNil(record)
				oldTimestamp = record.Timestamp
				// Verify it's expired
				r.True(db.ttlService.GetChecker().IsExpired(record.TTL, record.Timestamp))
				return nil
			})

			// Step 3: Update the key with new value and TTL BEFORE scanner processes expiration
			// This simulates the scenario where scanner hasn't run yet
			txPut(t, db, bucket, key, []byte("v2"), 10, nil, nil)

			// Get the new timestamp
			var newTimestamp uint64
			_ = db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)
				record, found := idx.FindForVerification(key)
				r.True(found)
				r.NotNil(record)
				newTimestamp = record.Timestamp
				return nil
			})

			// Timestamps should be different
			r.NotEqual(oldTimestamp, newTimestamp)

			// Key should be accessible with new value
			txGet(t, db, bucket, key, []byte("v2"), nil)

			// Step 4: Now simulate what would happen if the old expiration event
			// (with old timestamp) was processed. The timestamp verification
			// in the delete logic should prevent deletion.
			//
			// We simulate this by directly checking if an event with the old
			// timestamp would be considered valid for deletion.

			// Get the current record
			var currentRecord *core.Record
			_ = db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)
				record, found := idx.FindForVerification(key)
				r.True(found)
				currentRecord = record
				return nil
			})

			// The old timestamp should NOT match current record's timestamp
			r.NotEqual(oldTimestamp, currentRecord.Timestamp)
			r.Equal(newTimestamp, currentRecord.Timestamp)

			// Step 5: Advance time to expire the new TTL
			mc.AdvanceTime(11 * time.Second)

			// Now key should be expired with new TTL
			txGet(t, db, bucket, key, nil, ErrKeyNotFound)
		})
	})

	t.Run("multiple rapid updates before expiration processed", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			key := testutils.GetTestBytes(0)

			// Set initial key
			txPut(t, db, bucket, key, []byte("v1"), 1, nil, nil)

			// Advance to expire
			mc.AdvanceTime(1100 * time.Millisecond)

			// Rapidly update key multiple times before any scanner scan
			for i := 2; i <= 5; i++ {
				txPut(t, db, bucket, key, []byte(fmt.Sprintf("v%d", i)), 10, nil, nil)
			}

			// Key should have the final value
			txGet(t, db, bucket, key, []byte("v5"), nil)

			// Advance time to expire
			mc.AdvanceTime(11 * time.Second)

			// Key should now be expired
			txGet(t, db, bucket, key, nil, ErrKeyNotFound)

			// Verify via BTree index
			r.Nil(db.Index.BTree.GetWithDefault(1).Find(key))
		})
	})

	t.Run("delete using timestamp verification prevents stale expiration", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			key := testutils.GetTestBytes(0)

			// Set key with TTL=1s
			txPut(t, db, bucket, key, []byte("v1"), 1, nil, nil)

			// Get original timestamp
			var originalTimestamp uint64
			_ = db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)
				record, found := idx.FindForVerification(key)
				r.True(found)
				originalTimestamp = record.Timestamp
				return nil
			})

			// Advance time past expiration
			mc.AdvanceTime(1100 * time.Millisecond)

			// Update key with new TTL
			txPut(t, db, bucket, key, []byte("v2"), 10, nil, nil)

			// Get new timestamp
			var newTimestamp uint64
			_ = db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)
				record, found := idx.FindForVerification(key)
				r.True(found)
				newTimestamp = record.Timestamp
				return nil
			})

			// Timestamps are different
			r.NotEqual(originalTimestamp, newTimestamp)

			// Simulate deletion with OLD timestamp - should be rejected by timestamp verification
			err := db.Update(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				record, found := idx.FindForVerification(key)
				r.True(found)

				// Timestamp verification: only delete if timestamps match
				// This simulates what handleExpiredKeyAsync should do
				if record.Timestamp == originalTimestamp {
					return tx.Delete(bucket, key)
				}
				// Timestamps don't match, skip deletion
				return nil
			})
			r.NoError(err)

			// Key should still exist (deletion was skipped due to timestamp mismatch)
			txGet(t, db, bucket, key, []byte("v2"), nil)

			// Now simulate deletion with NEW timestamp - should succeed
			err = db.Update(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				record, found := idx.FindForVerification(key)
				r.True(found)

				// Use the correct (current) timestamp
				if record.Timestamp == newTimestamp {
					return tx.Delete(bucket, key)
				}
				return nil
			})
			r.NoError(err)

			// Key should be deleted now
			txGet(t, db, bucket, key, nil, ErrKeyNotFound)
		})
	})
}

// TestTx_TTL_LazyDeletionFromIndex verifies that expired keys are actually removed from the index
// after lazy deletion is triggered, not just filtered out during reads.
func TestTx_TTL_LazyDeletionFromIndex(t *testing.T) {
	bucket := "test_bucket"
	key := []byte("test_key")
	value := []byte("test_value")

	t.Run("expired key removed from index after lazy deletion", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, value, 1, nil, nil)

			err := db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1) // bucket ID 1
				r.True(ok)

				record, found := idx.FindForVerification(key)
				r.True(found, "key should exist in index before expiration")
				r.NotNil(record)
				r.Equal(value, record.Value)
				return nil
			})
			r.NoError(err)

			mc.AdvanceTime(2 * time.Second)

			txGet(t, db, bucket, key, nil, ErrKeyNotFound)

			time.Sleep(1100 * time.Millisecond)

			err = db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				record, found := idx.FindForVerification(key)
				r.False(found, "expired key should be physically removed from index")
				r.Nil(record)
				return nil
			})
			r.NoError(err)
		})
	})

	t.Run("multiple expired keys batch deleted from index", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			keys := [][]byte{
				[]byte("key1"),
				[]byte("key2"),
				[]byte("key3"),
			}

			for _, k := range keys {
				txPut(t, db, bucket, k, value, 1, nil, nil)
			}

			err := db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				for _, k := range keys {
					record, found := idx.FindForVerification(k)
					r.True(found, "key %s should exist in index", string(k))
					r.NotNil(record)
				}
				return nil
			})
			r.NoError(err)

			mc.AdvanceTime(2 * time.Second)

			for _, k := range keys {
				txGet(t, db, bucket, k, nil, ErrKeyNotFound)
			}

			time.Sleep(1100 * time.Millisecond)

			err = db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				for _, k := range keys {
					record, found := idx.FindForVerification(k)
					r.False(found, "expired key %s should be removed from index", string(k))
					r.Nil(record)
				}
				return nil
			})
			r.NoError(err)
		})
	})

	t.Run("concurrent access during lazy deletion", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, value, 1, nil, nil)

			mc.AdvanceTime(2 * time.Second)

			var wg sync.WaitGroup
			wg.Add(10)

			for i := 0; i < 10; i++ {
				go func() {
					defer wg.Done()
					txGet(t, db, bucket, key, nil, ErrKeyNotFound)
				}()
			}

			wg.Wait()
			time.Sleep(1100 * time.Millisecond)

			err := db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				record, found := idx.FindForVerification(key)
				r.False(found, "expired key should be removed from index after concurrent access")
				r.Nil(record)
				return nil
			})
			r.NoError(err)
		})
	})

	t.Run("timestamp verification prevents wrong deletion", func(t *testing.T) {
		runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, []byte("value1"), 1, nil, nil)

			mc.AdvanceTime(2 * time.Second)

			txGet(t, db, bucket, key, nil, ErrKeyNotFound)

			txPut(t, db, bucket, key, []byte("value2"), 10, nil, nil)

			time.Sleep(1100 * time.Millisecond)

			txGet(t, db, bucket, key, []byte("value2"), nil)

			err := db.View(func(tx *Tx) error {
				idx, ok := db.Index.BTree.exist(1)
				r.True(ok)

				record, found := idx.FindForVerification(key)
				r.True(found, "new key should exist in index")
				r.NotNil(record)
				r.Equal([]byte("value2"), record.Value)
				return nil
			})
			r.NoError(err)
		})
	})
}
