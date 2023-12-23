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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xujiajun/utils/strconv2"
)

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

func TestTx_GetAll(t *testing.T) {
	bucket := "bucket_for_scanAll"

	t.Run("get_all_from_empty_db", func(t *testing.T) {

		withDefaultDB(t, func(t *testing.T, db *DB) {

			tx, err := db.Begin(false)
			require.NoError(t, err)
			defer assert.NoError(t, tx.Commit())

			_, err = tx.GetAll(bucket)
			assert.Error(t, err)
		})
	})

	t.Run("get_all_from_db", func(t *testing.T) {

		withDefaultDB(t, func(t *testing.T, db *DB) {

			{
				// setup the data
				txCreateBucket(t, db, DataStructureBTree, bucket, nil)
				tx, err := db.Begin(true)
				require.NoError(t, err)

				key0 := []byte("key_" + fmt.Sprintf("%07d", 0))
				val0 := []byte("val" + fmt.Sprintf("%07d", 0))
				err = tx.Put(bucket, key0, val0, Persistent)
				assert.NoError(t, err)

				key1 := []byte("key_" + fmt.Sprintf("%07d", 1))
				val1 := []byte("val" + fmt.Sprintf("%07d", 1))
				err = tx.Put(bucket, key1, val1, Persistent)
				assert.NoError(t, err)

				assert.NoError(t, tx.Commit())
			}

			{
				// check the data

				tx, err = db.Begin(false)
				require.NoError(t, err)

				values, err := tx.GetAll(bucket)
				assert.NoError(t, err)

				for i, value := range values {
					wantVal := []byte("val" + fmt.Sprintf("%07d", i))
					assert.Equal(t, wantVal, value)
				}

				assert.NoError(t, tx.Commit())

			}
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
			txPut(t, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
		}

		for i := 0; i < 10; i++ {
			txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), nil)
		}

		txDel(t, db, bucket, GetTestBytes(3), nil)

		err := db.View(func(tx *Tx) error {
			r, ok := db.Index.bTree.getWithDefault(1).Find(GetTestBytes(3))
			require.Nil(t, r)
			require.False(t, ok)

			return nil
		})

		require.NoError(t, err)
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
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, GetTestBytes(0), GetTestBytes(0), 1, nil, nil)
			txPut(t, db, bucket, GetTestBytes(1), GetTestBytes(1), 2, nil, nil)
			txPut(t, db, bucket, GetTestBytes(2), GetTestBytes(2), 3, nil, nil)
			txGet(t, db, bucket, GetTestBytes(0), GetTestBytes(0), nil)
			txGet(t, db, bucket, GetTestBytes(1), GetTestBytes(1), nil)
			txGet(t, db, bucket, GetTestBytes(2), GetTestBytes(2), nil)

			txDel(t, db, bucket, GetTestBytes(2), nil)

			time.Sleep(1100 * time.Millisecond)

			// this entry will be deleted
			txGet(t, db, bucket, GetTestBytes(0), nil, ErrKeyNotFound)
			// this entry still alive
			txGet(t, db, bucket, GetTestBytes(1), GetTestBytes(1), nil)

			time.Sleep(1 * time.Second)

			// this entry will be deleted
			txGet(t, db, bucket, GetTestBytes(1), nil, ErrKeyNotFound)

			r, ok := db.Index.bTree.getWithDefault(1).Find(GetTestBytes(0))
			require.Nil(t, r)
			require.False(t, ok)

			r, ok = db.Index.bTree.getWithDefault(1).Find(GetTestBytes(1))
			require.Nil(t, r)
			require.False(t, ok)
		})
	})

	t.Run("update expire time", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, GetTestBytes(0), GetTestBytes(0), 1, nil, nil)
			time.Sleep(500 * time.Millisecond)

			// reset expire time
			txPut(t, db, bucket, GetTestBytes(0), GetTestBytes(0), 3, nil, nil)
			time.Sleep(1 * time.Second)
			txGet(t, db, bucket, GetTestBytes(0), GetTestBytes(0), nil)

			time.Sleep(3 * time.Second)
			txGet(t, db, bucket, GetTestBytes(0), nil, ErrKeyNotFound)
		})
	})

	t.Run("persist expire time", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, GetTestBytes(0), GetTestBytes(0), 1, nil, nil)
			time.Sleep(500 * time.Millisecond)

			// persist expire time
			txPut(t, db, bucket, GetTestBytes(0), GetTestBytes(0), Persistent, nil, nil)
			time.Sleep(1 * time.Second)
			txGet(t, db, bucket, GetTestBytes(0), GetTestBytes(0), nil)

			time.Sleep(3 * time.Second)
			txGet(t, db, bucket, GetTestBytes(0), GetTestBytes(0), nil)
		})
	})

	t.Run("expired deletion when open", func(t *testing.T) {
		opts := DefaultOptions
		runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, GetTestBytes(0), GetTestBytes(0), 1, nil, nil)
			txPut(t, db, bucket, GetTestBytes(1), GetTestBytes(1), 3, nil, nil)
			txPut(t, db, bucket, GetTestBytes(2), GetTestBytes(2), 3, nil, nil)
			txPut(t, db, bucket, GetTestBytes(3), GetTestBytes(3), Persistent, nil, nil)
			txPut(t, db, bucket, GetTestBytes(4), GetTestBytes(4), Persistent, nil, nil)
			txPut(t, db, bucket, GetTestBytes(5), GetTestBytes(5), 5, nil, nil)
			txPut(t, db, bucket, GetTestBytes(1), GetTestBytes(1), Persistent, nil, nil)
			txDel(t, db, bucket, GetTestBytes(5), nil)

			require.NoError(t, db.Close())

			time.Sleep(1100 * time.Millisecond)

			db, err := Open(opts)
			require.NoError(t, err)

			txGet(t, db, bucket, GetTestBytes(0), nil, ErrKeyNotFound)
			txGet(t, db, bucket, GetTestBytes(1), GetTestBytes(1), nil)
			txGet(t, db, bucket, GetTestBytes(2), GetTestBytes(2), nil)
			txGet(t, db, bucket, GetTestBytes(5), nil, ErrKeyNotFound)

			time.Sleep(2 * time.Second)

			txGet(t, db, bucket, GetTestBytes(1), GetTestBytes(1), nil)
			txGet(t, db, bucket, GetTestBytes(2), GetTestBytes(2), ErrKeyNotFound)

			time.Sleep(2 * time.Second)
			// this entry should be persistent
			txGet(t, db, bucket, GetTestBytes(1), GetTestBytes(1), nil)
			txGet(t, db, bucket, GetTestBytes(3), GetTestBytes(3), nil)
			txGet(t, db, bucket, GetTestBytes(4), GetTestBytes(4), nil)
		})
	})

	t.Run("expire deletion when merge", func(t *testing.T) {
		opts := DefaultOptions
		opts.SegmentSize = 1 * 100
		runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
			bucket := "bucket"
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, GetTestBytes(0), GetTestBytes(0), Persistent, nil, nil)
			txPut(t, db, bucket, GetTestBytes(1), GetTestBytes(1), Persistent, nil, nil)
			txPut(t, db, bucket, GetTestBytes(2), GetTestBytes(2), 1, nil, nil)

			time.Sleep(1100 * time.Millisecond)

			require.NoError(t, db.Merge())

			txGet(t, db, bucket, GetTestBytes(0), GetTestBytes(0), nil)
			txGet(t, db, bucket, GetTestBytes(1), GetTestBytes(1), nil)
			txGet(t, db, bucket, GetTestBytes(2), nil, ErrKeyNotFound)

			r, ok := db.Index.bTree.getWithDefault(1).Find(GetTestBytes(2))
			require.Nil(t, r)
			require.False(t, ok)
		})
	})

	t.Run("expire deletion use time heap", func(t *testing.T) {
		opts := DefaultOptions
		opts.ExpiredDeleteType = TimeHeap
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, GetTestBytes(0), GetTestBytes(0), 1, nil, nil)
			txPut(t, db, bucket, GetTestBytes(1), GetTestBytes(1), 2, nil, nil)
			txGet(t, db, bucket, GetTestBytes(0), GetTestBytes(0), nil)
			txGet(t, db, bucket, GetTestBytes(1), GetTestBytes(1), nil)

			time.Sleep(1100 * time.Millisecond)

			// this entry will be deleted
			txGet(t, db, bucket, GetTestBytes(0), nil, ErrKeyNotFound)
			// this entry still alive
			txGet(t, db, bucket, GetTestBytes(1), GetTestBytes(1), nil)

			time.Sleep(1 * time.Second)

			// this entry will be deleted
			txGet(t, db, bucket, GetTestBytes(1), nil, ErrKeyNotFound)

			r, ok := db.Index.bTree.getWithDefault(1).Find(GetTestBytes(0))
			require.Nil(t, r)
			require.False(t, ok)

			r, ok = db.Index.bTree.getWithDefault(1).Find(GetTestBytes(1))
			require.Nil(t, r)
			require.False(t, ok)
		})
	})
}

func TestTx_GetMaxOrMinKey(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 10; i++ {
			txPut(t, db, bucket, GetTestBytes(i), GetRandomBytes(24), Persistent, nil, nil)
		}

		txGetMaxOrMinKey(t, db, bucket, true, GetTestBytes(9), nil)
		txGetMaxOrMinKey(t, db, bucket, false, GetTestBytes(0), nil)

		txDel(t, db, bucket, GetTestBytes(9), nil)
		txDel(t, db, bucket, GetTestBytes(0), nil)

		txGetMaxOrMinKey(t, db, bucket, true, GetTestBytes(8), nil)
		txGetMaxOrMinKey(t, db, bucket, false, GetTestBytes(1), nil)

		txPut(t, db, bucket, GetTestBytes(-1), GetRandomBytes(24), Persistent, nil, nil)
		txPut(t, db, bucket, GetTestBytes(100), GetRandomBytes(24), Persistent, nil, nil)

		txGetMaxOrMinKey(t, db, bucket, false, GetTestBytes(-1), nil)
		txGetMaxOrMinKey(t, db, bucket, true, GetTestBytes(100), nil)
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
	getIntegerValue := func(value int64) []byte {
		return []byte(fmt.Sprintf("%d", value))
	}

	key := GetTestBytes(0)

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
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txPut(t, db, bucket, key, []byte("1"), 1, nil, nil)

			time.Sleep(2 * time.Second)

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
			txPutIfNotExists(t, db, bucket, GetTestBytes(i), val, nil, nil)
		}

		for i := 0; i < 10; i++ {
			txGet(t, db, bucket, GetTestBytes(i), val, nil)
		}

		for i := 0; i < 10; i++ {
			txPutIfExists(t, db, bucket, GetTestBytes(i), updated_val, nil, nil)
		}

		for i := 0; i < 10; i++ {
			txGet(t, db, bucket, GetTestBytes(i), updated_val, nil)
		}
	})
}

func TestTx_GetAndSetBit(t *testing.T) {
	bucket := "bucket"
	key := GetTestBytes(0)

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
			txPut(t, db, bucket, GetTestBytes(1), val, Persistent, nil, nil)
			txValueLen(t, db, bucket, GetTestBytes(1), 5, nil)
		})
	})

	t.Run("bucket not exist", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txValueLen(t, db, bucket, GetTestBytes(1), 0, ErrBucketNotExist)
		})
	})

	t.Run("key not found", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, GetTestBytes(1), val, Persistent, nil, nil)
			txValueLen(t, db, bucket, GetTestBytes(2), 0, ErrKeyNotFound)
		})
	})

	t.Run("expired test", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, GetTestBytes(1), val, 1, nil, nil)
			time.Sleep(3 * time.Second)
			txValueLen(t, db, bucket, GetTestBytes(1), 0, ErrKeyNotFound)
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
			txPut(t, db, bucket, GetTestBytes(1), val, Persistent, nil, nil)
			txGetSet(t, db, bucket, GetTestBytes(1), newVal, val, nil)
			txGet(t, db, bucket, GetTestBytes(1), newVal, nil)
		})
	})

	t.Run("bucket not exist", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txGetSet(t, db, bucket, GetTestBytes(1), newVal, nil, ErrBucketNotExist)
		})
	})

	t.Run("expired test", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txPut(t, db, bucket, GetTestBytes(1), val, 1, nil, nil)
			time.Sleep(3 * time.Second)
			txGetSet(t, db, bucket, GetTestBytes(1), newVal, nil, ErrKeyNotFound)
		})
	})
}

func TestTx_GetTTLAndPersist(t *testing.T) {
	bucket := "bucket1"
	key := []byte("key1")
	value := []byte("value1")
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txGetTTL(t, db, bucket, key, 0, ErrBucketNotExist)

		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		txGetTTL(t, db, bucket, key, 0, ErrKeyNotFound)
		txPersist(t, db, bucket, key, ErrKeyNotFound)

		txPut(t, db, bucket, key, value, 1, nil, nil)
		time.Sleep(2 * time.Second) // Wait till value to expire
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
				GetTestBytes(0), GetTestBytes(1), GetTestBytes(2),
			}, Persistent, ErrKVArgsLenNotEven, nil)
		})
	})

	t.Run("use MSet and MGet normally", func(t *testing.T) {
		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			txMSet(t, db, bucket, [][]byte{
				GetTestBytes(0), GetTestBytes(1), GetTestBytes(2), GetTestBytes(3),
			}, Persistent, nil, nil)
			txMGet(t, db, bucket, [][]byte{
				GetTestBytes(0), GetTestBytes(2),
			}, [][]byte{
				GetTestBytes(1), GetTestBytes(3),
			}, nil, nil)
		})
	})
}

func TestTx_Append(t *testing.T) {
	bucket := "bucket"
	key := GetTestBytes(0)

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

			txAppend(t, db, bucket, key, GetTestBytes(1), nil, nil)
			txGet(t, db, bucket, key, GetTestBytes(1), nil)
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
	key := GetTestBytes(0)

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
	key := GetTestBytes(0)

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
