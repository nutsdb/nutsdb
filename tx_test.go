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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// todo to check is there any deadlock here?
func TestTx_Rollback(t *testing.T) {

	withDefaultDB(t, func(t *testing.T, db *DB) {
		bucket := "bucket_rollback_test"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		tx, err := db.Begin(true)
		assert.NoError(t, err)

		for i := 0; i < 10; i++ {
			key := []byte("key_" + fmt.Sprintf("%03d", i))
			val := []byte("val_" + fmt.Sprintf("%03d", i))
			if i == 7 {
				key = []byte("") // set error key to make tx rollback
			}
			if err = tx.Put(bucket, key, val, Persistent); err != nil {
				// tx rollback
				tx.Rollback()

				if i < 7 {
					t.Fatal("err TestTx_Rollback")
				}
			}
		}

		// no one found
		for i := 0; i <= 10; i++ {
			tx, err = db.Begin(false)
			assert.NoError(t, err)

			key := []byte("key_" + fmt.Sprintf("%03d", i))
			if _, err := tx.Get(bucket, key); err != nil {
				// tx rollback
				tx.Rollback()
			} else {
				t.Fatal("err TestTx_Rollback")
			}
		}

	})
}

func TestTx_Begin(t *testing.T) {
	t.Run("Begin with default options, with only read", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			tx, err := db.Begin(false)
			assert.NoError(t, err)

			err = tx.Rollback()
			assert.NoError(t, err)

			err = db.Close()
			assert.NoError(t, err)
		})
	})

	t.Run("Begin with default options, with writable", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			tx, err := db.Begin(true)
			assert.NoError(t, err)

			tx.Rollback()

			err = db.Close()
			assert.NoError(t, err)
		})
	})

	t.Run("Begin with error: begin the closed db", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			tx, err := db.Begin(true)
			assert.NoError(t, err)

			tx.Rollback() // for unlock mutex

			err = db.Close()
			assert.NoError(t, err)

			_, err = db.Begin(false)
			assert.Error(t, err)
		})
	})
}

func TestTx_Close(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		tx, err := db.Begin(false)
		assert.NoError(t, err)

		err = tx.Rollback()
		assert.NoError(t, err)

		bucket := "bucket_tx_close_test"

		_, err = tx.Get(bucket, []byte("foo"))
		assert.Errorf(t, err, "err TestTx_Close")

		_, err = tx.RangeScan(bucket, []byte("foo0"), []byte("foo1"))
		assert.Errorf(t, err, "err TestTx_Close")

		_, err = tx.PrefixScan(bucket, []byte("foo"), 0, 1)
		assert.Errorf(t, err, "err TestTx_Close")

		_, err = tx.PrefixSearchScan(bucket, []byte("f"), "oo", 0, 1)
		assert.Errorf(t, err, "err TestTx_Close")
	})
}

func TestTx_CommittedStatus(t *testing.T) {

	withRAMIdxDB(t, func(t *testing.T, db *DB) {

		bucket := "bucket_committed_status"

		{ // setup data
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			tx, err := db.Begin(true)
			assert.NoError(t, err)

			err = tx.Put(bucket, []byte("key1"), []byte("value1"), 0)
			assert.NoError(t, err)

			err = tx.Put(bucket, []byte("key2"), []byte("value2"), 0)
			assert.NoError(t, err)

			err = tx.Commit()
			assert.NoError(t, err)
		}
	})
}

func TestTx_PutWithTimestamp(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		bucket := "bucket_put_with_timestamp"

		timestamps := []uint64{1547707905, 1547707910, uint64(time.Now().Unix())}

		{ // put with timestamp
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			tx, err := db.Begin(true)
			assert.NoError(t, err)
			for i, timestamp := range timestamps {
				key := []byte("key_" + fmt.Sprintf("%03d", i))
				val := []byte("val_" + fmt.Sprintf("%03d", i))

				err = tx.PutWithTimestamp(bucket, key, val, 0, timestamp)
				assert.NoError(t, err)

			}
			err = tx.Commit()
			assert.NoError(t, err)
		}
	})
}

func TestTx_Commit(t *testing.T) {
	t.Run("build_bucket_indexes_after_commit", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			bucket := "bucket1"

			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			assert.Equal(t, 1, db.Index.BTree.getIdxLen())
			txDeleteBucket(t, db, DataStructureBTree, bucket, nil)
			assert.Equal(t, 0, db.Index.BTree.getIdxLen())

			txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)
			assert.Equal(t, 1, db.Index.SortedSet.getIdxLen())
			txDeleteBucket(t, db, DataStructureSortedSet, bucket, nil)
			assert.Equal(t, 0, db.Index.SortedSet.getIdxLen())

			txCreateBucket(t, db, DataStructureList, bucket, nil)
			assert.Equal(t, 1, db.Index.List.getIdxLen())
			txDeleteBucket(t, db, DataStructureList, bucket, nil)
			assert.Equal(t, 0, db.Index.List.getIdxLen())

			txCreateBucket(t, db, DataStructureSet, bucket, nil)
			assert.Equal(t, 1, db.Index.Set.getIdxLen())
			txDeleteBucket(t, db, DataStructureSet, bucket, nil)
			assert.Equal(t, 0, db.Index.Set.getIdxLen())

		})
	})

}
