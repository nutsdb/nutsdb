// Copyright 2023 The nutsdb Author. All rights reserved.
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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xujiajun/utils/strconv2"
)

func TestDB_MergeForString(t *testing.T) {
	bucket := "bucket"
	opts := DefaultOptions
	opts.SegmentSize = kb
	opts.Dir = "/tmp/test-string-merge/"

	for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		opts.EntryIdxMode = idxMode
		db, err := Open(opts)
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		require.NoError(t, err)

		// Merge is not needed
		err = db.Merge()
		require.Equal(t, ErrDontNeedMerge, err)

		// Add some data
		n := 1000
		for i := 0; i < n; i++ {
			txPut(t, db, bucket, getTestBytes(i), getTestBytes(i), Persistent, nil, nil)
		}

		// Delete some data
		for i := 0; i < n/2; i++ {
			txDel(t, db, bucket, getTestBytes(i), nil)
		}

		// Merge and check the result
		require.NoError(t, db.Merge())

		dbCnt, err := db.getRecordCount()
		require.NoError(t, err)
		require.Equal(t, int64(n/2), dbCnt)

		// Check the deleted data is deleted
		for i := 0; i < n/2; i++ {
			txGet(t, db, bucket, getTestBytes(i), getTestBytes(i), ErrKeyNotFound)
		}

		// Check the added data is added
		for i := n / 2; i < n; i++ {
			txGet(t, db, bucket, getTestBytes(i), getTestBytes(i), nil)
		}

		// Close and reopen the db
		require.NoError(t, db.Close())

		db, err = Open(opts)
		require.NoError(t, err)

		dbCnt, err = db.getRecordCount()
		require.NoError(t, err)
		require.Equal(t, int64(n/2), dbCnt)

		// Check the deleted data is deleted
		for i := 0; i < n/2; i++ {
			txGet(t, db, bucket, getTestBytes(i), getTestBytes(i), ErrKeyNotFound)
		}

		// Check the added data is added
		for i := n / 2; i < n; i++ {
			txGet(t, db, bucket, getTestBytes(i), getTestBytes(i), nil)
		}

		require.NoError(t, db.Close())
		removeDir(opts.Dir)
	}
}

func TestDB_MergeForSet(t *testing.T) {
	bucket := "bucket"
	opts := DefaultOptions
	opts.SegmentSize = kb
	opts.Dir = "/tmp/test-set-merge/"

	for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		opts.EntryIdxMode = idxMode
		db, err := Open(opts)
		if exist := db.bm.existBucket(DataStructureSet, bucket); !exist {
			txCreateBucket(t, db, DataStructureSet, bucket, nil)
		}

		require.NoError(t, err)

		// Merge is not needed
		err = db.Merge()
		require.Equal(t, ErrDontNeedMerge, err)

		// Add some data
		n := 1000
		key := getTestBytes(0)
		for i := 0; i < n; i++ {
			txSAdd(t, db, bucket, key, getTestBytes(i), nil, nil)
		}

		// Delete some data
		for i := 0; i < n/2; i++ {
			txSRem(t, db, bucket, key, getTestBytes(i), nil)
		}

		// Pop a random value
		var spopValue []byte
		err = db.Update(func(tx *Tx) error {
			var err error
			spopValue, err = tx.SPop(bucket, key)
			assertErr(t, err, nil)
			return nil
		})
		require.NoError(t, err)

		// Check the random value is popped
		txSIsMember(t, db, bucket, key, spopValue, false)

		// txSPop(t, db, bucket, key,nil)
		dbCnt, err := db.getRecordCount()
		require.NoError(t, err)
		require.Equal(t, int64(n/2-1), dbCnt)

		// Merge and check the result
		require.NoError(t, db.Merge())

		dbCnt, err = db.getRecordCount()
		require.NoError(t, err)
		require.Equal(t, int64(n/2-1), dbCnt)

		// Check the random value is popped
		txSIsMember(t, db, bucket, key, spopValue, false)
		for i := n / 2; i < n; i++ {
			v := getTestBytes(i)
			if bytes.Equal(v, spopValue) {
				continue
			}
			txSIsMember(t, db, bucket, key, v, true)
		}

		// Close and reopen the db
		require.NoError(t, db.Close())

		// reopen db
		db, err = Open(opts)
		require.NoError(t, err)

		dbCnt, err = db.getRecordCount()
		require.NoError(t, err)
		require.Equal(t, int64(n/2-1), dbCnt)

		// Check the random value is popped
		txSIsMember(t, db, bucket, key, spopValue, false)
		for i := n / 2; i < n; i++ {
			v := getTestBytes(i)
			if bytes.Equal(v, spopValue) {
				continue
			}
			txSIsMember(t, db, bucket, key, v, true)
		}
		require.NoError(t, db.Close())
		removeDir(opts.Dir)
	}
}

// TestDB_MergeForZSet is a test function to check the Merge() function of the DB struct
// It creates a DB with two different EntryIdxMode, then adds and scores each item in the DB
// It then removes half of the items from the DB, then checks that the items that are left are the same as the ones that were removed
// It then closes the DB, reopens it, and checks that the items that were removed are now not present
func TestDB_MergeForZSet(t *testing.T) {
	bucket := "bucket"
	key := getTestBytes(0)
	n := 1000
	opts := DefaultOptions
	opts.SegmentSize = kb
	opts.Dir = "/tmp/test-zset-merge/"

	// test different EntryIdxMode
	for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {

		opts.EntryIdxMode = idxMode
		db, err := Open(opts)
		if exist := db.bm.existBucket(DataStructureSortedSet, bucket); !exist {
			txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)
		}
		require.NoError(t, err)

		// add items
		err = db.Merge()
		require.Equal(t, ErrDontNeedMerge, err)

		for i := 0; i < n; i++ {
			score, _ := strconv2.IntToFloat64(i)
			txZAdd(t, db, bucket, key, getTestBytes(i), score, nil, nil)
		}

		for i := 0; i < n; i++ {
			score, _ := strconv2.IntToFloat64(i)
			txZScore(t, db, bucket, key, getTestBytes(i), score, nil)
		}

		// remove half of the items
		for i := 0; i < n/2; i++ {
			txZRem(t, db, bucket, key, getTestBytes(i), nil)
		}

		// check that the items that are left are the same as the ones that were removed
		for i := 0; i < n/2; i++ {
			score, _ := strconv2.IntToFloat64(i)
			txZScore(t, db, bucket, key, getTestBytes(i), score, ErrSortedSetMemberNotExist)
		}

		// check that the items that are left are the same as the ones that were removed
		for i := n / 2; i < n; i++ {
			score, _ := strconv2.IntToFloat64(i)
			txZScore(t, db, bucket, key, getTestBytes(i), score, nil)
		}

		// check that the number of items in the DB is correct
		dbCnt, err := db.getRecordCount()
		require.NoError(t, err)
		require.Equal(t, int64(n/2), dbCnt)

		// merge
		require.NoError(t, db.Merge())

		// check that the number of items in the DB is correct
		dbCnt, err = db.getRecordCount()
		require.NoError(t, err)
		require.Equal(t, int64(n/2), dbCnt)

		// check that the items that were removed are now not present
		for i := 0; i < n/2; i++ {
			score, _ := strconv2.IntToFloat64(i)
			txZScore(t, db, bucket, key, getTestBytes(i), score, ErrSortedSetMemberNotExist)
		}

		// check that the items that are left are the same as the ones that were removed
		for i := n / 2; i < n; i++ {
			score, _ := strconv2.IntToFloat64(i)
			txZScore(t, db, bucket, key, getTestBytes(i), score, nil)
		}

		// close db
		require.NoError(t, db.Close())

		// reopen db
		db, err = Open(opts)
		require.NoError(t, err)
		dbCnt, err = db.getRecordCount()
		require.NoError(t, err)
		require.Equal(t, int64(n/2), dbCnt)

		// check that the items that were removed are now not present
		for i := 0; i < n/2; i++ {
			score, _ := strconv2.IntToFloat64(i)
			txZScore(t, db, bucket, key, getTestBytes(i), score, ErrSortedSetMemberNotExist)
		}

		// check that the items that are left are the same as the ones that were removed
		for i := n / 2; i < n; i++ {
			score, _ := strconv2.IntToFloat64(i)
			txZScore(t, db, bucket, key, getTestBytes(i), score, nil)
		}

		require.NoError(t, db.Close())
		removeDir(opts.Dir)
	}
}

// TestDB_MergeForList tests the Merge() function of the DB struct.
// It creates a DB with two different EntryIdxMode, pushes and pops data, and then merges the DB.
// It then reopens the DB and checks that the data is still there.
func TestDB_MergeForList(t *testing.T) {
	bucket := "bucket"
	key := getTestBytes(0)
	opts := DefaultOptions
	opts.SegmentSize = kb
	opts.Dir = "/tmp/test-list-merge/"

	// test different EntryIdxMode
	for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		opts.EntryIdxMode = idxMode
		db, err := Open(opts)
		if exist := db.bm.existBucket(DataStructureList, bucket); !exist {
			txCreateBucket(t, db, DataStructureList, bucket, nil)
		}

		require.NoError(t, err)

		// check that we don't need merge
		err = db.Merge()
		require.Equal(t, ErrDontNeedMerge, err)

		// push data
		n := 1000
		for i := 0; i < n; i++ {
			txPush(t, db, bucket, key, getTestBytes(i), true, nil, nil)
		}

		for i := n; i < 2*n; i++ {
			txPush(t, db, bucket, key, getTestBytes(i), false, nil, nil)
		}

		// pop data
		for i := n - 1; i >= n/2; i-- {
			txPop(t, db, bucket, key, getTestBytes(i), nil, true)
		}

		for i := 2*n - 1; i >= 3*n/2; i-- {
			txPop(t, db, bucket, key, getTestBytes(i), nil, false)
		}

		// trim and remove data
		txLTrim(t, db, bucket, key, 0, 9, nil)
		txLRem(t, db, bucket, key, 0, getTestBytes(100), nil)
		txLRemByIndex(t, db, bucket, key, nil, []int{7, 8, 9}...)

		dbCnt, err := db.getRecordCount()
		require.NoError(t, err)
		require.Equal(t, int64(7), dbCnt)

		// merge
		require.NoError(t, db.Merge())

		dbCnt, err = db.getRecordCount()
		require.NoError(t, err)
		require.Equal(t, int64(7), dbCnt)

		require.NoError(t, db.Close())

		// reopen db
		db, err = Open(opts)
		require.NoError(t, err)

		dbCnt, err = db.getRecordCount()
		require.NoError(t, err)
		require.Equal(t, int64(7), dbCnt)

		// pop data
		for i := n/2 - 1; i < n/2-8; i-- {
			txPop(t, db, bucket, key, getTestBytes(i), nil, true)
		}

		require.NoError(t, db.Close())
		removeDir(opts.Dir)
	}
}
