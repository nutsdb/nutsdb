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
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xujiajun/utils/strconv2"
)

type mergeTestMode struct {
	name          string
	enableMergeV2 bool
}

func runForMergeModes(t *testing.T, fn func(t *testing.T, mode mergeTestMode)) {
	modes := []mergeTestMode{{name: "mergeLegacy", enableMergeV2: false}, {name: "mergev2", enableMergeV2: true}}
	for _, mode := range modes {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			fn(t, mode)
		})
	}
}

type mergeFileSets struct {
	userIDs  []int64
	mergeIDs []int64
}

func collectMergeFileSets(t *testing.T, dir string) mergeFileSets {
	userIDs, mergeIDs, err := enumerateDataFileIDs(dir)
	require.NoError(t, err)
	return mergeFileSets{userIDs: userIDs, mergeIDs: mergeIDs}
}

func dataFileIDsForMode(mode mergeTestMode, sets mergeFileSets) []int64 {
	if mode.enableMergeV2 {
		return sets.mergeIDs
	}
	return sets.userIDs
}

func unionDataFileIDs(sets mergeFileSets) []int64 {
	ids := make([]int64, 0, len(sets.userIDs)+len(sets.mergeIDs))
	ids = append(ids, sets.userIDs...)
	ids = append(ids, sets.mergeIDs...)
	return ids
}

func TestDB_MergeForString(t *testing.T) {
	runForMergeModes(t, func(t *testing.T, mode mergeTestMode) {
		bucket := "bucket"
		opts := DefaultOptions
		opts.SegmentSize = KB
		opts.EnableMergeV2 = mode.enableMergeV2
		opts.Dir = fmt.Sprintf("/tmp/test-string-merge-%s/", mode.name)

		// Clean the test directory at the start
		removeDir(opts.Dir)

		for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
			removeDir(opts.Dir)
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
				txPut(t, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
			}

			// Delete some data
			for i := 0; i < n/2; i++ {
				txDel(t, db, bucket, GetTestBytes(i), nil)
			}

			// Merge and check the result
			require.NoError(t, db.Merge())

			dbCnt, err := db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(n/2), dbCnt)

			// Check the deleted data is deleted
			for i := 0; i < n/2; i++ {
				txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), ErrKeyNotFound)
			}

			// Check the added data is added
			for i := n / 2; i < n; i++ {
				txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), nil)
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
				txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), ErrKeyNotFound)
			}

			// Check the added data is added
			for i := n / 2; i < n; i++ {
				txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), nil)
			}

			require.NoError(t, db.Close())
		}
		removeDir(opts.Dir)
	})
}

func TestDB_MergeForSet(t *testing.T) {
	runForMergeModes(t, func(t *testing.T, mode mergeTestMode) {
		bucket := "bucket"
		opts := DefaultOptions
		opts.SegmentSize = KB
		opts.EnableMergeV2 = mode.enableMergeV2
		opts.Dir = fmt.Sprintf("/tmp/test-set-merge-%s/", mode.name)

		for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
			removeDir(opts.Dir)
			opts.EntryIdxMode = idxMode
			db, err := Open(opts)
			if exist := db.bm.ExistBucket(DataStructureSet, bucket); !exist {
				txCreateBucket(t, db, DataStructureSet, bucket, nil)
			}

			require.NoError(t, err)

			// Merge is not needed
			err = db.Merge()
			require.Equal(t, ErrDontNeedMerge, err)

			// Add some data
			n := 1000
			key := GetTestBytes(0)
			for i := 0; i < n; i++ {
				txSAdd(t, db, bucket, key, GetTestBytes(i), nil, nil)
			}

			// Delete some data
			for i := 0; i < n/2; i++ {
				txSRem(t, db, bucket, key, GetTestBytes(i), nil)
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
				v := GetTestBytes(i)
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
				v := GetTestBytes(i)
				if bytes.Equal(v, spopValue) {
					continue
				}
				txSIsMember(t, db, bucket, key, v, true)
			}
			require.NoError(t, db.Close())
		}
		removeDir(opts.Dir)
	})
}

// TestDB_MergeForZSet is a test function to check the Merge() function of the DB struct
// It creates a DB with two different EntryIdxMode, then adds and scores each item in the DB
// It then removes half of the items from the DB, then checks that the items that are left are the same as the ones that were removed
// It then closes the DB, reopens it, and checks that the items that were removed are now not present
func TestDB_MergeForZSet(t *testing.T) {
	runForMergeModes(t, func(t *testing.T, mode mergeTestMode) {
		bucket := "bucket"
		key := GetTestBytes(0)
		n := 1000
		opts := DefaultOptions
		opts.SegmentSize = KB
		opts.EnableMergeV2 = mode.enableMergeV2
		opts.Dir = fmt.Sprintf("/tmp/test-zset-merge-%s/", mode.name)

		// test different EntryIdxMode
		for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
			removeDir(opts.Dir)
			opts.EntryIdxMode = idxMode
			db, err := Open(opts)
			if exist := db.bm.ExistBucket(DataStructureSortedSet, bucket); !exist {
				txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)
			}
			require.NoError(t, err)

			// add items
			err = db.Merge()
			require.Equal(t, ErrDontNeedMerge, err)

			for i := 0; i < n; i++ {
				score, _ := strconv2.IntToFloat64(i)
				txZAdd(t, db, bucket, key, GetTestBytes(i), score, nil, nil)
			}

			for i := 0; i < n; i++ {
				score, _ := strconv2.IntToFloat64(i)
				txZScore(t, db, bucket, key, GetTestBytes(i), score, nil)
			}

			// remove half of the items
			for i := 0; i < n/2; i++ {
				txZRem(t, db, bucket, key, GetTestBytes(i), nil)
			}

			// check that the items that are left are the same as the ones that were removed
			for i := 0; i < n/2; i++ {
				score, _ := strconv2.IntToFloat64(i)
				txZScore(t, db, bucket, key, GetTestBytes(i), score, ErrSortedSetMemberNotExist)
			}

			// check that the items that are left are the same as the ones that were removed
			for i := n / 2; i < n; i++ {
				score, _ := strconv2.IntToFloat64(i)
				txZScore(t, db, bucket, key, GetTestBytes(i), score, nil)
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
				txZScore(t, db, bucket, key, GetTestBytes(i), score, ErrSortedSetMemberNotExist)
			}

			// check that the items that are left are the same as the ones that were removed
			for i := n / 2; i < n; i++ {
				score, _ := strconv2.IntToFloat64(i)
				txZScore(t, db, bucket, key, GetTestBytes(i), score, nil)
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
				txZScore(t, db, bucket, key, GetTestBytes(i), score, ErrSortedSetMemberNotExist)
			}

			// check that the items that are left are the same as the ones that were removed
			for i := n / 2; i < n; i++ {
				score, _ := strconv2.IntToFloat64(i)
				txZScore(t, db, bucket, key, GetTestBytes(i), score, nil)
			}

			require.NoError(t, db.Close())
		}
		removeDir(opts.Dir)
	})
}

// TestDB_MergeForList tests the Merge() function of the DB struct.
// It creates a DB with two different EntryIdxMode, pushes and pops data, and then merges the DB.
// It then reopens the DB and checks that the data is still there.
func TestDB_MergeForList(t *testing.T) {
	runForMergeModes(t, func(t *testing.T, mode mergeTestMode) {
		bucket := "bucket"
		key := GetTestBytes(0)
		opts := DefaultOptions
		opts.SegmentSize = KB
		opts.EnableMergeV2 = mode.enableMergeV2
		opts.Dir = fmt.Sprintf("/tmp/test-list-merge-%s/", mode.name)

		// test different EntryIdxMode
		for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
			removeDir(opts.Dir)
			opts.EntryIdxMode = idxMode
			db, err := Open(opts)
			if exist := db.bm.ExistBucket(DataStructureList, bucket); !exist {
				txCreateBucket(t, db, DataStructureList, bucket, nil)
			}

			require.NoError(t, err)

			// check that we don't need merge
			err = db.Merge()
			require.Equal(t, ErrDontNeedMerge, err)

			// push data
			n := 1000
			for i := 0; i < n; i++ {
				txPush(t, db, bucket, key, GetTestBytes(i), true, nil, nil)
			}

			for i := n; i < 2*n; i++ {
				txPush(t, db, bucket, key, GetTestBytes(i), false, nil, nil)
			}

			// pop data
			for i := n - 1; i >= n/2; i-- {
				txPop(t, db, bucket, key, GetTestBytes(i), nil, true)
			}

			for i := 2*n - 1; i >= 3*n/2; i-- {
				txPop(t, db, bucket, key, GetTestBytes(i), nil, false)
			}

			// trim and remove data
			txLTrim(t, db, bucket, key, 0, 9, nil)
			txLRem(t, db, bucket, key, 0, GetTestBytes(100), nil)
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
				txPop(t, db, bucket, key, GetTestBytes(i), nil, true)
			}

			require.NoError(t, db.Close())
		}
		removeDir(opts.Dir)
	})
}

func TestDB_MergeWithHintFile(t *testing.T) {
	runForMergeModes(t, func(t *testing.T, mode mergeTestMode) {
		bucket := "bucket"
		opts := DefaultOptions
		opts.SegmentSize = KB
		opts.Dir = fmt.Sprintf("/tmp/test-merge-hintfile-%s/", mode.name)
		opts.EnableHintFile = true
		opts.EnableMergeV2 = mode.enableMergeV2

		for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
			removeDir(opts.Dir)
			opts.EntryIdxMode = idxMode

			// First, create a database with some data
			db, err := Open(opts)
			require.NoError(t, err)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			// Add some data to create multiple data files
			n := 2000
			for i := 0; i < n; i++ {
				txPut(t, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
			}

			// Delete some data to create dirty entries
			for i := 0; i < n/2; i++ {
				txDel(t, db, bucket, GetTestBytes(i), nil)
			}

			// Close and reopen to ensure data is persisted
			require.NoError(t, db.Close())
			db, err = Open(opts)
			require.NoError(t, err)

			// Perform merge
			require.NoError(t, db.Merge())

			// Collect data file sets and select the relevant IDs for this mode
			sets := collectMergeFileSets(t, opts.Dir)
			idsToCheck := dataFileIDsForMode(mode, sets)
			require.Greater(t, len(idsToCheck), 0)

			// Count total entries across all merged files' hint files
			totalHintEntryCount := 0
			for _, fid := range idsToCheck {
				hintPath := getHintPath(fid, opts.Dir)

				// Verify hint file exists
				_, err = os.Stat(hintPath)
				require.NoError(t, err, "Hint file should exist after merge for file %d", fid)

				// Verify hint file content
				reader := &HintFileReader{}
				err = reader.Open(hintPath)
				require.NoError(t, err)

				// Count entries in this hint file
				hintEntryCount := 0
				for {
					_, err := reader.Read()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					hintEntryCount++
				}
				reader.Close()

				totalHintEntryCount += hintEntryCount
			}

			// Should have n/2 entries (the non-deleted ones) across all hint files
			require.Equal(t, n/2, totalHintEntryCount)

			// Verify data consistency after merge
			dbCnt, err := db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(n/2), dbCnt)

			// Check the deleted data is deleted
			for i := 0; i < n/2; i++ {
				txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), ErrKeyNotFound)
			}

			// Check the remaining data exists
			for i := n / 2; i < n; i++ {
				txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), nil)
			}

			// Close and reopen to test hint file loading
			require.NoError(t, db.Close())
			db, err = Open(opts)
			require.NoError(t, err)

			// Verify data is still correct after reopening with hint file
			dbCnt, err = db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(n/2), dbCnt)

			// Check the deleted data is still deleted
			for i := 0; i < n/2; i++ {
				txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), ErrKeyNotFound)
			}

			// Check the remaining data still exists
			for i := n / 2; i < n; i++ {
				txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), nil)
			}

			require.NoError(t, db.Close())
		}
		removeDir(opts.Dir)
	})
}

func TestDB_MergeHintFileCleanup(t *testing.T) {
	runForMergeModes(t, func(t *testing.T, mode mergeTestMode) {
		bucket := "bucket"
		opts := DefaultOptions
		opts.SegmentSize = KB
		opts.Dir = fmt.Sprintf("/tmp/test-merge-hintfile-cleanup-%s/", mode.name)
		opts.EnableHintFile = true
		opts.EnableMergeV2 = mode.enableMergeV2

		removeDir(opts.Dir)

		// Create a database with some data
		db, err := Open(opts)
		require.NoError(t, err)
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		// Add enough data to create multiple files
		n := 500
		for i := 0; i < n; i++ {
			txPut(t, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
		}

		// Delete some data to trigger merge
		for i := 0; i < n/4; i++ {
			txDel(t, db, bucket, GetTestBytes(i), nil)
		}

		// Get initial file IDs before merge (legacy files only)
		_, initialFileIDs := db.getMaxFileIDAndFileIDs()

		// Create hint files for initial files manually to test cleanup
		for _, fileID := range initialFileIDs {
			hintPath := getHintPath(fileID, opts.Dir)
			writer := &HintFileWriter{}
			err := writer.Create(hintPath)
			require.NoError(t, err)

			// Write a dummy entry
			entry := &HintEntry{
				BucketId:  1,
				KeySize:   3,
				ValueSize: 3,
				Timestamp: 1234567890,
				TTL:       3600,
				Flag:      DataSetFlag,
				Status:    Committed,
				Ds:        DataStructureBTree,
				DataPos:   100,
				FileID:    fileID,
				Key:       []byte("key"),
			}

			err = writer.Write(entry)
			require.NoError(t, err)
			err = writer.Close()
			require.NoError(t, err)
		}

		// Verify hint files exist before merge
		for _, fileID := range initialFileIDs {
			hintPath := getHintPath(fileID, opts.Dir)
			_, err := os.Stat(hintPath)
			require.NoError(t, err, "Hint file should exist before merge")
		}

		// Perform merge
		require.NoError(t, db.Merge())

		// Verify old hint files are cleaned up
		for _, fileID := range initialFileIDs {
			hintPath := getHintPath(fileID, opts.Dir)
			_, err := os.Stat(hintPath)
			if err == nil {
				t.Errorf("Old hint file %s should be cleaned up after merge", hintPath)
			}
		}

		// Verify new hint files exist for all merged files (legacy or merge v2)
		sets := collectMergeFileSets(t, opts.Dir)
		idsToCheck := dataFileIDsForMode(mode, sets)
		require.Greater(t, len(idsToCheck), 0)

		for _, fid := range idsToCheck {
			newHintPath := getHintPath(fid, opts.Dir)
			_, err = os.Stat(newHintPath)
			require.NoError(t, err, "New hint file should exist after merge for file %d", fid)
		}

		require.NoError(t, db.Close())
		removeDir(opts.Dir)
	})
}

func TestDB_MergeHintFileDisabled(t *testing.T) {
	runForMergeModes(t, func(t *testing.T, mode mergeTestMode) {
		bucket := "bucket"
		opts := DefaultOptions
		opts.SegmentSize = KB
		opts.Dir = fmt.Sprintf("/tmp/test-merge-hintfile-disabled-%s/", mode.name)
		opts.EnableHintFile = false // Disable hint file
		opts.EnableMergeV2 = mode.enableMergeV2

		removeDir(opts.Dir)

		// Create a database with some data
		db, err := Open(opts)
		require.NoError(t, err)
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		// Add some data
		n := 500
		for i := 0; i < n; i++ {
			txPut(t, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
		}

		// Delete some data to trigger merge
		for i := 0; i < n/4; i++ {
			txDel(t, db, bucket, GetTestBytes(i), nil)
		}

		// Perform merge
		require.NoError(t, db.Merge())

		// Verify no hint files are created
		sets := collectMergeFileSets(t, opts.Dir)
		idsToCheck := unionDataFileIDs(sets)
		for _, fid := range idsToCheck {
			hintPath := getHintPath(fid, opts.Dir)
			_, err := os.Stat(hintPath)
			if err == nil {
				t.Errorf("Hint file %s should not exist when EnableHintFile is false", hintPath)
			}
		}

		// Verify data is still correct after merge without hint files
		dbCnt, err := db.getRecordCount()
		require.NoError(t, err)
		require.Equal(t, int64(3*n/4), dbCnt)

		require.NoError(t, db.Close())
		removeDir(opts.Dir)
	})
}

func TestDB_MergeHintFileDifferentDataStructures(t *testing.T) {
	runForMergeModes(t, func(t *testing.T, mode mergeTestMode) {
		opts := DefaultOptions
		opts.SegmentSize = KB
		opts.Dir = fmt.Sprintf("/tmp/test-merge-hintfile-ds-%s/", mode.name)
		opts.EnableHintFile = true
		opts.EnableMergeV2 = mode.enableMergeV2

		removeDir(opts.Dir)

		db, err := Open(opts)
		require.NoError(t, err)

		// Test BTree
		bucketBTree := "bucket_btree"
		txCreateBucket(t, db, DataStructureBTree, bucketBTree, nil)
		for i := 0; i < 100; i++ {
			txPut(t, db, bucketBTree, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
		}

		// Test Set
		bucketSet := "bucket_set"
		txCreateBucket(t, db, DataStructureSet, bucketSet, nil)
		key := GetTestBytes(0)
		for i := 0; i < 50; i++ {
			txSAdd(t, db, bucketSet, key, GetTestBytes(i), nil, nil)
		}

		// Test List
		bucketList := "bucket_list"
		txCreateBucket(t, db, DataStructureList, bucketList, nil)
		listKey := GetTestBytes(0)
		for i := 0; i < 30; i++ {
			txPush(t, db, bucketList, listKey, GetTestBytes(i), true, nil, nil)
		}

		// Test SortedSet
		bucketZSet := "bucket_zset"
		txCreateBucket(t, db, DataStructureSortedSet, bucketZSet, nil)
		zsetKey := GetTestBytes(0)
		for i := 0; i < 20; i++ {
			txZAdd(t, db, bucketZSet, zsetKey, GetTestBytes(i), float64(i), nil, nil)
		}

		// Delete some data from each structure
		for i := 0; i < 25; i++ {
			txDel(t, db, bucketBTree, GetTestBytes(i), nil)
		}
		for i := 0; i < 10; i++ {
			txSRem(t, db, bucketSet, key, GetTestBytes(i), nil)
		}
		for i := 0; i < 5; i++ {
			txPop(t, db, bucketList, listKey, GetTestBytes(i), nil, false)
		}
		for i := 0; i < 5; i++ {
			txZRem(t, db, bucketZSet, zsetKey, GetTestBytes(i), nil)
		}

		// Perform merge
		require.NoError(t, db.Merge())

		// Verify hint files exist and contain entries for all data structures
		sets := collectMergeFileSets(t, opts.Dir)
		idsToCheck := dataFileIDsForMode(mode, sets)
		require.Greater(t, len(idsToCheck), 0)

		// Count entries by data structure across all hint files
		btreeCount := 0
		setCount := 0
		listCount := 0
		zsetCount := 0

		// Check that hint files exist for all merged files and count entries
		for _, fid := range idsToCheck {
			hintPath := getHintPath(fid, opts.Dir)
			_, err = os.Stat(hintPath)
			require.NoError(t, err, "Hint file should exist after merge for file %d", fid)

			// Verify hint file content
			reader := &HintFileReader{}
			err = reader.Open(hintPath)
			require.NoError(t, err)

			for {
				entry, err := reader.Read()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)

				switch entry.Ds {
				case DataStructureBTree:
					btreeCount++
				case DataStructureSet:
					setCount++
				case DataStructureList:
					listCount++
				case DataStructureSortedSet:
					zsetCount++
				}
			}
			reader.Close()
		}

		// Verify counts match expected remaining entries
		require.Equal(t, 75, btreeCount) // 100 - 25 deleted
		require.Equal(t, 40, setCount)   // 50 - 10 deleted
		require.Equal(t, 25, listCount)  // 30 - 5 deleted
		require.Equal(t, 15, zsetCount)  // 20 - 5 deleted

		// Verify data integrity after merge and reopen
		require.NoError(t, db.Close())
		db, err = Open(opts)
		require.NoError(t, err)

		// Check BTree data
		for i := 25; i < 100; i++ {
			txGet(t, db, bucketBTree, GetTestBytes(i), GetTestBytes(i), nil)
		}
		for i := 0; i < 25; i++ {
			txGet(t, db, bucketBTree, GetTestBytes(i), GetTestBytes(i), ErrKeyNotFound)
		}

		// Check Set data
		for i := 10; i < 50; i++ {
			txSIsMember(t, db, bucketSet, key, GetTestBytes(i), true)
		}
		for i := 0; i < 10; i++ {
			txSIsMember(t, db, bucketSet, key, GetTestBytes(i), false)
		}

		// Check List data
		for i := 29; i < 24; i++ {
			txLRange(t, db, bucketList, listKey, i-5, i-5, 1, [][]byte{GetTestBytes(i)}, nil)
		}

		// Check SortedSet data
		for i := 5; i < 20; i++ {
			txZScore(t, db, bucketZSet, zsetKey, GetTestBytes(i), float64(i), nil)
		}
		for i := 0; i < 5; i++ {
			txZScore(t, db, bucketZSet, zsetKey, GetTestBytes(i), 0, ErrSortedSetMemberNotExist)
		}

		require.NoError(t, db.Close())
		removeDir(opts.Dir)
	})
}
