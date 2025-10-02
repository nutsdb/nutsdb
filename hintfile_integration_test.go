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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestHintFileIntegration_WriteMergeRestartVerify tests the complete workflow:
// Write data -> Merge -> Restart -> Verify data integrity
func TestHintFileIntegration_WriteMergeRestartVerify(t *testing.T) {
	bucket := "bucket"
	opts := DefaultOptions
	opts.SegmentSize = 64 * KB
	opts.Dir = "/tmp/test-hintfile-integration/"
	opts.EnableHintFile = true

	// Clean the test directory at the start
	removeDir(opts.Dir)

	for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		opts.EntryIdxMode = idxMode

		// Step 1: Create database and write data
		db, err := Open(opts)
		require.NoError(t, err)
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		// Write a significant amount of data to trigger merge
		n := 2000
		for i := 0; i < n; i++ {
			txPut(t, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
		}

		// Delete some data to create dirty entries
		for i := 0; i < n/4; i++ {
			txDel(t, db, bucket, GetTestBytes(i), nil)
		}

		// Verify data before merge
		for i := n / 4; i < n; i++ {
			txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), nil)
		}
		for i := 0; i < n/4; i++ {
			txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), ErrKeyNotFound)
		}

		// Step 2: Perform merge to create hint files
		require.NoError(t, db.Merge())

		// Verify hint files are created for all merged files
		_, mergedFileIDs := db.getMaxFileIDAndFileIDs()
		require.Greater(t, len(mergedFileIDs), 0)

		// Check that hint files exist for all merged files
		for _, fileID := range mergedFileIDs {
			hintPath := getHintPath(int64(fileID), opts.Dir)
			_, err = os.Stat(hintPath)
			require.NoError(t, err, "Hint file should exist after merge for file %d", fileID)
		}

		// Step 3: Close database
		require.NoError(t, db.Close())

		// Step 4: Restart database (should use hint files for fast recovery)
		db, err = Open(opts)
		require.NoError(t, err)

		// Step 5: Verify all data is correctly recovered
		for i := n / 4; i < n; i++ {
			txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), nil)
		}
		for i := 0; i < n/4; i++ {
			txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), ErrKeyNotFound)
		}

		// Verify record count
		dbCnt, err := db.getRecordCount()
		require.NoError(t, err)
		require.Equal(t, int64(3*n/4), dbCnt)

		require.NoError(t, db.Close())
		removeDir(opts.Dir)
	}
}

// TestHintFileIntegration_EnableDisableToggle tests enabling and disabling hint files
func TestHintFileIntegration_EnableDisableToggle(t *testing.T) {
	bucket := "bucket"
	opts := DefaultOptions
	opts.SegmentSize = 64 * KB
	opts.Dir = "/tmp/test-hintfile-toggle"

	// Clean the test directory at the start
	removeDir(opts.Dir)

	// Test with hint files enabled
	opts.EnableHintFile = true

	db, err := Open(opts)
	require.NoError(t, err)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	// Write enough data to generate at least 2 data files (for merge to work)
	// Each entry is ~82 bytes, so 2000 entries = ~160KB, which will create 3 files with 64KB segments
	n := 2000
	for i := 0; i < n; i++ {
		txPut(t, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
	}

	// Perform merge
	require.NoError(t, db.Merge())

	// Verify hint files are created for all merged files
	_, mergedFileIDs := db.getMaxFileIDAndFileIDs()
	require.Greater(t, len(mergedFileIDs), 0)

	// Check that hint files exist for all merged files
	for _, fileID := range mergedFileIDs {
		hintPath := getHintPath(int64(fileID), opts.Dir)
		_, err = os.Stat(hintPath)
		require.NoError(t, err, "Hint file should exist when EnableHintFile is true for file %d", fileID)
	}

	require.NoError(t, db.Close())

	// Test with hint files disabled
	opts.EnableHintFile = false

	db, err = Open(opts)
	require.NoError(t, err)

	// Write more data to generate additional files for the second merge
	// Write 1500 more entries to ensure we have at least 2 files to merge
	for i := n; i < n+1500; i++ {
		txPut(t, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
	}

	// Perform merge (should not create hint files)
	require.NoError(t, db.Merge())

	// Verify new hint files are not created for any merged files
	_, newMergedFileIDs := db.getMaxFileIDAndFileIDs()
	require.Greater(t, len(newMergedFileIDs), 0)

	// Check that hint files do not exist for any merged files
	for _, fileID := range newMergedFileIDs {
		newHintPath := getHintPath(int64(fileID), opts.Dir)
		_, err = os.Stat(newHintPath)
		if err == nil {
			t.Errorf("New hint file %s should not exist when EnableHintFile is false for file %d", newHintPath, fileID)
		}
	}

	// Verify old hint file should not exist too
	for _, fileID := range mergedFileIDs {
		oldHintPath := getHintPath(int64(fileID), opts.Dir)
		_, err = os.Stat(oldHintPath)
		require.Error(t, err, "Old hint file %s should not exist too", oldHintPath)
	}

	require.NoError(t, db.Close())
	removeDir(opts.Dir)
}

// TestHintFileIntegration_MultipleMerges tests multiple merge operations
func TestHintFileIntegration_MultipleMerges(t *testing.T) {
	bucket := "bucket"
	opts := DefaultOptions
	opts.SegmentSize = 32 * KB // Smaller segment size to trigger more merges
	opts.Dir = "/tmp/test-hintfile-multi-merge/"
	opts.EnableHintFile = true

	// Clean the test directory at the start
	removeDir(opts.Dir)

	db, err := Open(opts)
	require.NoError(t, err)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	// Perform multiple rounds of write->delete->merge
	for round := 0; round < 3; round++ {
		// Write data
		start := round * 500
		end := (round + 1) * 500
		for i := start; i < end; i++ {
			txPut(t, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
		}

		// Delete some data
		for i := start; i < start+100; i++ {
			txDel(t, db, bucket, GetTestBytes(i), nil)
		}

		// Perform merge
		require.NoError(t, db.Merge())

		// Verify hint files are created for all merged files
		_, mergedFileIDs := db.getMaxFileIDAndFileIDs()
		require.Greater(t, len(mergedFileIDs), 0)

		// Check that hint files exist for all merged files
		for _, fileID := range mergedFileIDs {
			hintPath := getHintPath(int64(fileID), opts.Dir)
			_, err = os.Stat(hintPath)
			require.NoError(t, err, "Hint file should exist after merge for file %d", fileID)
		}
	}

	// Verify all data is correct
	for round := 0; round < 3; round++ {
		start := round * 500
		end := (round + 1) * 500

		// Check deleted data
		for i := start; i < start+100; i++ {
			txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), ErrKeyNotFound)
		}

		// Check remaining data
		for i := start + 100; i < end; i++ {
			txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), nil)
		}
	}

	require.NoError(t, db.Close())

	// Restart and verify again
	db, err = Open(opts)
	require.NoError(t, err)

	for round := 0; round < 3; round++ {
		start := round * 500
		end := (round + 1) * 500

		// Check deleted data
		for i := start; i < start+100; i++ {
			txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), ErrKeyNotFound)
		}

		// Check remaining data
		for i := start + 100; i < end; i++ {
			txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), nil)
		}
	}

	require.NoError(t, db.Close())
	removeDir(opts.Dir)
}

// TestHintFileIntegration_MixedDataStructures tests hint files with mixed data structures
func TestHintFileIntegration_MixedDataStructures(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 64 * KB
	opts.Dir = "/tmp/test-hintfile-mixed-ds/"
	opts.EnableHintFile = true

	// Clean the test directory at the start
	removeDir(opts.Dir)

	db, err := Open(opts)
	require.NoError(t, err)

	// Create buckets for different data structures
	bucketBTree := "bucket_btree"
	bucketSet := "bucket_set"
	bucketList := "bucket_list"
	bucketZSet := "bucket_zset"

	txCreateBucket(t, db, DataStructureBTree, bucketBTree, nil)
	txCreateBucket(t, db, DataStructureSet, bucketSet, nil)
	txCreateBucket(t, db, DataStructureList, bucketList, nil)
	txCreateBucket(t, db, DataStructureSortedSet, bucketZSet, nil)

	// Add data to each structure - increase data size to ensure 2 files are created
	for i := 0; i < 2000; i++ {
		txPut(t, db, bucketBTree, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
	}

	setKey := GetTestBytes(0)
	for i := 0; i < 1000; i++ {
		txSAdd(t, db, bucketSet, setKey, GetTestBytes(i), nil, nil)
	}

	listKey := GetTestBytes(0)
	for i := 0; i < 500; i++ {
		txPush(t, db, bucketList, listKey, GetTestBytes(i), true, nil, nil)
	}

	zsetKey := GetTestBytes(0)
	for i := 0; i < 300; i++ {
		txZAdd(t, db, bucketZSet, zsetKey, GetTestBytes(i), float64(i), nil, nil)
	}

	// Delete some data from each structure
	for i := 0; i < 500; i++ {
		txDel(t, db, bucketBTree, GetTestBytes(i), nil)
	}
	for i := 0; i < 250; i++ {
		txSRem(t, db, bucketSet, setKey, GetTestBytes(i), nil)
	}
	for i := 0; i < 100; i++ {
		txPop(t, db, bucketList, listKey, GetTestBytes(i), nil, false)
	}
	for i := 0; i < 50; i++ {
		txZRem(t, db, bucketZSet, zsetKey, GetTestBytes(i), nil)
	}

	// Perform merge
	require.NoError(t, db.Merge())

	// Verify hint files are created for all merged files
	_, mergedFileIDs := db.getMaxFileIDAndFileIDs()
	require.GreaterOrEqual(t, len(mergedFileIDs), 2, "Should have at least 2 files")

	// Check that hint files exist for all merged files
	for _, fileID := range mergedFileIDs {
		hintPath := getHintPath(int64(fileID), opts.Dir)
		_, err = os.Stat(hintPath)
		require.NoError(t, err, "Hint file should exist after merge for file %d", fileID)
	}

	require.NoError(t, db.Close())

	// Restart and verify all data structures
	db, err = Open(opts)
	require.NoError(t, err)

	// Verify BTree data
	for i := 500; i < 2000; i++ {
		txGet(t, db, bucketBTree, GetTestBytes(i), GetTestBytes(i), nil)
	}
	for i := 0; i < 500; i++ {
		txGet(t, db, bucketBTree, GetTestBytes(i), GetTestBytes(i), ErrKeyNotFound)
	}

	// Verify Set data
	for i := 250; i < 1000; i++ {
		txSIsMember(t, db, bucketSet, setKey, GetTestBytes(i), true)
	}
	for i := 0; i < 250; i++ {
		txSIsMember(t, db, bucketSet, setKey, GetTestBytes(i), false)
	}

	// Verify List data
	txLRange(t, db, bucketList, listKey, 0, -1, 400, nil, nil)

	// Verify SortedSet data
	for i := 50; i < 300; i++ {
		txZScore(t, db, bucketZSet, zsetKey, GetTestBytes(i), float64(i), nil)
	}
	for i := 0; i < 50; i++ {
		txZScore(t, db, bucketZSet, zsetKey, GetTestBytes(i), 0, ErrSortedSetMemberNotExist)
	}

	require.NoError(t, db.Close())
	removeDir(opts.Dir)
}

// TestHintFileIntegration_CrashRecovery tests database recovery after simulated crashes
func TestHintFileIntegration_CrashRecovery(t *testing.T) {
	bucket := "bucket"
	opts := DefaultOptions
	opts.SegmentSize = 64 * KB
	opts.Dir = "/tmp/test-hintfile-crash/"
	opts.EnableHintFile = true

	// Clean the test directory at the start
	removeDir(opts.Dir)

	// Create database and write data
	db, err := Open(opts)
	require.NoError(t, err)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	n := 2000
	for i := 0; i < n; i++ {
		txPut(t, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
	}

	// Perform merge to create hint files
	require.NoError(t, db.Merge())

	// Simulate crash by not properly closing the database
	// (just release resources without calling Close())
	db.ActiveFile.rwManager.Release()
	db.fm.close()
	db.flock.Unlock()

	// Restart database (should handle incomplete shutdown gracefully)
	db, err = Open(opts)
	require.NoError(t, err)

	// Verify all data is correctly recovered
	for i := 0; i < n; i++ {
		txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), nil)
	}

	// Verify record count
	dbCnt, err := db.getRecordCount()
	require.NoError(t, err)
	require.Equal(t, int64(n), dbCnt)

	require.NoError(t, db.Close())
	removeDir(opts.Dir)
}

// TestHintFileIntegration_ConcurrentOperations tests hint files with concurrent operations
func TestHintFileIntegration_ConcurrentOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	opts := DefaultOptions
	opts.SegmentSize = 64 * KB
	opts.Dir = "/tmp/test-hintfile-concurrent/"
	opts.EnableHintFile = true

	// Clean the test directory at the start
	removeDir(opts.Dir)

	db, err := Open(opts)
	require.NoError(t, err)

	bucket := "bucket"
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	// Perform concurrent writes
	done := make(chan bool, 20)
	for i := 0; i < 20; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := GetTestBytes(id*100 + j)
				value := GetTestBytes(id*100 + j)
				txPut(t, db, bucket, key, value, Persistent, nil, nil)
			}
			done <- true
		}(i)
	}

	// Wait for all writes to complete
	for i := 0; i < 20; i++ {
		<-done
	}

	// Perform merge
	require.NoError(t, db.Merge())

	// Verify hint files are created for all merged files
	_, mergedFileIDs := db.getMaxFileIDAndFileIDs()
	require.Greater(t, len(mergedFileIDs), 0)

	// Check that hint files exist for all merged files
	for _, fileID := range mergedFileIDs {
		hintPath := getHintPath(int64(fileID), opts.Dir)
		_, err = os.Stat(hintPath)
		require.NoError(t, err, "Hint file should exist after merge for file %d", fileID)
	}

	require.NoError(t, db.Close())

	// Restart and verify all data
	db, err = Open(opts)
	require.NoError(t, err)

	// Verify all data is correctly recovered
	for i := 0; i < 10; i++ {
		for j := 0; j < 100; j++ {
			key := GetTestBytes(i*100 + j)
			value := GetTestBytes(i*100 + j)
			txGet(t, db, bucket, key, value, nil)
		}
	}

	require.NoError(t, db.Close())
	removeDir(opts.Dir)
}

// TestHintFileIntegration_LargeDataset tests hint files with large datasets
func TestHintFileIntegration_LargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	opts := DefaultOptions
	opts.SegmentSize = 256 * KB
	opts.Dir = "/tmp/test-hintfile-large/"
	opts.EnableHintFile = true

	// Clean the test directory at the start
	removeDir(opts.Dir)

	db, err := Open(opts)
	require.NoError(t, err)

	bucket := "bucket"
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	// Write a large amount of data
	n := 10000
	for i := 0; i < n; i++ {
		txPut(t, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
	}

	// Delete some data
	for i := 0; i < n/4; i++ {
		txDel(t, db, bucket, GetTestBytes(i), nil)
	}

	// Perform merge
	start := time.Now()
	require.NoError(t, db.Merge())
	mergeTime := time.Since(start)
	t.Logf("Merge completed in %v", mergeTime)

	// Verify hint files are created
	_, mergedFileIDs := db.getMaxFileIDAndFileIDs()
	require.Greater(t, len(mergedFileIDs), 0)

	maxFileID := mergedFileIDs[len(mergedFileIDs)-1]
	hintPath := getHintPath(int64(maxFileID), opts.Dir)
	_, err = os.Stat(hintPath)
	require.NoError(t, err, "Hint file should exist after merge")

	require.NoError(t, db.Close())

	// Restart and verify recovery
	start = time.Now()
	db, err = Open(opts)
	require.NoError(t, err)
	recoveryTime := time.Since(start)
	t.Logf("Recovery completed in %v", recoveryTime)

	// Verify sample data
	for i := n / 4; i < n/4+100; i++ {
		txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), nil)
	}
	for i := 0; i < 100; i++ {
		txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), ErrKeyNotFound)
	}

	// Verify record count
	dbCnt, err := db.getRecordCount()
	require.NoError(t, err)
	require.Equal(t, int64(3*n/4), dbCnt)

	require.NoError(t, db.Close())
	removeDir(opts.Dir)
}
