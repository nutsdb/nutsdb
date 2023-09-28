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
	"github.com/stretchr/testify/require"
	"github.com/xujiajun/utils/strconv2"
	"sync"
	"testing"
	"time"
)

func TestDB_MergeForString(t *testing.T) {

	// todo: add two index mode
	bucket := "bucket"
	opts := &DefaultOptions
	opts.SegmentSize = KB
	opts.Dir = "/tmp/test-string-merge/"

	defer removeDir(opts.Dir)
	db, err := Open(*opts)
	require.NoError(t, err)

	err = db.Merge()
	require.Equal(t, ErrDontNeedMerge, err)

	n := 1000
	for i := 0; i < n; i++ {
		txPut(t, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
	}

	for i := 0; i < n / 2; i++ {
		txDel(t, db, bucket, GetTestBytes(i), nil)
	}

	require.NoError(t, db.Merge())

	for i := 0; i < n / 2; i++ {
		txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), ErrKeyNotFound)
	}

	for i := n / 2; i < n; i++ {
		txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), nil)
	}

	require.NoError(t, db.Close())
	db, err = Open(*opts)
	require.NoError(t, err)
	dbCnt, err := db.getRecordCount()
	require.NoError(t, err)
	require.Equal(t, int64(n / 2), dbCnt)

	for i := 0; i < n / 2; i++ {
		txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), ErrKeyNotFound)
	}

	for i := n / 2; i < n; i++ {
		txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), nil)
	}
}

func TestDB_MergeRepeated(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 120
	opts.EntryIdxMode = HintKeyAndRAMIdxMode
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "bucket"
		for i := 0; i < 20; i++ {
			txPut(t, db, bucket, []byte("hello"), []byte("world"), Persistent, nil, nil)
		}
		require.Equal(t, int64(9), db.MaxFileID)
		txGet(t, db, bucket, []byte("hello"), []byte("world"), nil)
		require.NoError(t, db.Merge())
		require.Equal(t, int64(10), db.MaxFileID)
		txGet(t, db, bucket, []byte("hello"), []byte("world"), nil)
	})
}

func TestDB_MergeForSet(t *testing.T) {
	bucket := "bucket"
	opts := &DefaultOptions
	opts.SegmentSize = KB
	opts.Dir = "/tmp/test-set-merge/"

	defer removeDir(opts.Dir)
	db, err := Open(*opts)
	require.NoError(t, err)

	err = db.Merge()
	require.Equal(t, ErrDontNeedMerge, err)

	n := 1000
	key := GetTestBytes(0)
	for i := 0; i < n; i++ {
		txSAdd(t, db, bucket, key, GetTestBytes(i), nil, nil)
	}

	for i := 0; i < n / 2; i++ {
		txSRem(t, db, bucket, key, GetTestBytes(i),nil)
	}

	var spopValue []byte
	err = db.Update(func(tx *Tx) error {
		var err error
		spopValue, err = tx.SPop(bucket, key)
		assertErr(t, err, nil)
		return nil
	})
	require.NoError(t, err)

	txSIsMember(t, db, bucket, key, spopValue, false)

	//txSPop(t, db, bucket, key,nil)
	dbCnt, err := db.getRecordCount()
	require.NoError(t, err)
	require.Equal(t, int64(n / 2 - 1), dbCnt)

	require.NoError(t, db.Merge())

	require.NoError(t, db.Close())

	// reopen db
	db, err = Open(*opts)
	require.NoError(t, err)
	dbCnt, err = db.getRecordCount()
	require.NoError(t, err)
	require.Equal(t, int64(n / 2 - 1), dbCnt)

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

func TestDB_MergeForZSet(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 100
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "bucket"
		key := GetTestBytes(0)

		for i := 0; i < 100; i++ {
			score, _ := strconv2.IntToFloat64(i)
			txZAdd(t, db, bucket, key, GetTestBytes(i), score, nil, nil)
		}

		for i := 0; i < 100; i++ {
			score, _ := strconv2.IntToFloat64(i)
			txZScore(t, db, bucket, key, GetTestBytes(i), score, nil)
		}

		for i := 0; i < 50; i++ {
			txZRem(t, db, bucket, key, GetTestBytes(i), nil)
		}

		for i := 0; i < 50; i++ {
			score, _ := strconv2.IntToFloat64(i)
			txZScore(t, db, bucket, key, GetTestBytes(i), score, ErrSortedSetMemberNotExist)
		}

		for i := 50; i < 100; i++ {
			score, _ := strconv2.IntToFloat64(i)
			txZScore(t, db, bucket, key, GetTestBytes(i), score, nil)
		}

		require.NoError(t, db.Merge())

		for i := 50; i < 100; i++ {
			score, _ := strconv2.IntToFloat64(i)
			txZScore(t, db, bucket, key, GetTestBytes(i), score, nil)
		}
	})
}

func TestDB_MergeForList(t *testing.T) {
	bucket := "bucket"
	key := GetTestBytes(0)
	opts := &DefaultOptions
	opts.SegmentSize = KB
	opts.Dir = "/tmp/test-list-merge/"

	defer removeDir(opts.Dir)
	db, err := Open(*opts)
	require.NoError(t, err)

	err = db.Merge()
	require.Equal(t, ErrDontNeedMerge, err)

	n := 1000
	for i := 0; i < n; i++ {
		txPush(t, db, bucket, key, GetTestBytes(i), true, nil, nil)
	}

	for i := n; i < 2 * n; i++ {
		txPush(t, db, bucket, key, GetTestBytes(i), false, nil, nil)
	}

	// Lpop
	for i := n - 1; i >= n / 2; i-- {
		txPop(t, db, bucket, key, GetTestBytes(i), nil, true)
	}

	// Rpop
	for i := 2 * n - 1; i >= 3 * n / 2; i-- {
		txPop(t, db, bucket, key, GetTestBytes(i), nil, false)
	}

	txLTrim(t, db, bucket, key, 0, 9, nil)
	txLRem(t, db, bucket, key, 0, GetTestBytes(100), nil)
	txLRemByIndex(t, db, bucket, key, nil, []int{7, 8, 9}...)
	for i := 1; i < 7; i++ {
		//txLSet(t, db, bucket, key, i, GetTestBytes(i), nil)
	}

	dbCnt, err := db.getRecordCount()
	require.NoError(t, err)
	require.Equal(t, int64(7), dbCnt)

	require.NoError(t, db.Merge())

	dbCnt, err = db.getRecordCount()
	require.NoError(t, err)
	require.Equal(t, int64(7), dbCnt)

	require.NoError(t, db.Close())

	// reopen db
	db, err = Open(*opts)
	require.NoError(t, err)
	dbCnt, err = db.getRecordCount()
	require.NoError(t, err)
	require.Equal(t, int64(7), dbCnt)

	for i := n / 2 - 1; i < n / 2 - 8; i-- {
		fmt.Println(i)
		txPop(t, db, bucket, key, GetTestBytes(i), nil, true)
	}
}

func TestDB_MergeAutomatic(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 1024
	opts.MergeInterval = 200 * time.Millisecond

	bucket := "bucket"

	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		err := db.Merge()
		require.Error(t, err)
		require.Error(t, ErrDontNeedMerge, err)

		key := GetTestBytes(0)
		value := GetRandomBytes(24)

		for i := 0; i < 100; i++ {
			txPut(t, db, bucket, key, value, Persistent, nil, nil)
		}

		txGet(t, db, bucket, key, value, nil)

		// waiting for the merge work to be triggered.
		time.Sleep(200 * time.Millisecond)

		_, pendingMergeFileIds := db.getMaxFileIDAndFileIDs()
		// because there is only one valid entry, there will be only one data file after merging
		require.Len(t, pendingMergeFileIds, 1)

		txGet(t, db, bucket, key, value, nil)
	})
}

func TestDB_MergeWithTx(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 24 * MB
	opts.SyncEnable = false
	opts.EntryIdxMode = HintKeyAndRAMIdxMode
	bucket := "bucket"

	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		_, pendingMergeFileIds := db.getMaxFileIDAndFileIDs()
		require.Len(t, pendingMergeFileIds, 1)

		values := make([][]byte, 10)
		for i := 0; i < 10; i++ {
			values[i] = GetRandomBytes(1024)
		}

		// By repeatedly inserting values, a large amount of garbage data has been generated,
		// which can lead to the creation of multiple data files and trigger the merge process
		for i := 0; i < 10000; i++ {
			for i := 0; i < 10; i++ {
				txPut(t, db, bucket, GetTestBytes(i), values[i], Persistent, nil, nil)
			}
		}

		newValues := make([][]byte, 10)
		for i := 0; i < 10; i++ {
			newValues[i] = GetRandomBytes(1024)
		}

		wg := new(sync.WaitGroup)

		wg.Add(1)
		// this goroutine will run concurrently with the merge goroutine.
		go func() {

			// Waiting for the merge to start.
			time.Sleep(10 * time.Millisecond)

			for i := 0; i < 10; i++ {
				// By selectively updating some values,
				// check if the merge process will overwrite the new values
				if i%2 == 0 {
					txPut(t, db, bucket, GetTestBytes(i), newValues[i], Persistent, nil, nil)
				}
			}

			wg.Done()
		}()

		err := db.Merge()
		require.NoError(t, err)

		wg.Wait()

		// check if the data after the merge is correct
		for i := 0; i < 10; i++ {
			if i%2 == 0 {
				txGet(t, db, bucket, GetTestBytes(i), newValues[i], nil)
			} else {
				txGet(t, db, bucket, GetTestBytes(i), values[i], nil)
			}
		}
	})
}
