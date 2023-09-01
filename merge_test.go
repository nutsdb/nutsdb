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
	"github.com/stretchr/testify/require"
	"github.com/xujiajun/utils/strconv2"
	"sync"
	"testing"
	"time"
)

func TestDB_MergeForString(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 1 * 100
	opts.EntryIdxMode = HintKeyAndRAMIdxMode
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txPut(t, db, bucket, GetTestBytes(0), GetRandomBytes(24), Persistent, nil)
		txPut(t, db, bucket, GetTestBytes(1), GetRandomBytes(24), Persistent, nil)
		txDel(t, db, bucket, GetTestBytes(1), nil)
		txGet(t, db, bucket, GetTestBytes(1), nil, ErrKeyNotFound)
		require.NoError(t, db.Merge())
	})
}

func TestDB_MergeRepeated(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 120
	opts.EntryIdxMode = HintKeyAndRAMIdxMode
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "bucket"
		for i := 0; i < 20; i++ {
			txPut(t, db, bucket, []byte("hello"), []byte("world"), Persistent, nil)
		}
		require.Equal(t, int64(9), db.MaxFileID)
		txGet(t, db, bucket, []byte("hello"), []byte("world"), nil)
		require.NoError(t, db.Merge())
		require.Equal(t, int64(10), db.MaxFileID)
		txGet(t, db, bucket, []byte("hello"), []byte("world"), nil)
	})
}

func TestDB_MergeForSet(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 100
	opts.EntryIdxMode = HintKeyAndRAMIdxMode
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "bucket"
		key := GetTestBytes(0)

		for i := 0; i < 100; i++ {
			txSAdd(t, db, bucket, key, GetTestBytes(i), nil)
		}

		for i := 0; i < 100; i++ {
			txSIsMember(t, db, bucket, key, GetTestBytes(i), true)
		}

		for i := 0; i < 50; i++ {
			txSRem(t, db, bucket, key, GetTestBytes(i), nil)
		}

		for i := 0; i < 50; i++ {
			txSIsMember(t, db, bucket, key, GetTestBytes(i), false)
		}

		for i := 50; i < 100; i++ {
			txSIsMember(t, db, bucket, key, GetTestBytes(i), true)
		}

		require.NoError(t, db.Merge())
	})
}

func TestDB_MergeForZSet(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 100
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "bucket"
		key := GetTestBytes(0)

		for i := 0; i < 100; i++ {
			score, _ := strconv2.IntToFloat64(i)
			txZAdd(t, db, bucket, key, GetTestBytes(i), score, nil)
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
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txPush(t, db, bucket, key, GetTestBytes(0), nil, true)
		require.Error(t, ErrNotSupportMergeWhenUsingList, db.Merge())
	})
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
			txPut(t, db, bucket, key, value, Persistent, nil)
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
				txPut(t, db, bucket, GetTestBytes(i), values[i], Persistent, nil)
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
					txPut(t, db, bucket, GetTestBytes(i), newValues[i], Persistent, nil)
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
