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
	"sync"
	"testing"
	"time"
)

func TestDB_MergeAutomatic(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 1024
	opts.MergeInterval = 2 * time.Second

	bucket := "bucket"

	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		_, pendingMergeFileIds := db.getMaxFileIDAndFileIDs()
		require.Len(t, pendingMergeFileIds, 1)

		key := GetTestBytes(0)
		value := GetRandomBytes(24)

		for i := 0; i < 100; i++ {
			txPut(t, db, bucket, key, value, Persistent, nil)
		}

		txGet(t, db, bucket, key, value, nil)

		_, pendingMergeFileIds = db.getMaxFileIDAndFileIDs()
		// this means that the merge can now be performed
		require.Len(t, pendingMergeFileIds, 10)

		// waiting for the merge work to be triggered.
		time.Sleep(2 * time.Second)

		_, pendingMergeFileIds = db.getMaxFileIDAndFileIDs()
		// because there is only one valid entry, there will be only one data file after merging
		require.Len(t, pendingMergeFileIds, 1)

		txGet(t, db, bucket, key, value, nil)
	})
}

func TestDB_MergeWithTx(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 24 * MB
	opts.SyncEnable = false

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

		// this goroutine will run concurrently with the merge goroutine.
		go func() {
			wg.Add(1)

			// Waiting for the merge to start.
			time.Sleep(10 * time.Millisecond)

			require.Equal(t, true, db.isMerging)

			for i := 0; i < 10; i++ {
				// By selectively updating some values,
				// check if the merge process will overwrite the new values
				if i%2 == 0 {
					txPut(t, db, bucket, GetTestBytes(i), newValues[i], Persistent, nil)
				}
			}

			require.Equal(t, true, db.isMerging)

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
