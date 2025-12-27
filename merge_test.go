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
	"sync"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/testutils"
	"github.com/nutsdb/nutsdb/internal/ttl"
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

		removeDir(opts.Dir)

		for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
			removeDir(opts.Dir)
			opts.EntryIdxMode = idxMode
			db, err := Open(opts)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			require.NoError(t, err)

			err = db.Merge()
			require.Equal(t, ErrDontNeedMerge, err)

			n := 1000
			for i := 0; i < n; i++ {
				txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
			}

			for i := 0; i < n/2; i++ {
				txDel(t, db, bucket, testutils.GetTestBytes(i), nil)
			}

			require.NoError(t, db.Merge())

			dbCnt, err := db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(n/2), dbCnt)

			for i := 0; i < n/2; i++ {
				txGet(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), ErrKeyNotFound)
			}

			for i := n / 2; i < n; i++ {
				txGet(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), nil)
			}

			require.NoError(t, db.Close())

			db, err = Open(opts)
			require.NoError(t, err)

			dbCnt, err = db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(n/2), dbCnt)

			for i := 0; i < n/2; i++ {
				txGet(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), ErrKeyNotFound)
			}

			for i := n / 2; i < n; i++ {
				txGet(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), nil)
			}

			require.NoError(t, db.Close())
		}
		removeDir(opts.Dir)
	})
}

func TestDB_MergeMultipleTimesWithRestarts(t *testing.T) {
	runForMergeModes(t, func(t *testing.T, mode mergeTestMode) {
		opts := DefaultOptions
		opts.SegmentSize = KB
		opts.EnableMergeV2 = mode.enableMergeV2
		opts.Dir = fmt.Sprintf("/tmp/test-merge-multi-restart-%s/", mode.name)

		const (
			totalCycles     = 3
			entriesPerCycle = 200
		)

		stringBucket := "bucket_multi_btree"
		setBucket := "bucket_multi_set"
		listBucket := "bucket_multi_list"
		zsetBucket := "bucket_multi_zset"

		type multiDSState struct {
			stringExpected map[string][]byte
			stringDeleted  map[string]struct{}
			setExpected    map[string]struct{}
			setDeleted     map[string]struct{}
			listExpected   [][]byte
			zsetExpected   map[string]float64
			zsetDeleted    map[string]struct{}
			setKey         []byte
			listKey        []byte
			zsetKey        []byte
		}

		validateState := func(db *DB, st *multiDSState) {
			totalExpected := len(st.stringExpected) + len(st.setExpected) + len(st.listExpected) + len(st.zsetExpected)

			dbCnt, err := db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(totalExpected), dbCnt)

			for keyStr, val := range st.stringExpected {
				txGet(t, db, stringBucket, []byte(keyStr), val, nil)
			}
			for keyStr := range st.stringDeleted {
				if _, ok := st.stringExpected[keyStr]; ok {
					continue
				}
				txGet(t, db, stringBucket, []byte(keyStr), nil, ErrKeyNotFound)
			}

			for member := range st.setExpected {
				txSIsMember(t, db, setBucket, st.setKey, []byte(member), true)
			}
			for member := range st.setDeleted {
				if _, ok := st.setExpected[member]; ok {
					continue
				}
				txSIsMember(t, db, setBucket, st.setKey, []byte(member), false)
			}

			txLRange(t, db, listBucket, st.listKey, 0, -1, len(st.listExpected), st.listExpected, nil)

			for member, score := range st.zsetExpected {
				txZScore(t, db, zsetBucket, st.zsetKey, []byte(member), score, nil)
			}
			for member := range st.zsetDeleted {
				if _, ok := st.zsetExpected[member]; ok {
					continue
				}
				txZScore(t, db, zsetBucket, st.zsetKey, []byte(member), 0, ErrSortedSetMemberNotExist)
			}
		}

		for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
			removeDir(opts.Dir)
			opts.EntryIdxMode = idxMode

			db, err := Open(opts)
			require.NoError(t, err)

			txCreateBucket(t, db, DataStructureBTree, stringBucket, nil)
			txCreateBucket(t, db, DataStructureSet, setBucket, nil)
			txCreateBucket(t, db, DataStructureList, listBucket, nil)
			txCreateBucket(t, db, DataStructureSortedSet, zsetBucket, nil)

			state := &multiDSState{
				stringExpected: make(map[string][]byte),
				stringDeleted:  make(map[string]struct{}),
				setExpected:    make(map[string]struct{}),
				setDeleted:     make(map[string]struct{}),
				listExpected:   nil,
				zsetExpected:   make(map[string]float64),
				zsetDeleted:    make(map[string]struct{}),
				setKey:         testutils.GetTestBytes(0),
				listKey:        testutils.GetTestBytes(1),
				zsetKey:        testutils.GetTestBytes(2),
			}

			half := entriesPerCycle / 2

			for cycle := 0; cycle < totalCycles; cycle++ {
				if cycle > 0 {
					if len(state.stringExpected) > 0 {
						for keyStr := range state.stringExpected {
							txDel(t, db, stringBucket, []byte(keyStr), nil)
							state.stringDeleted[keyStr] = struct{}{}
						}
						state.stringExpected = make(map[string][]byte)
					}

					if len(state.setExpected) > 0 {
						for member := range state.setExpected {
							txSRem(t, db, setBucket, state.setKey, []byte(member), nil)
							state.setDeleted[member] = struct{}{}
						}
						state.setExpected = make(map[string]struct{})
					}

					if len(state.listExpected) > 0 {
						for _, val := range state.listExpected {
							txLRem(t, db, listBucket, state.listKey, 0, val, nil)
						}
						state.listExpected = nil
					}

					if len(state.zsetExpected) > 0 {
						for member := range state.zsetExpected {
							txZRem(t, db, zsetBucket, state.zsetKey, []byte(member), nil)
							state.zsetDeleted[member] = struct{}{}
						}
						state.zsetExpected = make(map[string]float64)
					}
				}

				cycleBase := cycle * entriesPerCycle * 20
				stringBase := cycleBase
				setBase := cycleBase + entriesPerCycle*5
				listBase := cycleBase + entriesPerCycle*10
				zsetBase := cycleBase + entriesPerCycle*15

				currentStringKeys := make([][]byte, 0, entriesPerCycle)
				for i := 0; i < entriesPerCycle; i++ {
					rawKey := testutils.GetTestBytes(stringBase + i)
					txPut(t, db, stringBucket, rawKey, rawKey, Persistent, nil, nil)
					currentStringKeys = append(currentStringKeys, rawKey)
				}
				for _, rawKey := range currentStringKeys {
					txPut(t, db, stringBucket, rawKey, rawKey, Persistent, nil, nil)
				}
				for i, rawKey := range currentStringKeys {
					keyStr := string(rawKey)
					if i < half {
						txDel(t, db, stringBucket, rawKey, nil)
						state.stringDeleted[keyStr] = struct{}{}
						continue
					}
					state.stringExpected[keyStr] = append([]byte(nil), rawKey...)
				}

				currentSetMembers := make([][]byte, 0, entriesPerCycle)
				for i := 0; i < entriesPerCycle; i++ {
					member := testutils.GetTestBytes(setBase + i)
					txSAdd(t, db, setBucket, state.setKey, member, nil, nil)
					currentSetMembers = append(currentSetMembers, member)
				}
				for _, member := range currentSetMembers {
					txSAdd(t, db, setBucket, state.setKey, member, nil, nil)
				}
				for i, member := range currentSetMembers {
					memberStr := string(member)
					if i < half {
						txSRem(t, db, setBucket, state.setKey, member, nil)
						state.setDeleted[memberStr] = struct{}{}
						continue
					}
					state.setExpected[memberStr] = struct{}{}
				}

				currentListValues := make([][]byte, 0, entriesPerCycle)
				for i := 0; i < entriesPerCycle; i++ {
					val := testutils.GetTestBytes(listBase + i)
					txPush(t, db, listBucket, state.listKey, val, false, nil, nil)
					currentListValues = append(currentListValues, val)
				}
				for _, val := range currentListValues {
					txPush(t, db, listBucket, state.listKey, val, false, nil, nil)
				}
				for i := 0; i < half; i++ {
					txLRem(t, db, listBucket, state.listKey, 0, currentListValues[i], nil)
				}
				state.listExpected = make([][]byte, 0, entriesPerCycle-half)
				for i := half; i < entriesPerCycle; i++ {
					val := currentListValues[i]
					txLRem(t, db, listBucket, state.listKey, 1, val, nil)
					state.listExpected = append(state.listExpected, append([]byte(nil), val...))
				}

				currentZSetMembers := make([][]byte, 0, entriesPerCycle)
				for i := 0; i < entriesPerCycle; i++ {
					member := testutils.GetTestBytes(zsetBase + i)
					score1 := float64(zsetBase + i)
					score2 := score1 + 0.5
					txZAdd(t, db, zsetBucket, state.zsetKey, member, score1, nil, nil)
					txZAdd(t, db, zsetBucket, state.zsetKey, member, score2, nil, nil)
					currentZSetMembers = append(currentZSetMembers, member)
				}
				for i, member := range currentZSetMembers {
					memberStr := string(member)
					if i < half {
						txZRem(t, db, zsetBucket, state.zsetKey, member, nil)
						state.zsetDeleted[memberStr] = struct{}{}
						continue
					}
					state.zsetExpected[memberStr] = float64(zsetBase+i) + 0.5
				}

				validateState(db, state)

				require.NoError(t, db.Merge())

				validateState(db, state)

				require.NoError(t, db.Close())

				db, err = Open(opts)
				require.NoError(t, err)

				validateState(db, state)
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
			if exist := db.bucketMgr.ExistBucket(DataStructureSet, bucket); !exist {
				txCreateBucket(t, db, DataStructureSet, bucket, nil)
			}

			require.NoError(t, err)

			err = db.Merge()
			require.Equal(t, ErrDontNeedMerge, err)

			n := 1000
			key := testutils.GetTestBytes(0)
			for i := 0; i < n; i++ {
				txSAdd(t, db, bucket, key, testutils.GetTestBytes(i), nil, nil)
			}

			for i := 0; i < n/2; i++ {
				txSRem(t, db, bucket, key, testutils.GetTestBytes(i), nil)
			}

			var spopValue []byte
			err = db.Update(func(tx *Tx) error {
				var err error
				spopValue, err = tx.SPop(bucket, key)
				AssertErr(t, err, nil)
				return nil
			})
			require.NoError(t, err)

			txSIsMember(t, db, bucket, key, spopValue, false)

			dbCnt, err := db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(n/2-1), dbCnt)

			require.NoError(t, db.Merge())

			dbCnt, err = db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(n/2-1), dbCnt)

			txSIsMember(t, db, bucket, key, spopValue, false)
			for i := n / 2; i < n; i++ {
				v := testutils.GetTestBytes(i)
				if bytes.Equal(v, spopValue) {
					continue
				}
				txSIsMember(t, db, bucket, key, v, true)
			}

			require.NoError(t, db.Close())

			db, err = Open(opts)
			require.NoError(t, err)

			dbCnt, err = db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(n/2-1), dbCnt)

			txSIsMember(t, db, bucket, key, spopValue, false)
			for i := n / 2; i < n; i++ {
				v := testutils.GetTestBytes(i)
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

func TestDB_MergeForZSet(t *testing.T) {
	runForMergeModes(t, func(t *testing.T, mode mergeTestMode) {
		bucket := "bucket"
		key := testutils.GetTestBytes(0)
		n := 1000
		opts := DefaultOptions
		opts.SegmentSize = KB
		opts.EnableMergeV2 = mode.enableMergeV2
		opts.Dir = fmt.Sprintf("/tmp/test-zset-merge-%s/", mode.name)

		for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
			removeDir(opts.Dir)
			opts.EntryIdxMode = idxMode
			db, err := Open(opts)
			if exist := db.bucketMgr.ExistBucket(DataStructureSortedSet, bucket); !exist {
				txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)
			}
			require.NoError(t, err)

			err = db.Merge()
			require.Equal(t, ErrDontNeedMerge, err)

			for i := 0; i < n; i++ {
				score, _ := strconv2.IntToFloat64(i)
				txZAdd(t, db, bucket, key, testutils.GetTestBytes(i), score, nil, nil)
			}

			for i := 0; i < n; i++ {
				score, _ := strconv2.IntToFloat64(i)
				txZScore(t, db, bucket, key, testutils.GetTestBytes(i), score, nil)
			}

			for i := 0; i < n/2; i++ {
				txZRem(t, db, bucket, key, testutils.GetTestBytes(i), nil)
			}

			for i := 0; i < n/2; i++ {
				score, _ := strconv2.IntToFloat64(i)
				txZScore(t, db, bucket, key, testutils.GetTestBytes(i), score, ErrSortedSetMemberNotExist)
			}

			for i := n / 2; i < n; i++ {
				score, _ := strconv2.IntToFloat64(i)
				txZScore(t, db, bucket, key, testutils.GetTestBytes(i), score, nil)
			}

			dbCnt, err := db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(n/2), dbCnt)

			require.NoError(t, db.Merge())

			dbCnt, err = db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(n/2), dbCnt)

			for i := 0; i < n/2; i++ {
				score, _ := strconv2.IntToFloat64(i)
				txZScore(t, db, bucket, key, testutils.GetTestBytes(i), score, ErrSortedSetMemberNotExist)
			}

			for i := n / 2; i < n; i++ {
				score, _ := strconv2.IntToFloat64(i)
				txZScore(t, db, bucket, key, testutils.GetTestBytes(i), score, nil)
			}

			require.NoError(t, db.Close())

			db, err = Open(opts)
			require.NoError(t, err)
			dbCnt, err = db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(n/2), dbCnt)

			for i := 0; i < n/2; i++ {
				score, _ := strconv2.IntToFloat64(i)
				txZScore(t, db, bucket, key, testutils.GetTestBytes(i), score, ErrSortedSetMemberNotExist)
			}

			for i := n / 2; i < n; i++ {
				score, _ := strconv2.IntToFloat64(i)
				txZScore(t, db, bucket, key, testutils.GetTestBytes(i), score, nil)
			}

			require.NoError(t, db.Close())
		}
		removeDir(opts.Dir)
	})
}

func TestDB_MergeForList(t *testing.T) {
	runForMergeModes(t, func(t *testing.T, mode mergeTestMode) {
		bucket := "bucket"
		key := testutils.GetTestBytes(0)
		opts := DefaultOptions
		opts.SegmentSize = KB
		opts.EnableMergeV2 = mode.enableMergeV2
		opts.Dir = fmt.Sprintf("/tmp/test-list-merge-%s/", mode.name)

		for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
			removeDir(opts.Dir)
			opts.EntryIdxMode = idxMode
			db, err := Open(opts)
			if exist := db.bucketMgr.ExistBucket(DataStructureList, bucket); !exist {
				txCreateBucket(t, db, DataStructureList, bucket, nil)
			}

			require.NoError(t, err)

			err = db.Merge()
			require.Equal(t, ErrDontNeedMerge, err)

			n := 1000
			for i := 0; i < n; i++ {
				txPush(t, db, bucket, key, testutils.GetTestBytes(i), true, nil, nil)
			}

			for i := n; i < 2*n; i++ {
				txPush(t, db, bucket, key, testutils.GetTestBytes(i), false, nil, nil)
			}

			for i := n - 1; i >= n/2; i-- {
				txPop(t, db, bucket, key, testutils.GetTestBytes(i), nil, true)
			}

			for i := 2*n - 1; i >= 3*n/2; i-- {
				txPop(t, db, bucket, key, testutils.GetTestBytes(i), nil, false)
			}

			txLTrim(t, db, bucket, key, 0, 9, nil)
			txLRem(t, db, bucket, key, 0, testutils.GetTestBytes(100), nil)
			txLRemByIndex(t, db, bucket, key, nil, []int{7, 8, 9}...)

			dbCnt, err := db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(7), dbCnt)

			require.NoError(t, db.Merge())

			dbCnt, err = db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(7), dbCnt)

			require.NoError(t, db.Close())

			db, err = Open(opts)
			require.NoError(t, err)

			dbCnt, err = db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(7), dbCnt)

			for i := n/2 - 1; i < n/2-8; i-- {
				txPop(t, db, bucket, key, testutils.GetTestBytes(i), nil, true)
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

			db, err := Open(opts)
			require.NoError(t, err)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			n := 2000
			for i := 0; i < n; i++ {
				txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
			}

			for i := 0; i < n/2; i++ {
				txDel(t, db, bucket, testutils.GetTestBytes(i), nil)
			}

			require.NoError(t, db.Close())
			db, err = Open(opts)
			require.NoError(t, err)

			require.NoError(t, db.Merge())

			sets := collectMergeFileSets(t, opts.Dir)
			idsToCheck := dataFileIDsForMode(mode, sets)
			require.Greater(t, len(idsToCheck), 0)

			totalHintEntryCount := 0
			for _, fid := range idsToCheck {
				hintPath := getHintPath(fid, opts.Dir)

				_, err = os.Stat(hintPath)
				require.NoError(t, err, "Hint file should exist after merge for file %d", fid)

				reader := &HintFileReader{}
				err = reader.Open(hintPath)
				require.NoError(t, err)

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

			require.Equal(t, n/2, totalHintEntryCount)

			dbCnt, err := db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(n/2), dbCnt)

			for i := 0; i < n/2; i++ {
				txGet(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), ErrKeyNotFound)
			}

			for i := n / 2; i < n; i++ {
				txGet(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), nil)
			}

			require.NoError(t, db.Close())
			db, err = Open(opts)
			require.NoError(t, err)

			dbCnt, err = db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(n/2), dbCnt)

			for i := 0; i < n/2; i++ {
				txGet(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), ErrKeyNotFound)
			}

			for i := n / 2; i < n; i++ {
				txGet(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), nil)
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

		db, err := Open(opts)
		require.NoError(t, err)
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		n := 500
		for i := 0; i < n; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		for i := 0; i < n/4; i++ {
			txDel(t, db, bucket, testutils.GetTestBytes(i), nil)
		}

		_, initialFileIDs := db.getMaxFileIDAndFileIDs()

		for _, fileID := range initialFileIDs {
			hintPath := getHintPath(fileID, opts.Dir)
			writer := &HintFileWriter{}
			err := writer.Create(hintPath)
			require.NoError(t, err)

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

		for _, fileID := range initialFileIDs {
			hintPath := getHintPath(fileID, opts.Dir)
			_, err := os.Stat(hintPath)
			require.NoError(t, err, "Hint file should exist before merge")
		}

		require.NoError(t, db.Merge())

		for _, fileID := range initialFileIDs {
			hintPath := getHintPath(fileID, opts.Dir)
			_, err := os.Stat(hintPath)
			if err == nil {
				t.Errorf("Old hint file %s should be cleaned up after merge", hintPath)
			}
		}

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

		db, err := Open(opts)
		require.NoError(t, err)
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		n := 500
		for i := 0; i < n; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		for i := 0; i < n/4; i++ {
			txDel(t, db, bucket, testutils.GetTestBytes(i), nil)
		}

		require.NoError(t, db.Merge())

		sets := collectMergeFileSets(t, opts.Dir)
		idsToCheck := unionDataFileIDs(sets)
		for _, fid := range idsToCheck {
			hintPath := getHintPath(fid, opts.Dir)
			_, err := os.Stat(hintPath)
			if err == nil {
				t.Errorf("Hint file %s should not exist when EnableHintFile is false", hintPath)
			}
		}

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

		bucketBTree := "bucket_btree"
		txCreateBucket(t, db, DataStructureBTree, bucketBTree, nil)
		for i := 0; i < 100; i++ {
			txPut(t, db, bucketBTree, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		bucketSet := "bucket_set"
		txCreateBucket(t, db, DataStructureSet, bucketSet, nil)
		key := testutils.GetTestBytes(0)
		for i := 0; i < 50; i++ {
			txSAdd(t, db, bucketSet, key, testutils.GetTestBytes(i), nil, nil)
		}

		bucketList := "bucket_list"
		txCreateBucket(t, db, DataStructureList, bucketList, nil)
		listKey := testutils.GetTestBytes(0)
		for i := 0; i < 30; i++ {
			txPush(t, db, bucketList, listKey, testutils.GetTestBytes(i), true, nil, nil)
		}

		bucketZSet := "bucket_zset"
		txCreateBucket(t, db, DataStructureSortedSet, bucketZSet, nil)
		zsetKey := testutils.GetTestBytes(0)
		for i := 0; i < 20; i++ {
			txZAdd(t, db, bucketZSet, zsetKey, testutils.GetTestBytes(i), float64(i), nil, nil)
		}

		for i := 0; i < 25; i++ {
			txDel(t, db, bucketBTree, testutils.GetTestBytes(i), nil)
		}
		for i := 0; i < 10; i++ {
			txSRem(t, db, bucketSet, key, testutils.GetTestBytes(i), nil)
		}
		for i := 0; i < 5; i++ {
			txPop(t, db, bucketList, listKey, testutils.GetTestBytes(i), nil, false)
		}
		for i := 0; i < 5; i++ {
			txZRem(t, db, bucketZSet, zsetKey, testutils.GetTestBytes(i), nil)
		}

		require.NoError(t, db.Merge())

		sets := collectMergeFileSets(t, opts.Dir)
		idsToCheck := dataFileIDsForMode(mode, sets)
		require.Greater(t, len(idsToCheck), 0)

		btreeCount := 0
		setCount := 0
		listCount := 0
		zsetCount := 0

		for _, fid := range idsToCheck {
			hintPath := getHintPath(fid, opts.Dir)
			_, err = os.Stat(hintPath)
			require.NoError(t, err, "Hint file should exist after merge for file %d", fid)

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

		require.Equal(t, 75, btreeCount)
		require.Equal(t, 40, setCount)
		require.Equal(t, 25, listCount)
		require.Equal(t, 15, zsetCount)

		require.NoError(t, db.Close())
		db, err = Open(opts)
		require.NoError(t, err)

		for i := 25; i < 100; i++ {
			txGet(t, db, bucketBTree, testutils.GetTestBytes(i), testutils.GetTestBytes(i), nil)
		}
		for i := 0; i < 25; i++ {
			txGet(t, db, bucketBTree, testutils.GetTestBytes(i), testutils.GetTestBytes(i), ErrKeyNotFound)
		}

		for i := 10; i < 50; i++ {
			txSIsMember(t, db, bucketSet, key, testutils.GetTestBytes(i), true)
		}
		for i := 0; i < 10; i++ {
			txSIsMember(t, db, bucketSet, key, testutils.GetTestBytes(i), false)
		}

		for i := 29; i < 24; i++ {
			txLRange(t, db, bucketList, listKey, i-5, i-5, 1, [][]byte{testutils.GetTestBytes(i)}, nil)
		}

		for i := 5; i < 20; i++ {
			txZScore(t, db, bucketZSet, zsetKey, testutils.GetTestBytes(i), float64(i), nil)
		}
		for i := 0; i < 5; i++ {
			txZScore(t, db, bucketZSet, zsetKey, testutils.GetTestBytes(i), 0, ErrSortedSetMemberNotExist)
		}

		require.NoError(t, db.Close())
		removeDir(opts.Dir)
	})
}

func TestDB_MergeModeSwitching(t *testing.T) {
	const (
		totalKeys = 300
		valueSize = 512
	)

	dir := t.TempDir()

	baseOpts := DefaultOptions
	baseOpts.Dir = dir
	baseOpts.EnableHintFile = true
	baseOpts.SegmentSize = 4 * KB
	baseOpts.RWMode = FileIO

	legacyOpts := baseOpts
	legacyOpts.EnableMergeV2 = false

	v2Opts := baseOpts
	v2Opts.EnableMergeV2 = true

	bucket := "switch-bucket"

	valueFor := func(stage byte, idx int) []byte {
		body := bytes.Repeat([]byte{stage}, valueSize-8)
		suffix := []byte(fmt.Sprintf("%08d", idx))
		return append(body, suffix...)
	}

	writeStage := func(db *DB, stage byte) {
		for i := 0; i < totalKeys; i++ {
			key := []byte(fmt.Sprintf("key-%04d", i))
			value := valueFor(stage, i)
			require.NoError(t, db.Update(func(tx *Tx) error {
				return tx.Put(bucket, key, value, Persistent)
			}))
		}
	}

	assertStage := func(db *DB, stage byte) {
		require.NoError(t, db.View(func(tx *Tx) error {
			for i := 0; i < totalKeys; i++ {
				key := []byte(fmt.Sprintf("key-%04d", i))
				expected := valueFor(stage, i)
				got, err := tx.Get(bucket, key)
				if err != nil {
					return fmt.Errorf("get %q: %w", key, err)
				}
				if !bytes.Equal(got, expected) {
					return fmt.Errorf("value mismatch for %q", key)
				}
			}
			return nil
		}))
	}

	openWith := func(opts Options) *DB {
		db, err := Open(opts)
		require.NoError(t, err)
		return db
	}

	db := openWith(legacyOpts)
	require.NoError(t, db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, bucket)
	}))

	writeStage(db, 'A')
	assertStage(db, 'A')

	require.NoError(t, db.Merge())
	assertStage(db, 'A')
	require.NoError(t, db.Close())

	db = openWith(v2Opts)
	assertStage(db, 'A')

	writeStage(db, 'B')
	assertStage(db, 'B')

	require.NoError(t, db.Merge())
	assertStage(db, 'B')
	require.NoError(t, db.Close())

	db = openWith(legacyOpts)
	assertStage(db, 'B')

	writeStage(db, 'C')
	assertStage(db, 'C')

	require.NoError(t, db.Merge())
	assertStage(db, 'C')
	require.NoError(t, db.Close())
}

func TestDB_MergeWithTTLScanner(t *testing.T) {
	runForMergeModes(t, func(t *testing.T, mode mergeTestMode) {
		bucket := "test_bucket"
		opts := DefaultOptions
		opts.SegmentSize = 64 * KB // Small segment size to trigger more frequent merges
		opts.EnableMergeV2 = mode.enableMergeV2
		opts.Dir = fmt.Sprintf("/tmp/test-merge-ttl-scanner-%s-%d/", mode.name, time.Now().UnixNano())
		opts.TTLConfig = ttl.Config{
			ScanInterval:     50 * time.Millisecond, // Fast scan interval
			SampleSize:       100,                   // More samples per scan
			ExpiredThreshold: 0.5,                   // Continue scanning if 50% expired
			BatchSize:        50,
			BatchTimeout:     10 * time.Millisecond,
		}
		opts.MergeInterval = 200 * time.Millisecond

		removeDir(opts.Dir)
		defer removeDir(opts.Dir)

		db, err := Open(opts)
		require.NoError(t, err)
		defer db.Close()

		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		const initialKeys = 500
		for i := 0; i < initialKeys; i++ {
			key := testutils.GetTestBytes(i)
			err := db.Update(func(tx *Tx) error {
				return tx.Put(bucket, key, testutils.GetTestBytes(i), uint32(1000))
			})
			require.NoError(t, err)
		}

		for fileIdx := 0; fileIdx < 3; fileIdx++ {
			require.NoError(t, db.Close())
			db, err = Open(opts)
			require.NoError(t, err)
			if exist := db.bucketMgr.ExistBucket(DataStructureBTree, bucket); !exist {
				txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			}

			for i := initialKeys + fileIdx*200; i < initialKeys+(fileIdx+1)*200; i++ {
				key := testutils.GetTestBytes(i)
				err := db.Update(func(tx *Tx) error {
					return tx.Put(bucket, key, testutils.GetTestBytes(i), uint32(5000))
				})
				require.NoError(t, err)
			}
		}

		require.NoError(t, db.Close())
		db, err = Open(opts)
		require.NoError(t, err)
		defer db.Close()
		if exist := db.bucketMgr.ExistBucket(DataStructureBTree, bucket); !exist {
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		}

		var wg sync.WaitGroup
		stopCh := make(chan struct{})

		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
					key := testutils.GetTestBytes(int(time.Now().UnixNano()))
					_ = db.Update(func(tx *Tx) error {
						return tx.Put(bucket, key, []byte("value"), uint32(5000))
					})
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
					_ = db.Merge()
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(30 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
					_ = db.View(func(tx *Tx) error {
						it := NewIterator(tx, bucket, IteratorOptions{})
						for it.Seek(nil); it.Valid(); it.Next() {
							_, _ = it.Value()
						}
						it.Release()
						return nil
					})
				}
			}
		}()

		time.Sleep(2 * time.Second)

		close(stopCh)
		wg.Wait()

		require.NoError(t, db.Close())
	})
}
