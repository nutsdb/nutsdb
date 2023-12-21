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
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	db  *DB
	opt Options
	err error
)

const NutsDBTestDirPath = "/tmp/nutsdb-test"

func assertErr(t *testing.T, err error, expectErr error) {
	if expectErr != nil {
		require.Equal(t, expectErr, err)
	} else {
		require.NoError(t, err)
	}
}

func removeDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
}

func runNutsDBTest(t *testing.T, opts *Options, test func(t *testing.T, db *DB)) {
	if opts == nil {
		opts = &DefaultOptions
	}
	if opts.Dir == "" {
		opts.Dir = NutsDBTestDirPath
	}
	defer removeDir(opts.Dir)
	db, err := Open(*opts)
	require.NoError(t, err)

	test(t, db)
	t.Cleanup(func() {
		if !db.IsClose() {
			require.NoError(t, db.Close())
		}
	})
}

func txPut(t *testing.T, db *DB, bucket string, key, value []byte, ttl uint32, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err = tx.Put(bucket, key, value, ttl)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txGet(t *testing.T, db *DB, bucket string, key []byte, expectVal []byte, expectErr error) {
	err := db.View(func(tx *Tx) error {
		value, err := tx.Get(bucket, key)
		if expectErr != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
			require.EqualValuesf(t, expectVal, value, "err Tx Get. got %s want %s", string(value), string(expectVal))
		}
		return nil
	})
	require.NoError(t, err)
}

func txDel(t *testing.T, db *DB, bucket string, key []byte, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.Delete(bucket, key)
		assertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func txGetMaxOrMinKey(t *testing.T, db *DB, bucket string, isMax bool, expectVal []byte, expectErr error) {
	err := db.View(func(tx *Tx) error {
		value, err := tx.getMaxOrMinKey(bucket, isMax)
		if expectErr != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
			require.EqualValuesf(t, expectVal, value, "err Tx Get. got %s want %s", string(value), string(expectVal))
		}
		return nil
	})
	require.NoError(t, err)
}

func txDeleteBucket(t *testing.T, db *DB, ds uint16, bucket string, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.DeleteBucket(ds, bucket)
		assertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func txCreateBucket(t *testing.T, db *DB, ds uint16, bucket string, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.NewBucket(ds, bucket)
		assertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func InitOpt(fileDir string, isRemoveFiles bool) {
	if fileDir == "" {
		fileDir = "/tmp/nutsdbtest"
	}
	if isRemoveFiles {
		files, _ := ioutil.ReadDir(fileDir)
		for _, f := range files {
			name := f.Name()
			if name != "" {
				err := os.RemoveAll(fileDir + "/" + name)
				if err != nil {
					panic(err)
				}
			}
		}
	}

	opt = DefaultOptions
	opt.Dir = fileDir
	opt.SegmentSize = 8 * 1024
	opt.CleanFdsCacheThreshold = 0.5
	opt.MaxFdNumsInCache = 1024
}

func TestDB_Basic(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		key0 := GetTestBytes(0)
		val0 := GetRandomBytes(24)

		// put
		txPut(t, db, bucket, key0, val0, Persistent, nil, nil)
		txGet(t, db, bucket, key0, val0, nil)

		val1 := GetRandomBytes(24)

		// update
		txPut(t, db, bucket, key0, val1, Persistent, nil, nil)
		txGet(t, db, bucket, key0, val1, nil)

		// del
		txDel(t, db, bucket, key0, nil)
		txGet(t, db, bucket, key0, val1, ErrKeyNotFound)
	})
}

func TestDB_ReopenWithDelete(t *testing.T) {
	var opts *Options
	if opts == nil {
		opts = &DefaultOptions
	}
	if opts.Dir == "" {
		opts.Dir = NutsDBTestDirPath
	}
	db, err := Open(*opts)
	require.NoError(t, err)
	defer removeDir(opts.Dir)

	bucket := "bucket"
	txCreateBucket(t, db, DataStructureList, bucket, nil)
	txPush(t, db, bucket, GetTestBytes(5), GetTestBytes(0), true, nil, nil)
	txPush(t, db, bucket, GetTestBytes(5), GetTestBytes(1), true, nil, nil)
	txDeleteBucket(t, db, DataStructureList, bucket, nil)

	if !db.IsClose() {
		require.NoError(t, db.Close())
	}

	db, err = Open(*opts)
	require.NoError(t, err)
	txCreateBucket(t, db, DataStructureList, bucket, nil)
	txDeleteBucket(t, db, DataStructureList, bucket, nil)
	if !db.IsClose() {
		require.NoError(t, db.Close())
	}
}

func TestDB_Flock(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		db2, err := Open(db.opt)
		require.Nil(t, db2)
		require.Equal(t, ErrDirLocked, err)

		err = db.Close()
		require.NoError(t, err)

		db2, err = Open(db.opt)
		require.NoError(t, err)
		require.NotNil(t, db2)

		err = db2.flock.Unlock()
		require.NoError(t, err)
		require.False(t, db2.flock.Locked())

		err = db2.Close()
		require.Error(t, err)
		require.Equal(t, ErrDirUnlocked, err)
	})
}

func TestDB_DeleteANonExistKey(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		testBucket := "test_bucket"
		txCreateBucket(t, db, DataStructureBTree, testBucket, nil)

		txDel(t, db, testBucket, GetTestBytes(0), ErrKeyNotFound)
		txPut(t, db, testBucket, GetTestBytes(1), GetRandomBytes(24), Persistent, nil, nil)
		txDel(t, db, testBucket, GetTestBytes(0), ErrKeyNotFound)
	})
}

func TestDB_CheckListExpired(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		testBucket := "test_bucket"
		txCreateBucket(t, db, DataStructureBTree, testBucket, nil)

		txPut(t, db, testBucket, GetTestBytes(0), GetTestBytes(1), Persistent, nil, nil)
		txPut(t, db, testBucket, GetTestBytes(1), GetRandomBytes(24), 1, nil, nil)

		time.Sleep(1100 * time.Millisecond)

		db.checkListExpired()

		// this entry still alive
		txGet(t, db, testBucket, GetTestBytes(0), GetTestBytes(1), nil)
		// this entry will be deleted
		txGet(t, db, testBucket, GetTestBytes(1), nil, ErrKeyNotFound)
	})
}

func txLRem(t *testing.T, db *DB, bucket string, key []byte, count int, value []byte, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.LRem(bucket, key, count, value)
		assertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func txLRemByIndex(t *testing.T, db *DB, bucket string, key []byte, expectErr error, indexes ...int) {
	err := db.Update(func(tx *Tx) error {
		err := tx.LRemByIndex(bucket, key, indexes...)
		assertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func txSAdd(t *testing.T, db *DB, bucket string, key, value []byte, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.SAdd(bucket, key, value)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txSKeys(t *testing.T, db *DB, bucket, pattern string, f func(key string) bool, expectVal int, expectErr error) {
	err := db.View(func(tx *Tx) error {
		patternMatchNum := 0
		err := tx.SKeys(bucket, pattern, func(key string) bool {
			patternMatchNum += 1
			return f(key)
		})
		if expectErr != nil {
			assert.ErrorIs(t, expectErr, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, expectVal, patternMatchNum)
		}
		return nil
	})
	require.NoError(t, err)
}

func txSIsMember(t *testing.T, db *DB, bucket string, key, value []byte, expect bool) {
	err := db.View(func(tx *Tx) error {
		ok, _ := tx.SIsMember(bucket, key, value)
		require.Equal(t, expect, ok)
		return nil
	})
	require.NoError(t, err)
}

func txSAreMembers(t *testing.T, db *DB, bucket string, key []byte, expect bool, value ...[]byte) {
	err := db.View(func(tx *Tx) error {
		ok, _ := tx.SAreMembers(bucket, key, value...)
		require.Equal(t, expect, ok)
		return nil
	})
	require.NoError(t, err)
}

func txSHasKey(t *testing.T, db *DB, bucket string, key []byte, expect bool) {
	err := db.View(func(tx *Tx) error {
		ok, _ := tx.SHasKey(bucket, key)
		require.Equal(t, expect, ok)
		return nil
	})
	require.NoError(t, err)
}

func txSMembers(t *testing.T, db *DB, bucket string, key []byte, expectLength int, expectErr error) {
	err := db.View(func(tx *Tx) error {
		members, err := tx.SMembers(bucket, key)
		if expectErr != nil {
			assert.ErrorIs(t, expectErr, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, expectLength, len(members))
		}
		return nil
	})
	require.NoError(t, err)
}

func txSCard(t *testing.T, db *DB, bucket string, key []byte, expectLength int, expectErr error) {
	err := db.View(func(tx *Tx) error {
		length, err := tx.SCard(bucket, key)
		if expectErr != nil {
			assert.ErrorIs(t, expectErr, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, expectLength, length)
		}
		return nil
	})
	require.NoError(t, err)
}

func txSDiffByOneBucket(t *testing.T, db *DB, bucket string, key1, key2 []byte, expectVal [][]byte, expectErr error) {
	err := db.View(func(tx *Tx) error {
		diff, err := tx.SDiffByOneBucket(bucket, key1, key2)
		if expectErr != nil {
			assert.ErrorIs(t, expectErr, err)
		} else {
			assert.NoError(t, err)
			assert.ElementsMatch(t, expectVal, diff)
		}
		return nil
	})
	require.NoError(t, err)
}

func txSDiffByTwoBucket(t *testing.T, db *DB, bucket1 string, key1 []byte, bucket2 string, key2 []byte, expectVal [][]byte, expectErr error) {
	err := db.View(func(tx *Tx) error {
		diff, err := tx.SDiffByTwoBuckets(bucket1, key1, bucket2, key2)
		if expectErr != nil {
			assert.ErrorIs(t, err, expectErr)
		} else {
			assert.NoError(t, err)
			assert.ElementsMatch(t, expectVal, diff)
		}
		return nil
	})
	require.NoError(t, err)
}

func txSPop(t *testing.T, db *DB, bucket string, key []byte, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		_, err := tx.SPop(bucket, key)
		assertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func txSMoveByOneBucket(t *testing.T, db *DB, bucket1 string, key1, key2, val []byte, expectVal bool, expectErr error) {
	err := db.View(func(tx *Tx) error {
		ok, err := tx.SMoveByOneBucket(bucket1, key1, key2, val)
		if expectErr != nil {
			assert.ErrorIs(t, err, expectErr)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, expectVal, ok)
		}
		return nil
	})
	require.NoError(t, err)
}

func txSMoveByTwoBuckets(t *testing.T, db *DB, bucket1 string, key1 []byte, bucket2 string, key2 []byte, val []byte, expectVal bool, expectErr error) {
	err := db.View(func(tx *Tx) error {
		ok, err := tx.SMoveByTwoBuckets(bucket1, key1, bucket2, key2, val)
		if expectErr != nil {
			assert.ErrorIs(t, err, expectErr)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, expectVal, ok)
		}
		return nil
	})
	require.NoError(t, err)
}

func txSUnionByOneBucket(t *testing.T, db *DB, bucket1 string, key1, key2 []byte, expectVal [][]byte, expectErr error) {
	err := db.View(func(tx *Tx) error {
		union, err := tx.SUnionByOneBucket(bucket1, key1, key2)
		if expectErr != nil {
			assert.ErrorIs(t, err, expectErr)
		} else {
			assert.NoError(t, err)
			assert.ElementsMatch(t, expectVal, union)
		}
		return nil
	})
	require.NoError(t, err)
}

func txSUnionByTwoBuckets(t *testing.T, db *DB, bucket1 string, key1 []byte, bucket2 string, key2 []byte, expectVal [][]byte, expectErr error) {
	err := db.View(func(tx *Tx) error {
		union, err := tx.SUnionByTwoBuckets(bucket1, key1, bucket2, key2)
		if expectErr != nil {
			assert.ErrorIs(t, err, expectErr)
		} else {
			assert.NoError(t, err)
			assert.ElementsMatch(t, expectVal, union)
		}
		return nil
	})
	require.NoError(t, err)
}

func txSRem(t *testing.T, db *DB, bucket string, key, value []byte, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.SRem(bucket, key, value)
		assertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func txZAdd(t *testing.T, db *DB, bucket string, key, value []byte, score float64, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.ZAdd(bucket, key, score, value)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txZRem(t *testing.T, db *DB, bucket string, key, value []byte, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.ZRem(bucket, key, value)
		assertErr(t, err, expectErr)
		return nil
	})
	assert.NoError(t, err)
}

func txZCard(t *testing.T, db *DB, bucket string, key []byte, expectLength int, expectErr error) {
	err := db.View(func(tx *Tx) error {
		length, err := tx.ZCard(bucket, key)
		if expectErr != nil {
			assert.Equal(t, expectErr, err)
		} else {
			assert.Equal(t, expectLength, length)
		}
		return nil
	})
	assert.NoError(t, err)
}

func txZScore(t *testing.T, db *DB, bucket string, key, value []byte, expectScore float64, expectErr error) {
	err := db.View(func(tx *Tx) error {
		score, err := tx.ZScore(bucket, key, value)
		if err != nil {
			assert.Equal(t, expectErr, err)
		} else {
			assert.Equal(t, expectScore, score)
		}
		return nil
	})
	assert.NoError(t, err)
}

func txZRank(t *testing.T, db *DB, bucket string, key, value []byte, isRev bool, expectRank int, expectErr error) {
	err := db.View(func(tx *Tx) error {
		var (
			rank int
			err  error
		)
		if isRev {
			rank, err = tx.ZRevRank(bucket, key, value)
		} else {
			rank, err = tx.ZRank(bucket, key, value)
		}
		if expectErr != nil {
			assert.Equal(t, expectErr, err)
		} else {
			assert.Equal(t, expectRank, rank)
		}
		return nil
	})
	assert.NoError(t, err)
}

func txZPop(t *testing.T, db *DB, bucket string, key []byte, isMax bool, expectVal []byte, expectScore float64, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		var (
			member *SortedSetMember
			err    error
		)
		if isMax {
			member, err = tx.ZPopMax(bucket, key)
		} else {
			member, err = tx.ZPopMin(bucket, key)
		}

		if expectErr != nil {
			assert.Equal(t, expectErr, err)
		} else {
			assert.Equal(t, expectVal, member.Value)
			assert.Equal(t, expectScore, member.Score)
		}
		return nil
	})
	assert.NoError(t, err)
}

func txZPeekMin(t *testing.T, db *DB, bucket string, key, expectVal []byte, expectScore float64, expectErr, finalExpectErr error) {
	err := db.View(func(tx *Tx) error {
		minMem, err1 := tx.ZPeekMin(bucket, key)
		assertErr(t, err1, finalExpectErr)

		if expectErr == nil {
			require.Equal(t, &SortedSetMember{
				Value: expectVal,
				Score: expectScore,
			}, minMem)
		}
		return err1
	})
	assertErr(t, err, finalExpectErr)
}

func txZKeys(t *testing.T, db *DB, bucket, pattern string, f func(key string) bool, expectVal int, expectErr error) {
	err := db.View(func(tx *Tx) error {
		patternMatchNum := 0
		err := tx.ZKeys(bucket, pattern, func(key string) bool {
			patternMatchNum += 1
			return f(key)
		})
		if expectErr != nil {
			assert.ErrorIs(t, expectErr, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, expectVal, patternMatchNum)
		}
		return nil
	})
	require.NoError(t, err)
}

func txPop(t *testing.T, db *DB, bucket string, key, expectVal []byte, expectErr error, isLeft bool) {
	err := db.Update(func(tx *Tx) error {
		var item []byte
		var err error

		if isLeft {
			item, err = tx.LPop(bucket, key)
		} else {
			item, err = tx.RPop(bucket, key)
		}

		if expectErr != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.Equal(t, expectVal, item)
		}

		return nil
	})
	require.NoError(t, err)
}

func txPush(t *testing.T, db *DB, bucket string, key, val []byte, isLeft bool, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		var err error

		if isLeft {
			err = tx.LPush(bucket, key, val)
		} else {
			err = tx.RPush(bucket, key, val)
		}

		assertErr(t, err, expectErr)

		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txMPush(t *testing.T, db *DB, bucket string, key []byte, vals [][]byte, isLeft bool, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		var err error

		if isLeft {
			err = tx.LPush(bucket, key, vals...)
		} else {
			err = tx.RPush(bucket, key, vals...)
		}

		assertErr(t, err, expectErr)

		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txPushRaw(t *testing.T, db *DB, bucket string, key, val []byte, isLeft bool, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		var err error

		if isLeft {
			err = tx.LPushRaw(bucket, key, val)
		} else {
			err = tx.RPushRaw(bucket, key, val)
		}

		assertErr(t, err, expectErr)

		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txExpireList(t *testing.T, db *DB, bucket string, key []byte, ttl uint32, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.ExpireList(bucket, key, ttl)
		assertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func txGetListTTL(t *testing.T, db *DB, bucket string, key []byte, expectVal uint32, expectErr error) {
	err := db.View(func(tx *Tx) error {
		ttl, err := tx.GetListTTL(bucket, key)
		assertErr(t, err, expectErr)
		require.Equal(t, ttl, expectVal)
		return nil
	})
	require.NoError(t, err)
}

func txLKeys(t *testing.T, db *DB, bucket, pattern string, expectLen int, expectErr error, keysOperation func(keys []string) bool) {
	err := db.View(func(tx *Tx) error {
		var keys []string
		err := tx.LKeys(bucket, pattern, func(key string) bool {
			keys = append(keys, key)
			return keysOperation(keys)
		})
		assertErr(t, err, expectErr)
		require.Equal(t, expectLen, len(keys))
		return nil
	})
	require.NoError(t, err)
}

func txLRange(t *testing.T, db *DB, bucket string, key []byte, start, end, expectLen int, expectVal [][]byte, expectErr error) {
	err := db.View(func(tx *Tx) error {
		list, err := tx.LRange(bucket, key, start, end)
		assertErr(t, err, expectErr)

		require.Equal(t, expectLen, len(list))

		if len(expectVal) > 0 {
			for i, val := range list {
				assert.Equal(t, expectVal[i], val)
			}
		}

		return nil
	})
	require.NoError(t, err)
}

func txLSize(t *testing.T, db *DB, bucket string, key []byte, expectVal int, expectErr error) {
	err := db.View(func(tx *Tx) error {
		size, err := tx.LSize(bucket, key)
		assertErr(t, err, expectErr)

		require.Equal(t, expectVal, size)

		return nil
	})
	require.NoError(t, err)
}

func txLTrim(t *testing.T, db *DB, bucket string, key []byte, start int, end int, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.LTrim(bucket, key, start, end)
		assertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func txIterateBuckets(t *testing.T, db *DB, ds uint16, pattern string, f func(key string) bool, expectErr error, containsKey ...string) {
	err := db.View(func(tx *Tx) error {
		var elements []string
		err := tx.IterateBuckets(ds, pattern, func(key string) bool {
			if f != nil && !f(key) {
				return false
			}
			elements = append(elements, key)
			return true
		})
		if err != nil {
			assert.Equal(t, expectErr, err)
		} else {
			assert.NoError(t, err)
			for _, key := range containsKey {
				assert.Contains(t, elements, key)
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func TestDB_GetKeyNotFound(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		txGet(t, db, bucket, GetTestBytes(0), nil, ErrKeyNotFound)
		txPut(t, db, bucket, GetTestBytes(1), GetRandomBytes(24), Persistent, nil, nil)
		txGet(t, db, bucket, GetTestBytes(0), nil, ErrKeyNotFound)
	})
}

func TestDB_Backup(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		backUpDir := "/tmp/nutsdb-backup"
		require.NoError(t, db.Backup(backUpDir))
	})
}

func TestDB_BackupTarGZ(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		backUpFile := "/tmp/nutsdb-backup/backup.tar.gz"
		f, err := os.Create(backUpFile)
		require.NoError(t, err)
		require.NoError(t, db.BackupTarGZ(f))
	})
}

func TestDB_Close(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		require.NoError(t, db.Close())
		require.Equal(t, ErrDBClosed, db.Close())
	})
}

func TestDB_ErrThenReadWrite(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		bucket := "testForDeadLock"
		err = db.View(
			func(tx *Tx) error {
				return fmt.Errorf("err happened")
			})
		require.NotNil(t, err)

		err = db.View(
			func(tx *Tx) error {
				key := []byte("key1")
				_, err := tx.Get(bucket, key)
				if err != nil {
					return err
				}

				return nil
			})
		require.NotNil(t, err)

		notice := make(chan struct{})
		go func() {
			err = db.Update(
				func(tx *Tx) error {
					notice <- struct{}{}

					return nil
				})
			require.NoError(t, err)
		}()

		select {
		case <-notice:
		case <-time.After(1 * time.Second):
			t.Fatalf("exist deadlock")
		}
	})
}

func TestDB_ErrorHandler(t *testing.T) {
	opts := DefaultOptions
	handleErrCalled := false
	opts.ErrorHandler = ErrorHandlerFunc(func(err error) {
		handleErrCalled = true
	})

	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		err = db.View(
			func(tx *Tx) error {
				return fmt.Errorf("err happened")
			})
		require.NotNil(t, err)
		require.Equal(t, handleErrCalled, true)
	})
}

func TestDB_CommitBuffer(t *testing.T) {
	bucket := "bucket"

	opts := DefaultOptions
	opts.CommitBufferSize = 8 * MB
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		require.Equal(t, int64(8*MB), db.opt.CommitBufferSize)
		// When the database starts, the commit buffer should be allocated with the size of CommitBufferSize.
		require.Equal(t, 0, db.commitBuffer.Len())
		require.Equal(t, db.opt.CommitBufferSize, int64(db.commitBuffer.Cap()))
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		txPut(t, db, bucket, GetTestBytes(0), GetRandomBytes(24), Persistent, nil, nil)

		// When tx is committed, content of commit buffer should be empty, but do not release memory
		require.Equal(t, 0, db.commitBuffer.Len())
		require.Equal(t, db.opt.CommitBufferSize, int64(db.commitBuffer.Cap()))
	})

	opts = DefaultOptions
	opts.CommitBufferSize = 1 * KB
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		require.Equal(t, int64(1*KB), db.opt.CommitBufferSize)

		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		err := db.Update(func(tx *Tx) error {
			// making this tx big enough, it should not use the commit buffer
			for i := 0; i < 1000; i++ {
				err := tx.Put(bucket, GetTestBytes(i), GetRandomBytes(1024), Persistent)
				require.NoError(t, err)
			}
			return nil
		})
		require.NoError(t, err)

		require.Equal(t, 0, db.commitBuffer.Len())
		require.Equal(t, db.opt.CommitBufferSize, int64(db.commitBuffer.Cap()))
	})
}

func TestDB_DeleteBucket(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		key := GetTestBytes(0)
		val := GetTestBytes(0)
		txPut(t, db, bucket, key, val, Persistent, nil, nil)
		txGet(t, db, bucket, key, val, nil)

		txDeleteBucket(t, db, DataStructureBTree, bucket, nil)
		txPut(t, db, bucket, key, val, Persistent, ErrorBucketNotExist, nil)
	})
}

func withDBOption(t *testing.T, opt Options, fn func(t *testing.T, db *DB)) {
	db, err := Open(opt)
	require.NoError(t, err)

	defer func() {
		os.RemoveAll(db.opt.Dir)
		db.Close()
	}()

	fn(t, db)
}

func withDefaultDB(t *testing.T, fn func(t *testing.T, db *DB)) {
	tmpdir, _ := os.MkdirTemp("", "nutsdb")
	opt := DefaultOptions
	opt.Dir = tmpdir
	opt.SegmentSize = 8 * 1024

	withDBOption(t, opt, fn)
}

func withRAMIdxDB(t *testing.T, fn func(t *testing.T, db *DB)) {
	tmpdir, _ := os.MkdirTemp("", "nutsdb")
	opt := DefaultOptions
	opt.Dir = tmpdir
	opt.EntryIdxMode = HintKeyAndRAMIdxMode

	withDBOption(t, opt, fn)
}

func TestDB_HintKeyValAndRAMIdxMode_RestartDB(t *testing.T) {
	opts := DefaultOptions
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		key := GetTestBytes(0)
		val := GetTestBytes(0)

		txPut(t, db, bucket, key, val, Persistent, nil, nil)
		txGet(t, db, bucket, key, val, nil)

		db.Close()
		// restart db with HintKeyValAndRAMIdxMode EntryIdxMode
		db, err := Open(db.opt)
		require.NoError(t, err)
		txGet(t, db, bucket, key, val, nil)
	})
}

func TestDB_HintKeyAndRAMIdxMode_RestartDB(t *testing.T) {
	opts := DefaultOptions
	opts.EntryIdxMode = HintKeyAndRAMIdxMode
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		key := GetTestBytes(0)
		val := GetTestBytes(0)

		txPut(t, db, bucket, key, val, Persistent, nil, nil)
		txGet(t, db, bucket, key, val, nil)
		db.Close()

		// restart db with HintKeyAndRAMIdxMode EntryIdxMode
		db, err := Open(db.opt)
		require.NoError(t, err)
		txGet(t, db, bucket, key, val, nil)
	})
}

func TestDB_HintKeyAndRAMIdxMode_LruCache(t *testing.T) {
	opts := DefaultOptions
	opts.EntryIdxMode = HintKeyAndRAMIdxMode
	lruCacheSizes := []int{0, 5000, 10000, 20000}

	for _, lruCacheSize := range lruCacheSizes {
		opts.HintKeyAndRAMIdxCacheSize = lruCacheSize
		runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
			bucket := "bucket"
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			for i := 0; i < 10000; i++ {
				key := []byte(fmt.Sprintf("%10d", i))
				val := []byte(fmt.Sprintf("%10d", i))
				txPut(t, db, bucket, key, val, Persistent, nil, nil)
				txGet(t, db, bucket, key, val, nil)
				txGet(t, db, bucket, key, val, nil)
			}
			db.Close()
		})
	}
}

func TestDB_ChangeMode_RestartDB(t *testing.T) {
	changeModeRestart := func(firstMode EntryIdxMode, secondMode EntryIdxMode) {
		opts := DefaultOptions
		opts.EntryIdxMode = firstMode
		var err error

		runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
			bucket := "bucket"
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txCreateBucket(t, db, DataStructureList, bucket, nil)
			txCreateBucket(t, db, DataStructureSet, bucket, nil)
			txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

			// k-v
			for i := 0; i < 10; i++ {
				txPut(t, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
			}

			// list
			for i := 0; i < 10; i++ {
				txPush(t, db, bucket, GetTestBytes(0), GetTestBytes(i), true, nil, nil)
			}

			err = db.Update(func(tx *Tx) error {
				return tx.LRem(bucket, GetTestBytes(0), 1, GetTestBytes(5))
			})
			require.NoError(t, err)

			for i := 0; i < 2; i++ {
				txPop(t, db, bucket, GetTestBytes(0), GetTestBytes(9-i), nil, true)
			}

			for i := 0; i < 2; i++ {
				txPop(t, db, bucket, GetTestBytes(0), GetTestBytes(i), nil, false)
			}

			// set
			for i := 0; i < 10; i++ {
				txSAdd(t, db, bucket, GetTestBytes(0), GetTestBytes(i), nil, nil)
			}

			for i := 0; i < 3; i++ {
				txSRem(t, db, bucket, GetTestBytes(0), GetTestBytes(i), nil)
			}

			// zset
			for i := 0; i < 10; i++ {
				txZAdd(t, db, bucket, GetTestBytes(0), GetTestBytes(i), float64(i), nil, nil)
			}

			for i := 0; i < 3; i++ {
				txZRem(t, db, bucket, GetTestBytes(0), GetTestBytes(i), nil)
			}

			require.NoError(t, db.Close())

			opts.EntryIdxMode = secondMode
			db, err = Open(opts)
			require.NoError(t, err)

			// k-v
			for i := 0; i < 10; i++ {
				txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), nil)
			}

			// list
			txPop(t, db, bucket, GetTestBytes(0), GetTestBytes(7), nil, true)
			txPop(t, db, bucket, GetTestBytes(0), GetTestBytes(6), nil, true)
			txPop(t, db, bucket, GetTestBytes(0), GetTestBytes(4), nil, true)
			txPop(t, db, bucket, GetTestBytes(0), GetTestBytes(2), nil, false)

			err = db.View(func(tx *Tx) error {
				size, err := tx.LSize(bucket, GetTestBytes(0))
				require.NoError(t, err)
				require.Equal(t, 1, size)
				return nil
			})
			require.NoError(t, err)

			// set
			for i := 0; i < 3; i++ {
				txSIsMember(t, db, bucket, GetTestBytes(0), GetTestBytes(i), false)
			}

			for i := 3; i < 10; i++ {
				txSIsMember(t, db, bucket, GetTestBytes(0), GetTestBytes(i), true)
			}

			// zset
			for i := 0; i < 3; i++ {
				txZScore(t, db, bucket, GetTestBytes(0), GetTestBytes(i), float64(i), ErrSortedSetMemberNotExist)
			}

			for i := 3; i < 10; i++ {
				txZScore(t, db, bucket, GetTestBytes(0), GetTestBytes(i), float64(i), nil)
			}
		})
	}

	// HintKeyValAndRAMIdxMode to HintKeyAndRAMIdxMode
	changeModeRestart(HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode)
	// HintKeyAndRAMIdxMode to HintKeyValAndRAMIdxMode
	changeModeRestart(HintKeyAndRAMIdxMode, HintKeyValAndRAMIdxMode)
}

func TestTx_SmallFile(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 100
	opts.EntryIdxMode = HintKeyAndRAMIdxMode
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		err := db.Update(func(tx *Tx) error {
			for i := 0; i < 100; i++ {
				err := tx.Put(bucket, GetTestBytes(i), GetTestBytes(i), Persistent)
				if err != nil {
					return err
				}
			}
			return nil
		})
		require.Nil(t, err)
		require.NoError(t, db.Close())
		db, _ = Open(opts)

		txGet(t, db, bucket, GetTestBytes(10), GetTestBytes(10), nil)
	})
}

func TestDB_DataStructureBTreeWriteRecordLimit(t *testing.T) {
	opts := DefaultOptions
	limitCount := int64(1000)
	opts.MaxWriteRecordCount = limitCount
	bucket1 := "bucket1"
	bucket2 := "bucket2"
	// Iterate over different EntryIdxModes
	for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		opts.EntryIdxMode = idxMode
		runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket1, nil)
			txCreateBucket(t, db, DataStructureBTree, bucket2, nil)

			// Add limitCount records
			err := db.Update(func(tx *Tx) error {
				for i := 0; i < int(limitCount); i++ {
					key := []byte(strconv.Itoa(i))
					value := []byte(strconv.Itoa(i))
					err = tx.Put(bucket1, key, value, Persistent)
					assertErr(t, err, nil)
				}
				return nil
			})
			require.NoError(t, err)
			// Trigger the limit
			txPut(t, db, bucket1, []byte("key1"), []byte("value1"), Persistent, nil, ErrTxnExceedWriteLimit)
			// Add a key that is within the limit
			txPut(t, db, bucket1, []byte("0"), []byte("000"), Persistent, nil, nil)
			// Delete and add one item
			txDel(t, db, bucket1, []byte("0"), nil)
			txPut(t, db, bucket1, []byte("key1"), []byte("value1"), Persistent, nil, nil)
			// Add an item to another bucket
			txPut(t, db, bucket2, []byte("key2"), []byte("value2"), Persistent, nil, ErrTxnExceedWriteLimit)
			// Delete bucket1
			txDeleteBucket(t, db, DataStructureBTree, bucket1, nil)
			// Add data to bucket2
			err = db.Update(func(tx *Tx) error {
				for i := 0; i < (int(limitCount) - 1); i++ {
					key := []byte(strconv.Itoa(i))
					value := []byte(strconv.Itoa(i))
					err = tx.Put(bucket2, key, value, Persistent)
					assertErr(t, err, nil)
				}
				return nil
			})
			require.NoError(t, err)
			// Add items to bucket2
			txPut(t, db, bucket2, []byte("key1"), []byte("value1"), Persistent, nil, nil)
			txPut(t, db, bucket2, []byte("key2"), []byte("value2"), Persistent, nil, ErrTxnExceedWriteLimit)
		})
	}
}

func TestDB_DataStructureListWriteRecordLimit(t *testing.T) {
	// Set options
	opts := DefaultOptions
	limitCount := int64(1000)
	opts.MaxWriteRecordCount = limitCount
	// Define bucket names
	bucket1 := "bucket1"
	bucket2 := "bucket2"
	// Iterate over EntryIdxMode options
	for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {

		opts.EntryIdxMode = idxMode
		runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureList, bucket1, nil)
			txCreateBucket(t, db, DataStructureList, bucket2, nil)
			// Add limitCount records
			err := db.Update(func(tx *Tx) error {
				for i := 0; i < int(limitCount); i++ {
					key := []byte("0")
					value := []byte(strconv.Itoa(i))
					err = tx.LPush(bucket1, key, value)
					assertErr(t, err, nil)
				}
				return nil
			})
			require.NoError(t, err)
			// Trigger the limit
			txPush(t, db, bucket1, []byte("0"), []byte("value1"), false, nil, ErrTxnExceedWriteLimit)
			// Test LRem
			err = db.Update(func(tx *Tx) error {
				err := tx.LRem(bucket1, []byte("0"), 1, []byte("0"))
				assertErr(t, err, nil)
				return nil
			})
			require.NoError(t, err)
			txPush(t, db, bucket1, []byte("0"), []byte("value1"), true, nil, nil)
			txPush(t, db, bucket1, []byte("0"), []byte("value1"), false, nil, ErrTxnExceedWriteLimit)
			// Test for DataLPopFlag
			err = db.Update(func(tx *Tx) error {
				_, err := tx.LPop(bucket1, []byte("0"))
				assertErr(t, err, nil)
				return nil
			})
			require.NoError(t, err)
			txPush(t, db, bucket1, []byte("0"), []byte("value1"), false, nil, nil)
			txPush(t, db, bucket1, []byte("0"), []byte("value1"), false, nil, ErrTxnExceedWriteLimit)
			// Test for DataLTrimFlag
			err = db.Update(func(tx *Tx) error {
				err := tx.LTrim(bucket1, []byte("0"), 0, 0)
				assertErr(t, err, nil)
				return nil
			})
			require.NoError(t, err)
			err = db.Update(func(tx *Tx) error {
				for i := 0; i < int(limitCount)-2; i++ {
					key := []byte("0")
					value := []byte(strconv.Itoa(i))
					err = tx.RPush(bucket1, key, value)
					assertErr(t, err, nil)
				}
				return nil
			})
			require.NoError(t, err)
			txPush(t, db, bucket1, []byte("0"), []byte("value11"), false, nil, nil)
			txPush(t, db, bucket1, []byte("0"), []byte("value11"), false, nil, ErrTxnExceedWriteLimit)
			// Test for LRemByIndex
			err = db.Update(func(tx *Tx) error {
				err := tx.LRemByIndex(bucket1, []byte("0"), 0, 1, 2)
				assertErr(t, err, nil)
				return nil
			})
			require.NoError(t, err)
			err = db.Update(func(tx *Tx) error {
				for i := 0; i < 2; i++ {
					key := []byte("0")
					value := []byte(strconv.Itoa(i))
					err = tx.RPush(bucket1, key, value)
					assertErr(t, err, nil)
				}
				return nil
			})
			require.NoError(t, err)
			txPush(t, db, bucket2, []byte("0"), []byte("value11"), false, nil, nil)
			txPush(t, db, bucket1, []byte("0"), []byte("value11"), false, nil, ErrTxnExceedWriteLimit)
			// Delete bucket
			txDeleteBucket(t, db, DataStructureList, bucket1, nil)
			// Add data to another bucket
			err = db.Update(func(tx *Tx) error {
				for i := 0; i < int(limitCount)-1; i++ {
					key := []byte(strconv.Itoa(i))
					value := []byte(strconv.Itoa(i))
					err = tx.RPush(bucket2, key, value)
					assertErr(t, err, nil)
				}
				return nil
			})
			require.NoError(t, err)
			txPush(t, db, bucket2, []byte("key1"), []byte("value1"), false, nil, ErrTxnExceedWriteLimit)
		})
	}
}

func TestDB_DataStructureSetWriteRecordLimit(t *testing.T) {
	// Set default options and limitCount.
	opts := DefaultOptions
	limitCount := int64(1000)
	opts.MaxWriteRecordCount = limitCount
	// Define bucket names.
	bucket1 := "bucket1"
	bucket2 := "bucket2"
	// Loop through EntryIdxModes.
	for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		opts.EntryIdxMode = idxMode
		runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureSet, bucket1, nil)
			txCreateBucket(t, db, DataStructureSet, bucket2, nil)

			// Add limitCount records to bucket1.
			err := db.Update(func(tx *Tx) error {
				for i := 0; i < int(limitCount); i++ {
					key := []byte("0")
					value := []byte(strconv.Itoa(i))
					err := tx.SAdd(bucket1, key, value)
					assertErr(t, err, nil)
				}
				return nil
			})
			require.NoError(t, err)
			// Try to add one more item to bucket1 and check for ErrTxnExceedWriteLimit.
			txSAdd(t, db, bucket1, []byte("key1"), []byte("value1"), nil, ErrTxnExceedWriteLimit)
			// Remove one item and add another item to bucket1.
			txSRem(t, db, bucket1, []byte("0"), []byte("0"), nil)
			txSAdd(t, db, bucket1, []byte("key1"), []byte("value1"), nil, nil)
			// Add two more items to bucket1 and check for ErrTxnExceedWriteLimit.
			txSAdd(t, db, bucket1, []byte("key1"), []byte("value1"), nil, nil)
			txSAdd(t, db, bucket1, []byte("key11"), []byte("value11"), nil, ErrTxnExceedWriteLimit)
			// Test for SPOP, SPOP two items from bucket1.
			err = db.Update(func(tx *Tx) error {
				_, err := tx.SPop(bucket1, []byte("0"))
				assertErr(t, err, nil)
				_, err = tx.SPop(bucket1, []byte("key1"))
				assertErr(t, err, nil)
				return nil
			})
			require.NoError(t, err)
			// Add two items to bucket1 and check for ErrTxnExceedWriteLimit.
			txSAdd(t, db, bucket1, []byte("1"), []byte("value1"), nil, nil)
			txSAdd(t, db, bucket1, []byte("1"), []byte("value2"), nil, nil)
			txSAdd(t, db, bucket1, []byte("1"), []byte("value3"), nil, ErrTxnExceedWriteLimit)
			// Delete bucket1.
			txDeleteBucket(t, db, DataStructureSet, bucket1, nil)
			// Add data to bucket2.
			txSAdd(t, db, bucket2, []byte("key1"), []byte("value1"), nil, nil)
			err = db.Update(func(tx *Tx) error {
				for i := 0; i < int(limitCount)-1; i++ {
					value := []byte(strconv.Itoa(i))
					err = tx.SAdd(bucket2, []byte("2"), value)
					assertErr(t, err, nil)
				}
				return nil
			})
			require.NoError(t, err)
			// Try to add one more item to bucket2 and check for ErrTxnExceedWriteLimit.
			txSAdd(t, db, bucket2, []byte("key2"), []byte("value2"), nil, ErrTxnExceedWriteLimit)
		})
	}
}

func TestDB_DataStructureSortedSetWriteRecordLimit(t *testing.T) {
	// Set up options
	opts := DefaultOptions
	limitCount := int64(1000)
	opts.MaxWriteRecordCount = limitCount
	// Set up bucket names and score
	bucket1 := "bucket1"
	bucket2 := "bucket2"
	score := 1.0
	// Iterate over EntryIdxMode options
	for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		opts.EntryIdxMode = idxMode
		runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureSortedSet, bucket1, nil)
			// Add limitCount records
			err := db.Update(func(tx *Tx) error {
				for i := 0; i < int(limitCount); i++ {
					key := []byte("0")
					value := []byte(strconv.Itoa(i))
					err := tx.ZAdd(bucket1, key, score+float64(i), value)
					assertErr(t, err, nil)
				}
				return nil
			})
			require.NoError(t, err)
			// Trigger the limit
			txZAdd(t, db, bucket1, []byte("key1"), []byte("value1"), score, nil, ErrTxnExceedWriteLimit)
			// Delete and add one item
			txZRem(t, db, bucket1, []byte("0"), []byte("0"), nil)
			txZAdd(t, db, bucket1, []byte("key1"), []byte("value1"), score, nil, nil)
			// Add some data is ok
			txZAdd(t, db, bucket1, []byte("key1"), []byte("value1"), score, nil, nil)
			// Trigger the limit
			txZAdd(t, db, bucket1, []byte("key2"), []byte("value2"), score, nil, ErrTxnExceedWriteLimit)
			// Test for ZRemRangeByRank
			err = db.Update(func(tx *Tx) error {
				err := tx.ZRemRangeByRank(bucket1, []byte("0"), 1, 3)
				assert.NoError(t, err)
				return nil
			})
			assert.NoError(t, err)
			txZAdd(t, db, bucket1, []byte("0"), []byte("value1"), score, nil, nil)
			txZAdd(t, db, bucket1, []byte("0"), []byte("value2"), score, nil, nil)
			txZAdd(t, db, bucket1, []byte("0"), []byte("value3"), score+float64(1000), nil, nil)
			// Trigger the limit
			txZAdd(t, db, bucket1, []byte("0"), []byte("value4"), score, nil, ErrTxnExceedWriteLimit)
			// Test for ZPop
			txZPop(t, db, bucket1, []byte("0"), true, []byte("value3"), score+float64(1000), nil)
			txZAdd(t, db, bucket1, []byte("key3"), []byte("value3"), score, nil, nil)
			// Delete bucket
			txDeleteBucket(t, db, DataStructureSortedSet, bucket1, nil)
			// Add data to another bucket
			txCreateBucket(t, db, DataStructureSortedSet, bucket1, nil)
			txCreateBucket(t, db, DataStructureSortedSet, bucket2, nil)
			txZAdd(t, db, bucket2, []byte("key1"), []byte("value1"), score, nil, nil)
			// Add data to bucket1
			err = db.Update(func(tx *Tx) error {
				for i := 0; i < int(limitCount)-1; i++ {
					key := []byte(strconv.Itoa(i))
					value := []byte(strconv.Itoa(i))
					err = tx.ZAdd(bucket1, key, score, value)
					assertErr(t, err, nil)
				}
				return nil
			})
			require.NoError(t, err)
			// Trigger the limit
			txZAdd(t, db, bucket2, []byte("key1"), []byte("value2"), score, nil, ErrTxnExceedWriteLimit)
		})
	}
}

func TestDB_AllDsWriteRecordLimit(t *testing.T) {
	// Set up options
	opts := DefaultOptions
	limitCount := int64(1000)
	opts.MaxWriteRecordCount = limitCount
	// Set up bucket names and score
	bucket1 := "bucket1"
	bucket2 := "bucket2"
	score := 1.0
	// Iterate over EntryIdxMode options
	for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		opts.EntryIdxMode = idxMode
		runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket1, nil)
			txCreateBucket(t, db, DataStructureList, bucket1, nil)
			txCreateBucket(t, db, DataStructureSet, bucket1, nil)
			txCreateBucket(t, db, DataStructureSortedSet, bucket1, nil)
			txCreateBucket(t, db, DataStructureList, bucket2, nil)

			// Add limitCount records
			err := db.Update(func(tx *Tx) error {
				for i := 0; i < int(limitCount); i++ {
					key := []byte(strconv.Itoa(i))
					value := []byte(strconv.Itoa(i))
					err = tx.Put(bucket1, key, value, Persistent)
					assertErr(t, err, nil)
				}
				return nil
			})
			require.NoError(t, err)
			// Trigger the limit
			txPush(t, db, bucket1, []byte("0"), []byte("value1"), false, nil, ErrTxnExceedWriteLimit)
			// Delete item and add one
			txDel(t, db, bucket1, []byte("0"), nil)
			txPush(t, db, bucket1, []byte("0"), []byte("value1"), false, nil, nil)
			// Trigger the limit
			txSAdd(t, db, bucket1, []byte("key1"), []byte("value1"), nil, ErrTxnExceedWriteLimit)
			// Delete item and add one
			txDel(t, db, bucket1, []byte("1"), nil)
			txSAdd(t, db, bucket1, []byte("key1"), []byte("value1"), nil, nil)
			// Trigger the limit
			txZAdd(t, db, bucket1, []byte("key1"), []byte("value1"), score, nil, ErrTxnExceedWriteLimit)
			// Delete item and add one
			txDel(t, db, bucket1, []byte("2"), nil)
			txZAdd(t, db, bucket1, []byte("key1"), []byte("value1"), score, nil, nil)
			// Delete bucket
			txDeleteBucket(t, db, DataStructureSortedSet, bucket1, nil)
			// Add data to another bucket
			txPush(t, db, bucket2, []byte("key1"), []byte("value1"), false, nil, nil)
			// Trigger the limit
			txPush(t, db, bucket2, []byte("key2"), []byte("value2"), false, nil, ErrTxnExceedWriteLimit)
		})
	}
}

func txIncrement(t *testing.T, db *DB, bucket string, key []byte, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.Incr(bucket, key)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txDecrement(t *testing.T, db *DB, bucket string, key []byte, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.Decr(bucket, key)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txIncrementBy(t *testing.T, db *DB, bucket string, key []byte, value int64, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.IncrBy(bucket, key, value)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txDecrementBy(t *testing.T, db *DB, bucket string, key []byte, value int64, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.DecrBy(bucket, key, value)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txPutIfNotExists(t *testing.T, db *DB, bucket string, key, value []byte, expectedErr, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.PutIfNotExists(bucket, key, value, Persistent)
		assertErr(t, err, expectedErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txPutIfExists(t *testing.T, db *DB, bucket string, key, value []byte, expectedErr, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.PutIfExists(bucket, key, value, Persistent)
		assertErr(t, err, expectedErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txValueLen(t *testing.T, db *DB, bucket string, key []byte, expectLength int, expectErr error) {
	err := db.View(func(tx *Tx) error {
		length, err := tx.ValueLen(bucket, key)
		if expectErr != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
		}
		require.EqualValuesf(t, expectLength, length, "err Tx ValueLen. got %s want %s", length, expectLength)
		return nil
	})
	require.NoError(t, err)
}

func txGetSet(t *testing.T, db *DB, bucket string, key, value []byte, expectOldValue []byte, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		oldValue, err := tx.GetSet(bucket, key, value)
		assertErr(t, err, expectErr)
		require.EqualValuesf(t, oldValue, expectOldValue, "err Tx GetSet. got %s want %s", string(oldValue), string(expectOldValue))
		return nil
	})
	require.NoError(t, err)
}

func txGetBit(t *testing.T, db *DB, bucket string, key []byte, offset int, expectVal byte, expectErr error, finalExpectErr error) {
	err := db.View(func(tx *Tx) error {
		value, err := tx.GetBit(bucket, key, offset)
		assertErr(t, err, expectErr)
		require.Equal(t, expectVal, value)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txSetBit(t *testing.T, db *DB, bucket string, key []byte, offset int, value byte, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.SetBit(bucket, key, offset, value)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txGetTTL(t *testing.T, db *DB, bucket string, key []byte, expectedTTL int64, expectedErr error) {
	err := db.View(func(tx *Tx) error {
		ttl, err := tx.GetTTL(bucket, key)
		assertErr(t, err, expectedErr)

		// If diff between expectedTTL and realTTL lesser than 1s, We'll consider as equal
		diff := int(math.Abs(float64(ttl - expectedTTL)))
		assert.LessOrEqual(t, diff, 1)
		return nil
	})
	require.NoError(t, err)
}

func txPersist(t *testing.T, db *DB, bucket string, key []byte, expectedErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.Persist(bucket, key)
		assertErr(t, err, expectedErr)
		return nil
	})
	require.NoError(t, err)
}

func txMSet(t *testing.T, db *DB, bucket string, args [][]byte, ttl uint32, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.MSet(bucket, ttl, args...)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txMGet(t *testing.T, db *DB, bucket string, keys [][]byte, expectValues [][]byte, expectErr error, finalExpectErr error) {
	err := db.View(func(tx *Tx) error {
		values, err := tx.MGet(bucket, keys...)
		assertErr(t, err, expectErr)
		require.EqualValues(t, expectValues, values)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txAppend(t *testing.T, db *DB, bucket string, key, appendage []byte, expectErr error, expectFinalErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.Append(bucket, key, appendage)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, expectFinalErr)
}

func txGetRange(t *testing.T, db *DB, bucket string, key []byte, start, end int, expectVal []byte, expectErr error, expectFinalErr error) {
	err := db.View(func(tx *Tx) error {
		value, err := tx.GetRange(bucket, key, start, end)
		assertErr(t, err, expectErr)
		require.EqualValues(t, expectVal, value)
		return nil
	})
	assertErr(t, err, expectFinalErr)
}
