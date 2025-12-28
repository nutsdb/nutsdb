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
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/testutils"
	"github.com/nutsdb/nutsdb/internal/ttl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	db  *DB
	opt Options
	err error
)

func AssertErr(t *testing.T, err error, expectErr error) {
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
		opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	}
	defer removeDir(opts.Dir)
	db, err := Open(*opts)
	require.NoError(t, err)

	test(t, db)
	if !db.IsClose() {
		require.NoError(t, db.Close())
	}
}

// runNutsDBTestWithMockClock runs a test with a MockClock for deterministic TTL testing.
// The MockClock is initialized with the current system time in milliseconds.
// The test function receives both the DB and the MockClock to allow time manipulation.
func runNutsDBTestWithMockClock(t *testing.T, opts *Options, test func(t *testing.T, db *DB, mc ttl.Clock)) {
	mc := ttl.NewMockClock(time.Now().UnixMilli())
	if opts == nil {
		defaultOpts := DefaultOptions
		opts = &defaultOpts
	}
	opts.Clock = mc
	if opts.Dir == "" {
		opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	}
	defer removeDir(opts.Dir)
	db, err := Open(*opts)
	require.NoError(t, err)

	test(t, db, mc)
	if !db.IsClose() {
		require.NoError(t, db.Close())
	}
}

func txPut(t *testing.T, db *DB, bucket string, key, value []byte, ttl uint32, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err = tx.Put(bucket, key, value, ttl)
		AssertErr(t, err, expectErr)
		return nil
	})
	AssertErr(t, err, finalExpectErr)
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

func txHas(t *testing.T, db *DB, bucket string, key []byte, expectVal bool, expectErr error) {
	err := db.View(func(tx *Tx) error {
		exists, err := tx.Has(bucket, key)
		if expectErr != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
			require.EqualValuesf(t, expectVal, exists, "err Tx Has. got %v want %v", exists, expectVal)
		}
		return nil
	})
	require.NoError(t, err)
}

func txGetAll(t *testing.T, db *DB, bucket string, expectKeys [][]byte, expectValues [][]byte, expectErr error) {
	require.NoError(t, db.View(func(tx *Tx) error {
		keys, values, err := tx.GetAll(bucket)
		if expectErr != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
			n := len(keys)
			for i := 0; i < n; i++ {
				require.Equal(t, expectKeys[i], keys[i])
				require.Equal(t, expectValues[i], values[i])
			}
		}
		return nil
	}))
}

func txDel(t *testing.T, db *DB, bucket string, key []byte, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.Delete(bucket, key)
		AssertErr(t, err, expectErr)
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
		AssertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func txCreateBucket(t *testing.T, db *DB, ds uint16, bucket string, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.NewBucket(ds, bucket)
		AssertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func InitOpt(fileDir string, isRemoveFiles bool) {
	if fileDir == "" {
		fileDir = "/tmp/nutsdbtest"
	}
	if isRemoveFiles {
		files, _ := os.ReadDir(fileDir)
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
		key0 := testutils.GetTestBytes(0)
		val0 := testutils.GetRandomBytes(24)

		// put
		txPut(t, db, bucket, key0, val0, Persistent, nil, nil)
		txGet(t, db, bucket, key0, val0, nil)

		val1 := testutils.GetRandomBytes(24)

		// update
		txPut(t, db, bucket, key0, val1, Persistent, nil, nil)
		txGet(t, db, bucket, key0, val1, nil)

		// del
		txDel(t, db, bucket, key0, nil)
		txGet(t, db, bucket, key0, val1, ErrKeyNotFound)
	})
}

func TestDB_ReopenWithDelete(t *testing.T) {
	opts := &DefaultOptions
	if opts.Dir == "" {
		opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	}
	db, err := Open(*opts)
	require.NoError(t, err)
	defer removeDir(opts.Dir)

	bucket := "bucket"
	txCreateBucket(t, db, DataStructureList, bucket, nil)
	txPush(t, db, bucket, testutils.GetTestBytes(5), testutils.GetTestBytes(0), true, nil, nil)
	txPush(t, db, bucket, testutils.GetTestBytes(5), testutils.GetTestBytes(1), true, nil, nil)
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
	testDir := filepath.Join(t.TempDir(), "testdb_flock")
	opt := DefaultOptions
	opt.Dir = testDir
	db1, err := Open(opt)
	db2, err := Open(db1.opt)

	require.Nil(t, db2)
	require.Equal(t, ErrDirLocked, err)

	err = db1.Close()
	require.NoError(t, err)

	db2, err = Open(db1.opt)
	require.NoError(t, err)
	require.NotNil(t, db2)

	err = db2.flock.Unlock()
	require.NoError(t, err)
	require.False(t, db2.flock.Locked())

	err = db2.Close()
	require.NoError(t, err)
}

func TestDB_DeleteANonExistKey(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		testBucket := "test_bucket"
		txCreateBucket(t, db, DataStructureBTree, testBucket, nil)

		txDel(t, db, testBucket, testutils.GetTestBytes(0), ErrKeyNotFound)
		txPut(t, db, testBucket, testutils.GetTestBytes(1), testutils.GetRandomBytes(24), Persistent, nil, nil)
		txDel(t, db, testBucket, testutils.GetTestBytes(0), ErrKeyNotFound)
	})
}

func TestDB_CheckListExpired(t *testing.T) {
	runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
		testBucket := "test_bucket"
		txCreateBucket(t, db, DataStructureBTree, testBucket, nil)

		txPut(t, db, testBucket, testutils.GetTestBytes(0), testutils.GetTestBytes(1), Persistent, nil, nil)
		txPut(t, db, testBucket, testutils.GetTestBytes(1), testutils.GetRandomBytes(24), 1, nil, nil)

		mc.AdvanceTime(1100 * time.Millisecond)

		db.checkListExpired()

		// this entry still alive
		txGet(t, db, testBucket, testutils.GetTestBytes(0), testutils.GetTestBytes(1), nil)
		// this entry will be deleted
		txGet(t, db, testBucket, testutils.GetTestBytes(1), nil, ErrKeyNotFound)
	})
}

func txLRem(t *testing.T, db *DB, bucket string, key []byte, count int, value []byte, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.LRem(bucket, key, count, value)
		AssertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func txLRemByIndex(t *testing.T, db *DB, bucket string, key []byte, expectErr error, indexes ...int) {
	err := db.Update(func(tx *Tx) error {
		err := tx.LRemByIndex(bucket, key, indexes...)
		AssertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func txSAdd(t *testing.T, db *DB, bucket string, key, value []byte, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.SAdd(bucket, key, value)
		AssertErr(t, err, expectErr)
		return nil
	})
	AssertErr(t, err, finalExpectErr)
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
		AssertErr(t, err, expectErr)
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
		AssertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func txZAdd(t *testing.T, db *DB, bucket string, key, value []byte, score float64, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.ZAdd(bucket, key, score, value)
		AssertErr(t, err, expectErr)
		return nil
	})
	AssertErr(t, err, finalExpectErr)
}

func txZRem(t *testing.T, db *DB, bucket string, key, value []byte, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.ZRem(bucket, key, value)
		AssertErr(t, err, expectErr)
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
		AssertErr(t, err1, finalExpectErr)

		if expectErr == nil {
			require.Equal(t, &SortedSetMember{
				Value: expectVal,
				Score: expectScore,
			}, minMem)
		}
		return err1
	})
	AssertErr(t, err, finalExpectErr)
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

		AssertErr(t, err, expectErr)

		return nil
	})
	AssertErr(t, err, finalExpectErr)
}

func txMPush(t *testing.T, db *DB, bucket string, key []byte, vals [][]byte, isLeft bool, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		var err error

		if isLeft {
			err = tx.LPush(bucket, key, vals...)
		} else {
			err = tx.RPush(bucket, key, vals...)
		}

		AssertErr(t, err, expectErr)

		return nil
	})
	AssertErr(t, err, finalExpectErr)
}

func txPushRaw(t *testing.T, db *DB, bucket string, key, val []byte, isLeft bool, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		var err error

		if isLeft {
			err = tx.LPushRaw(bucket, key, val)
		} else {
			err = tx.RPushRaw(bucket, key, val)
		}

		AssertErr(t, err, expectErr)

		return nil
	})
	AssertErr(t, err, finalExpectErr)
}

func txExpireList(t *testing.T, db *DB, bucket string, key []byte, ttl uint32, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.ExpireList(bucket, key, ttl)
		AssertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func txGetListTTL(t *testing.T, db *DB, bucket string, key []byte, expectVal uint32, expectErr error) {
	err := db.View(func(tx *Tx) error {
		ttl, err := tx.GetListTTL(bucket, key)
		AssertErr(t, err, expectErr)
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
		AssertErr(t, err, expectErr)
		require.Equal(t, expectLen, len(keys))
		return nil
	})
	require.NoError(t, err)
}

func txLRange(t *testing.T, db *DB, bucket string, key []byte, start, end, expectLen int, expectVal [][]byte, expectErr error) {
	err := db.View(func(tx *Tx) error {
		list, err := tx.LRange(bucket, key, start, end)
		AssertErr(t, err, expectErr)

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
		AssertErr(t, err, expectErr)

		require.Equal(t, expectVal, size)

		return nil
	})
	require.NoError(t, err)
}

func txLTrim(t *testing.T, db *DB, bucket string, key []byte, start int, end int, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.LTrim(bucket, key, start, end)
		AssertErr(t, err, expectErr)
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
		txGet(t, db, bucket, testutils.GetTestBytes(0), nil, ErrKeyNotFound)
		txPut(t, db, bucket, testutils.GetTestBytes(1), testutils.GetRandomBytes(24), Persistent, nil, nil)
		txGet(t, db, bucket, testutils.GetTestBytes(0), nil, ErrKeyNotFound)
	})
}

func TestDB_Backup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		backUpDir := filepath.Join(t.TempDir(), "nutsdb-backup")
		require.NoError(t, db.Backup(backUpDir))
	})
}

func TestDB_BackupTarGZ(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		backUpFile := filepath.Join(t.TempDir(), "nutsdb-backup", "backup.tar.gz")

		_ = os.MkdirAll(filepath.Dir(backUpFile), os.ModePerm)
		f, err := os.Create(backUpFile)
		require.NoError(t, err)
		require.NoError(t, db.BackupTarGZ(f))
	})
}

// TestDB_Close_CompleteShutdownFlow tests the complete shutdown flow
func TestDB_Close_CompleteShutdownFlow(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "test-close-complete")
	defer func() { _ = os.RemoveAll(opts.Dir) }()

	db, err := Open(opts)
	require.NoError(t, err)
	require.NotNil(t, db)

	// Create a bucket and add some data
	bucket := "test_bucket"
	err = db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, bucket)
	})
	require.NoError(t, err)

	// Add some test data
	for i := 0; i < 10; i++ {
		key := testutils.GetTestBytes(i)
		val := testutils.GetRandomBytes(100)
		err = db.Update(func(tx *Tx) error {
			return tx.Put(bucket, key, val, Persistent)
		})
		require.NoError(t, err)
	}

	// Verify database is operational before close
	require.False(t, db.IsClose())
	require.False(t, db.statusMgr.isClosingOrClosed())

	// Close the database
	err = db.Close()
	require.NoError(t, err)

	// Verify database is closed
	require.True(t, db.IsClose())
	require.True(t, db.statusMgr.isClosed())

	// Verify resources are released
	require.Nil(t, db.ActiveFile)
	require.Nil(t, db.Index)
	require.Nil(t, db.fm)
	require.Nil(t, db.commitBuffer)

	// Verify file lock is released
	require.False(t, db.flock.Locked())
}

// TestDB_Close_WithActiveTxs tests closing database with active transactions
func TestDB_Close_WithActiveTxs(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "test-close-active-txs")
	defer func() { _ = os.RemoveAll(opts.Dir) }()

	db, err := Open(opts)
	require.NoError(t, err)
	require.NotNil(t, db)

	bucket := "test_bucket"
	err = db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, bucket)
	})
	require.NoError(t, err)

	// Start a long-running transaction
	txStarted := make(chan struct{})
	txCompleted := make(chan struct{})
	txErr := make(chan error, 1)

	go func() {
		err := db.Update(func(tx *Tx) error {
			close(txStarted)
			// Simulate some work
			time.Sleep(500 * time.Millisecond)
			return tx.Put(bucket, []byte("key1"), []byte("value1"), Persistent)
		})
		txErr <- err
		close(txCompleted)
	}()

	// Wait for transaction to start
	<-txStarted

	// Verify transaction is active
	activeTxCount := db.transactionMgr.GetActiveTxCount()
	require.Greater(t, activeTxCount, int64(0), "Should have active transactions")

	// Close database (should wait for active transaction)
	closeStartTime := time.Now()
	err = db.Close()
	closeDuration := time.Since(closeStartTime)

	require.NoError(t, err)

	// Verify transaction completed
	select {
	case err := <-txErr:
		// Transaction should complete successfully or be rejected
		// If it completes, it should be before close started
		// If rejected, it should get ErrDBClosed
		if err != nil {
			require.ErrorIs(t, err, ErrDBClosed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Transaction did not complete")
	}

	// Verify close waited for transaction (should take at least 500ms)
	require.Greater(t, closeDuration, 400*time.Millisecond, "Close should wait for active transactions")

	// Verify database is closed
	require.True(t, db.IsClose())
}

// TestDB_Close_RejectsNewTxsDuringShutdown tests that new transactions are rejected during shutdown
func TestDB_Close_RejectsNewTxsDuringShutdown(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "test-close-reject-txs")
	defer func() { _ = os.RemoveAll(opts.Dir) }()

	db, err := Open(opts)
	require.NoError(t, err)
	require.NotNil(t, db)

	bucket := "test_bucket"
	err = db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, bucket)
	})
	require.NoError(t, err)

	// Start a long-running transaction to delay close
	txStarted := make(chan struct{})
	go func() {
		_ = db.Update(func(tx *Tx) error {
			close(txStarted)
			time.Sleep(1 * time.Second)
			return nil
		})
	}()

	<-txStarted

	// Start closing in background
	closeStarted := make(chan struct{})
	closeDone := make(chan error, 1)
	go func() {
		close(closeStarted)
		closeDone <- db.Close()
	}()

	<-closeStarted

	// Give close time to transition to Closing state
	time.Sleep(100 * time.Millisecond)

	// Try to start new transactions during shutdown
	for i := 0; i < 5; i++ {
		err := db.Update(func(tx *Tx) error {
			return tx.Put(bucket, []byte(fmt.Sprintf("key%d", i)), []byte("value"), Persistent)
		})
		// Should get ErrDBClosed
		if err != nil {
			require.ErrorIs(t, err, ErrDBClosed, "New transactions should be rejected during shutdown")
		}
	}

	// Wait for close to complete
	err = <-closeDone
	require.NoError(t, err)
}

// TestDB_Close_Timeout tests timeout handling during close
func TestDB_Close_Timeout(t *testing.T) {
	r := require.New(t)
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "test-close-timeout")
	defer func() { _ = os.RemoveAll(opts.Dir) }()

	db, err := Open(opts)
	require.NoError(t, err)
	require.NotNil(t, db)

	// Configure a short shutdown timeout
	db.statusMgr.config.ShutdownTimeout = 1 * time.Second

	bucket := "test_bucket"
	err = db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, bucket)
	})
	require.NoError(t, err)

	// Start a long-running transaction that exceeds timeout
	txStarted := make(chan struct{})
	txDone := make(chan struct{})

	go func() {
		_ = db.Update(func(tx *Tx) error {
			close(txStarted)
			// Sleep longer than shutdown timeout
			time.Sleep(3 * time.Second)
			return tx.Put(bucket, []byte("key1"), []byte("value"), Persistent)
		})
		close(txDone)
	}()

	// Wait for transaction to start
	<-txStarted
	time.Sleep(100 * time.Millisecond) // Ensure tx is active

	// Close should timeout and force shutdown
	closeStartTime := time.Now()
	err = db.Close()
	closeDuration := time.Since(closeStartTime)

	require.Error(t, err, "Close should surface shutdown timeout")
	// Close should complete within reasonable time (timeout + buffer)
	// The important thing is it doesn't hang forever
	require.Less(t, closeDuration, 3*time.Second, "Close should timeout and not wait forever")

	// Database should be closed even if timeout occurred
	require.True(t, db.IsClose())
	require.True(t, db.statusMgr.isClosed())

	// Wait for background transaction to finish
	select {
	case <-txDone:
	case <-time.After(5 * time.Second):
		// Transaction may still be running, that's ok
	}

	// close all active file and bucket manager and flock,
	// let there is no opening file in temp dir.
	r.NoError(db.flock.Unlock())
	if db.ActiveFile != nil && db.ActiveFile.rwManager != nil {
		r.NoError(db.ActiveFile.rwManager.Close())
	}
	if db.bucketMgr != nil {
		r.NoError(db.bucketMgr.Close())
	}
}

// TestDB_Close_ConcurrentCalls tests concurrent close calls
// Note: This test may reveal race conditions in the release() method when called concurrently.
// In practice, applications should not call Close() concurrently from multiple goroutines.
func TestDB_Close_ConcurrentCalls(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "test-close-concurrent")
	defer func() { _ = os.RemoveAll(opts.Dir) }()

	db, err := Open(opts)
	require.NoError(t, err)
	require.NotNil(t, db)

	bucket := "test_bucket"
	err = db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, bucket)
	})
	require.NoError(t, err)

	// Call Close() concurrently from multiple goroutines
	// Note: This is testing the safety of concurrent Close() calls,
	// though in practice this should be avoided
	numGoroutines := 5
	var wg sync.WaitGroup
	errors := make([]error, numGoroutines)

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			// Add small delay to reduce contention
			time.Sleep(time.Duration(idx) * 10 * time.Millisecond)
			errors[idx] = db.Close()
		}(i)
	}

	wg.Wait()

	// At least one close should succeed or return ErrDBClosed
	hasSuccess := false
	hasClosedErr := false

	for _, err := range errors {
		switch err {
		case nil:
			hasSuccess = true
		case ErrDBClosed:
			hasClosedErr = true
		default:
			t.Logf("Unexpected error: %v", err)
		}
	}

	// Either we got a success or all got ErrDBClosed
	require.True(t, hasSuccess || hasClosedErr, "Should have at least one successful close or ErrDBClosed")

	// Database should be closed
	require.True(t, db.IsClose())
	require.True(t, db.statusMgr.isClosed())
}

// TestDB_Close_IdempotentCalls tests that multiple sequential close calls are safe
func TestDB_Close_IdempotentCalls(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "test-close-idempotent")
	defer func() { _ = os.RemoveAll(opts.Dir) }()

	db, err := Open(opts)
	require.NoError(t, err)
	require.NotNil(t, db)

	// First close should succeed
	err = db.Close()
	require.NoError(t, err)
	require.True(t, db.IsClose())

	// Subsequent closes should return ErrDBClosed
	for i := 0; i < 5; i++ {
		err = db.Close()
		require.Equal(t, ErrDBClosed, err, "Subsequent Close() calls should return ErrDBClosed")
		require.True(t, db.IsClose())
	}
}

// TestDB_Close_ComponentShutdownOrder tests that components are shut down in reverse order
func TestDB_Close_ComponentShutdownOrder(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "test-close-order")
	defer func() { _ = os.RemoveAll(opts.Dir) }()

	db, err := Open(opts)
	require.NoError(t, err)
	require.NotNil(t, db)

	err = db.Close()
	require.NoError(t, err)

	// Verify database is closed
	require.True(t, db.statusMgr.isClosed())
}

// TestDB_Close_WithMergeInProgress tests closing database during merge operation
func TestDB_Close_WithMergeInProgress(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "test-close-merge")
	opts.SegmentSize = 1024 // Small segment to trigger merge easily
	defer func() { _ = os.RemoveAll(opts.Dir) }()

	db, err := Open(opts)
	require.NoError(t, err)
	require.NotNil(t, db)

	bucket := "test_bucket"
	err = db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, bucket)
	})
	require.NoError(t, err)

	// Add enough data to potentially trigger merge
	for i := 0; i < 100; i++ {
		key := testutils.GetTestBytes(i)
		val := testutils.GetRandomBytes(100)
		err = db.Update(func(tx *Tx) error {
			return tx.Put(bucket, key, val, Persistent)
		})
		require.NoError(t, err)
	}

	// Try to trigger merge (may or may not actually start)
	go func() {
		_ = db.Merge()
	}()

	// Give merge a moment to potentially start
	time.Sleep(100 * time.Millisecond)

	// Close should handle merge gracefully
	err = db.Close()
	require.NoError(t, err)

	// Verify database is closed
	require.True(t, db.IsClose())
	require.True(t, db.statusMgr.isClosed())
}

// TestDB_Close_ResourceCleanup tests that all resources are properly cleaned up
func TestDB_Close_ResourceCleanup(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "test-close-cleanup")
	defer func() { _ = os.RemoveAll(opts.Dir) }()

	db, err := Open(opts)
	require.NoError(t, err)
	require.NotNil(t, db)

	bucket := "test_bucket"
	err = db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, bucket)
	})
	require.NoError(t, err)

	// Add some data
	for i := 0; i < 10; i++ {
		key := testutils.GetTestBytes(i)
		val := testutils.GetRandomBytes(100)
		err = db.Update(func(tx *Tx) error {
			return tx.Put(bucket, key, val, Persistent)
		})
		require.NoError(t, err)
	}

	// Close database
	err = db.Close()
	require.NoError(t, err)

	// Verify all resources are released
	require.Nil(t, db.ActiveFile, "ActiveFile should be nil")
	require.Nil(t, db.Index, "Index should be nil")
	require.Nil(t, db.fm, "FileManager should be nil")
	require.Nil(t, db.commitBuffer, "commitBuffer should be nil")

	// Verify file lock is released
	require.False(t, db.flock.Locked(), "File lock should be released")

	// Verify we can open the database again (proves lock is released)
	db2, err := Open(opts)
	require.NoError(t, err)
	require.NotNil(t, db2)

	// Verify data is still accessible
	err = db2.View(func(tx *Tx) error {
		val, err := tx.Get(bucket, testutils.GetTestBytes(0))
		require.NoError(t, err)
		require.NotNil(t, val)
		return nil
	})
	require.NoError(t, err)

	// Clean up
	err = db2.Close()
	require.NoError(t, err)
}

// TestDB_Close_ErrorHandling tests error handling during component shutdown
func TestDB_Close_ErrorHandling(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "test-close-errors")
	defer func() { _ = os.RemoveAll(opts.Dir) }()

	db, err := Open(opts)
	require.NoError(t, err)
	require.NotNil(t, db)

	// Close should handle errors gracefully and continue shutting down other components
	err = db.Close()
	require.NoError(t, err)

	// Even if there were errors, database should be in closed state
	require.True(t, db.IsClose())
	require.True(t, db.statusMgr.isClosed())
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
		txPut(t, db, bucket, testutils.GetTestBytes(0), testutils.GetRandomBytes(24), Persistent, nil, nil)

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
				err := tx.Put(bucket, testutils.GetTestBytes(i), testutils.GetRandomBytes(1024), Persistent)
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
		key := testutils.GetTestBytes(0)
		val := testutils.GetTestBytes(0)
		txPut(t, db, bucket, key, val, Persistent, nil, nil)
		txGet(t, db, bucket, key, val, nil)

		txDeleteBucket(t, db, DataStructureBTree, bucket, nil)
		txPut(t, db, bucket, key, val, Persistent, ErrBucketNotFound, nil)
	})
}

func withDBOption(t *testing.T, opt Options, fn func(t *testing.T, db *DB)) {
	db, err := Open(opt)
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(db.opt.Dir)
		_ = db.Close()
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

		key := testutils.GetTestBytes(0)
		val := testutils.GetTestBytes(0)

		txPut(t, db, bucket, key, val, Persistent, nil, nil)
		txGet(t, db, bucket, key, val, nil)

		_ = db.Close()
		// restart db with HintKeyValAndRAMIdxMode EntryIdxMode
		var err error
		db, err = Open(db.opt)
		require.NoError(t, err)
		txGet(t, db, bucket, key, val, nil)
		// TODO:
		// I don't know why if I add this close here the cleanup will OK?
		require.NoError(t, db.Close())
	})
}

func TestDB_HintKeyAndRAMIdxMode_RestartDB(t *testing.T) {
	opts := DefaultOptions
	opts.EntryIdxMode = HintKeyAndRAMIdxMode
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		key := testutils.GetTestBytes(0)
		val := testutils.GetTestBytes(0)

		txPut(t, db, bucket, key, val, Persistent, nil, nil)
		txGet(t, db, bucket, key, val, nil)
		_ = db.Close()

		// restart db with HintKeyAndRAMIdxMode EntryIdxMode
		db, err := Open(db.opt)
		require.NoError(t, err)
		txGet(t, db, bucket, key, val, nil)
		// TODO:
		// I don't know why if I add this close here the cleanup will OK?
		require.NoError(t, db.Close())
	})
}

// this test case will cause many times.
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
			_ = db.Close()
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
			r := require.New(t)
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			txCreateBucket(t, db, DataStructureList, bucket, nil)
			txCreateBucket(t, db, DataStructureSet, bucket, nil)
			txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

			// k-v
			for i := 0; i < 10; i++ {
				txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
			}

			// list
			for i := 0; i < 10; i++ {
				txPush(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(i), true, nil, nil)
			}

			err = db.Update(func(tx *Tx) error {
				return tx.LRem(bucket, testutils.GetTestBytes(0), 1, testutils.GetTestBytes(5))
			})
			require.NoError(t, err)

			for i := 0; i < 2; i++ {
				txPop(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(9-i), nil, true)
			}

			for i := 0; i < 2; i++ {
				txPop(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(i), nil, false)
			}

			// set
			for i := 0; i < 10; i++ {
				txSAdd(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(i), nil, nil)
			}

			for i := 0; i < 3; i++ {
				txSRem(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(i), nil)
			}

			// zset
			for i := 0; i < 10; i++ {
				txZAdd(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(i), float64(i), nil, nil)
			}

			for i := 0; i < 3; i++ {
				txZRem(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(i), nil)
			}

			require.NoError(t, db.Close())

			opts.EntryIdxMode = secondMode
			db, err = Open(opts)
			require.NoError(t, err)

			// k-v
			for i := 0; i < 10; i++ {
				txGet(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), nil)
			}

			// list
			txPop(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(7), nil, true)
			txPop(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(6), nil, true)
			txPop(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(4), nil, true)
			txPop(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(2), nil, false)

			err = db.View(func(tx *Tx) error {
				size, err := tx.LSize(bucket, testutils.GetTestBytes(0))
				require.NoError(t, err)
				require.Equal(t, 1, size)
				return nil
			})
			require.NoError(t, err)

			// set
			for i := 0; i < 3; i++ {
				txSIsMember(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(i), false)
			}

			for i := 3; i < 10; i++ {
				txSIsMember(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(i), true)
			}

			// zset
			for i := 0; i < 3; i++ {
				txZScore(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(i), float64(i), ErrSortedSetMemberNotExist)
			}

			for i := 3; i < 10; i++ {
				txZScore(t, db, bucket, testutils.GetTestBytes(0), testutils.GetTestBytes(i), float64(i), nil)
			}
			r.NoError(db.Close())
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
		r := require.New(t)
		bucket := "bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		err := db.Update(func(tx *Tx) error {
			for i := 0; i < 100; i++ {
				err := tx.Put(bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent)
				if err != nil {
					return err
				}
			}
			return nil
		})
		r.Nil(err)
		r.NoError(db.Close())
		db, _ = Open(opts)

		txGet(t, db, bucket, testutils.GetTestBytes(10), testutils.GetTestBytes(10), nil)
		r.NoError(db.Close())
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
					AssertErr(t, err, nil)
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
					AssertErr(t, err, nil)
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
					AssertErr(t, err, nil)
				}
				return nil
			})
			require.NoError(t, err)
			// Trigger the limit
			txPush(t, db, bucket1, []byte("0"), []byte("value1"), false, nil, ErrTxnExceedWriteLimit)
			// Test LRem
			err = db.Update(func(tx *Tx) error {
				err := tx.LRem(bucket1, []byte("0"), 1, []byte("0"))
				AssertErr(t, err, nil)
				return nil
			})
			require.NoError(t, err)
			txPush(t, db, bucket1, []byte("0"), []byte("value1"), true, nil, nil)
			txPush(t, db, bucket1, []byte("0"), []byte("value1"), false, nil, ErrTxnExceedWriteLimit)
			// Test for DataLPopFlag
			err = db.Update(func(tx *Tx) error {
				_, err := tx.LPop(bucket1, []byte("0"))
				AssertErr(t, err, nil)
				return nil
			})
			require.NoError(t, err)
			txPush(t, db, bucket1, []byte("0"), []byte("value1"), false, nil, nil)
			txPush(t, db, bucket1, []byte("0"), []byte("value1"), false, nil, ErrTxnExceedWriteLimit)
			// Test for DataLTrimFlag
			err = db.Update(func(tx *Tx) error {
				err := tx.LTrim(bucket1, []byte("0"), 0, 0)
				AssertErr(t, err, nil)
				return nil
			})
			require.NoError(t, err)
			err = db.Update(func(tx *Tx) error {
				for i := 0; i < int(limitCount)-2; i++ {
					key := []byte("0")
					value := []byte(strconv.Itoa(i))
					err = tx.RPush(bucket1, key, value)
					AssertErr(t, err, nil)
				}
				return nil
			})
			require.NoError(t, err)
			txPush(t, db, bucket1, []byte("0"), []byte("value11"), false, nil, nil)
			txPush(t, db, bucket1, []byte("0"), []byte("value11"), false, nil, ErrTxnExceedWriteLimit)
			// Test for LRemByIndex
			err = db.Update(func(tx *Tx) error {
				err := tx.LRemByIndex(bucket1, []byte("0"), 0, 1, 2)
				AssertErr(t, err, nil)
				return nil
			})
			require.NoError(t, err)
			err = db.Update(func(tx *Tx) error {
				for i := 0; i < 2; i++ {
					key := []byte("0")
					value := []byte(strconv.Itoa(i))
					err = tx.RPush(bucket1, key, value)
					AssertErr(t, err, nil)
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
					AssertErr(t, err, nil)
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
					AssertErr(t, err, nil)
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
				AssertErr(t, err, nil)
				_, err = tx.SPop(bucket1, []byte("key1"))
				AssertErr(t, err, nil)
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
					AssertErr(t, err, nil)
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
					AssertErr(t, err, nil)
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
					AssertErr(t, err, nil)
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
					AssertErr(t, err, nil)
				}
				return nil
			})

			require.NoError(t, err)
			// Trigger the limit
			txPush(t, db, bucket1, []byte("0"), []byte("value1"), false, nil, ErrTxnExceedWriteLimit)
			//  Delete item and add one
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
		AssertErr(t, err, expectErr)
		return nil
	})
	AssertErr(t, err, finalExpectErr)
}

func txDecrement(t *testing.T, db *DB, bucket string, key []byte, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.Decr(bucket, key)
		AssertErr(t, err, expectErr)
		return nil
	})
	AssertErr(t, err, finalExpectErr)
}

func txIncrementBy(t *testing.T, db *DB, bucket string, key []byte, value int64, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.IncrBy(bucket, key, value)
		AssertErr(t, err, expectErr)
		return nil
	})
	AssertErr(t, err, finalExpectErr)
}

func txDecrementBy(t *testing.T, db *DB, bucket string, key []byte, value int64, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.DecrBy(bucket, key, value)
		AssertErr(t, err, expectErr)
		return nil
	})
	AssertErr(t, err, finalExpectErr)
}

func txPutIfNotExists(t *testing.T, db *DB, bucket string, key, value []byte, expectedErr, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.PutIfNotExists(bucket, key, value, Persistent)
		AssertErr(t, err, expectedErr)
		return nil
	})
	AssertErr(t, err, finalExpectErr)
}

func txPutIfExists(t *testing.T, db *DB, bucket string, key, value []byte, expectedErr, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.PutIfExists(bucket, key, value, Persistent)
		AssertErr(t, err, expectedErr)
		return nil
	})
	AssertErr(t, err, finalExpectErr)
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
		AssertErr(t, err, expectErr)
		require.EqualValuesf(t, oldValue, expectOldValue, "err Tx GetSet. got %s want %s", string(oldValue), string(expectOldValue))
		return nil
	})
	require.NoError(t, err)
}

func txGetBit(t *testing.T, db *DB, bucket string, key []byte, offset int, expectVal byte, expectErr error, finalExpectErr error) {
	err := db.View(func(tx *Tx) error {
		value, err := tx.GetBit(bucket, key, offset)
		AssertErr(t, err, expectErr)
		require.Equal(t, expectVal, value)
		return nil
	})
	AssertErr(t, err, finalExpectErr)
}

func txSetBit(t *testing.T, db *DB, bucket string, key []byte, offset int, value byte, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.SetBit(bucket, key, offset, value)
		AssertErr(t, err, expectErr)
		return nil
	})
	AssertErr(t, err, finalExpectErr)
}

func txGetTTL(t *testing.T, db *DB, bucket string, key []byte, expectedTTL int64, expectedErr error) {
	err := db.View(func(tx *Tx) error {
		ttl, err := tx.GetTTL(bucket, key)
		AssertErr(t, err, expectedErr)

		// If diff between expectedTTL and realTTL lesser than 1s, We'll consider as equal
		// diff := int(math.Abs(float64(ttl - expectedTTL)))
		assert.LessOrEqual(t, ttl, expectedTTL)
		return nil
	})
	require.NoError(t, err)
}

func txPersist(t *testing.T, db *DB, bucket string, key []byte, expectedErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.Persist(bucket, key)
		AssertErr(t, err, expectedErr)
		return nil
	})
	require.NoError(t, err)
}

func txMSet(t *testing.T, db *DB, bucket string, args [][]byte, ttl uint32, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.MSet(bucket, ttl, args...)
		AssertErr(t, err, expectErr)
		return nil
	})
	AssertErr(t, err, finalExpectErr)
}

func txMGet(t *testing.T, db *DB, bucket string, keys [][]byte, expectValues [][]byte, expectErr error, finalExpectErr error) {
	err := db.View(func(tx *Tx) error {
		values, err := tx.MGet(bucket, keys...)
		AssertErr(t, err, expectErr)
		require.EqualValues(t, expectValues, values)
		return nil
	})
	AssertErr(t, err, finalExpectErr)
}

func txAppend(t *testing.T, db *DB, bucket string, key, appendage []byte, expectErr error, expectFinalErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.Append(bucket, key, appendage)
		AssertErr(t, err, expectErr)
		return nil
	})
	AssertErr(t, err, expectFinalErr)
}

func txGetRange(t *testing.T, db *DB, bucket string, key []byte, start, end int, expectVal []byte, expectErr error, expectFinalErr error) {
	err := db.View(func(tx *Tx) error {
		value, err := tx.GetRange(bucket, key, start, end)
		AssertErr(t, err, expectErr)
		require.EqualValues(t, expectVal, value)
		return nil
	})
	AssertErr(t, err, expectFinalErr)
}

func TestDB_HintFileFastRecovery(t *testing.T) {
	bucket := "bucket"
	opts := DefaultOptions
	opts.SegmentSize = KB
	opts.Dir = filepath.Join(t.TempDir(), "test-hintfile-recovery")
	opts.EnableHintFile = true

	// Clean the test directory at the start
	removeDir(opts.Dir)

	for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		opts.EntryIdxMode = idxMode

		// Create a database with some data
		db, err := Open(opts)
		require.NoError(t, err)
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		// Add some data
		n := 500
		for i := 0; i < n; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		// Perform merge to create hint files
		require.NoError(t, db.Merge())

		// Close the database
		require.NoError(t, db.Close())

		// Reopen the database - it should use hint files for fast recovery
		db, err = Open(opts)
		require.NoError(t, err)

		// Verify all data is correctly recovered
		for i := 0; i < n; i++ {
			txGet(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), nil)
		}

		// Verify record count
		dbCnt, err := db.getRecordCount()
		require.NoError(t, err)
		require.Equal(t, int64(n), dbCnt)

		require.NoError(t, db.Close())
		removeDir(opts.Dir)
	}
}

func TestDB_HintFileMissingFallback(t *testing.T) {
	bucket := "bucket"
	opts := DefaultOptions
	opts.SegmentSize = KB
	opts.Dir = filepath.Join(t.TempDir(), "test-hintfile-missing")
	opts.EnableHintFile = true

	// Clean the test directory at the start
	removeDir(opts.Dir)

	// Create a database with some data
	db, err := Open(opts)
	require.NoError(t, err)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	// Add some data
	n := 300
	for i := 0; i < n; i++ {
		txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
	}

	// Perform merge to create hint files
	require.NoError(t, db.Merge())

	// Close the database
	require.NoError(t, db.Close())

	// Remove hint files to simulate missing hint files
	fileIDs := enumerateDataFilesInDir(opts.Dir)
	for _, fileID := range fileIDs {
		hintPath := getHintPath(fileID, opts.Dir)
		_ = os.Remove(hintPath)
	}

	// Reopen the database - it should fall back to scanning data files
	db, err = Open(opts)
	require.NoError(t, err)

	// Verify all data is correctly recovered
	for i := 0; i < n; i++ {
		txGet(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), nil)
	}

	// Verify record count
	dbCnt, err := db.getRecordCount()
	require.NoError(t, err)
	require.Equal(t, int64(n), dbCnt)

	require.NoError(t, db.Close())
	removeDir(opts.Dir)
}

// enumerateDataFilesInDir returns all data file IDs in the directory
func enumerateDataFilesInDir(dir string) []int64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	var fileIDs []int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		// Check if it's a data file (ends with .data)
		if strings.HasSuffix(name, DataSuffix) {
			// Extract file ID from filename
			idStr := strings.TrimSuffix(name, DataSuffix)
			if id, err := strconv.ParseInt(idStr, 10, 64); err == nil {
				fileIDs = append(fileIDs, id)
			}
		}
	}
	return fileIDs
}

func TestDB_HintFileCorruptedFallback(t *testing.T) {
	bucket := "bucket"
	opts := DefaultOptions
	opts.SegmentSize = KB
	opts.Dir = filepath.Join(t.TempDir(), "test-hintfile-corrupted")
	opts.EnableHintFile = true

	// Clean the test directory at the start
	removeDir(opts.Dir)

	// Create a database with some data
	db, err := Open(opts)
	require.NoError(t, err)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	// Add some data
	n := 200
	for i := 0; i < n; i++ {
		txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
	}

	// Perform merge to create hint files
	require.NoError(t, db.Merge())

	// Close the database
	require.NoError(t, db.Close())

	// Corrupt hint files to simulate corrupted hint files
	// Wait a moment to ensure all files are properly written and flushed to disk
	time.Sleep(200 * time.Millisecond)

	// Get file IDs and validate they exist before corruption
	fileIDs := enumerateDataFilesInDir(opts.Dir)
	require.NotEmpty(t, fileIDs, "Should have at least one data file after merge")

	corruptedFiles := 0
	for _, fileID := range fileIDs {
		hintPath := getHintPath(fileID, opts.Dir)

		// Check if hint file exists before corrupting it
		if stat, err := os.Stat(hintPath); err == nil {
			t.Logf("Found hint file %s (size: %d bytes)", hintPath, stat.Size())

			// Verify file is readable before corruption
			if originalData, err := os.ReadFile(hintPath); err == nil {
				t.Logf("Original hint file size: %d bytes", len(originalData))
				require.Greater(t, len(originalData), 0, "Hint file should not be empty")

				// Write garbage data to corrupt the file
				corruptionData := []byte{0xFF, 0xFF, 0xFF, 0xFD, 0xFE, 0xFF} // Different pattern
				err := os.WriteFile(hintPath, corruptionData, 0644)
				require.NoError(t, err)

				// Verify corruption was successful
				if corruptedData, err := os.ReadFile(hintPath); err == nil {
					t.Logf("Corrupted hint file %s, new size: %d bytes", hintPath, len(corruptedData))
					require.NotEqual(t, originalData, corruptedData, "File should be corrupted")
				}
				corruptedFiles++
			} else {
				t.Logf("Warning: Could not read hint file %s: %v", hintPath, err)
			}
		} else {
			t.Logf("Hint file %s does not exist, skipping corruption", hintPath)
		}
	}

	t.Logf("Corrupted %d hint files out of %d data files", corruptedFiles, len(fileIDs))
	require.Greater(t, corruptedFiles, 0, "Should have corrupted at least one hint file")

	// Reopen the database - it should fall back to scanning data files
	db, err = Open(opts)
	require.NoError(t, err)

	// Verify all data is correctly recovered
	for i := 0; i < n; i++ {
		txGet(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), nil)
	}

	// Verify record count
	dbCnt, err := db.getRecordCount()
	require.NoError(t, err)
	require.Equal(t, int64(n), dbCnt)

	require.NoError(t, db.Close())
	removeDir(opts.Dir)
}

func TestDB_HintFileDifferentEntryIdxModes(t *testing.T) {
	bucket := "bucket"
	opts := DefaultOptions
	opts.SegmentSize = KB
	opts.Dir = filepath.Join(t.TempDir(), "test-hintfile-modes")
	opts.EnableHintFile = true

	// Clean the test directory at the start
	removeDir(opts.Dir)

	// Test HintKeyValAndRAMIdxMode
	opts.EntryIdxMode = HintKeyValAndRAMIdxMode

	db, err := Open(opts)
	require.NoError(t, err)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	// Add some data
	n := 100
	for i := 0; i < n; i++ {
		txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
	}

	// Perform merge to create hint files
	require.NoError(t, db.Merge())

	// Close the database
	require.NoError(t, db.Close())

	// Reopen the database - it should use hint files for fast recovery
	db, err = Open(opts)
	require.NoError(t, err)

	// Verify all data is correctly recovered
	for i := 0; i < n; i++ {
		txGet(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), nil)
	}

	require.NoError(t, db.Close())
	removeDir(opts.Dir)

	// Test HintKeyAndRAMIdxMode
	opts.EntryIdxMode = HintKeyAndRAMIdxMode

	db, err = Open(opts)
	require.NoError(t, err)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	// Add some data
	for i := 0; i < n; i++ {
		txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
	}

	// Perform merge to create hint files
	require.NoError(t, db.Merge())

	// Close the database
	require.NoError(t, db.Close())

	// Reopen the database - it should use hint files for fast recovery
	db, err = Open(opts)
	require.NoError(t, err)

	// Verify all data is correctly recovered
	for i := 0; i < n; i++ {
		txGet(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), nil)
	}

	require.NoError(t, db.Close())
	removeDir(opts.Dir)
}

func TestDB_HintFileWithDifferentDataStructures(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = KB
	opts.Dir = filepath.Join(t.TempDir(), "test-hintfile-ds-recovery")
	opts.EnableHintFile = true

	// Clean the test directory at the start
	removeDir(opts.Dir)

	db, err := Open(opts)
	require.NoError(t, err)

	// Test BTree
	bucketBTree := "bucket_btree"
	txCreateBucket(t, db, DataStructureBTree, bucketBTree, nil)
	for i := 0; i < 50; i++ {
		txPut(t, db, bucketBTree, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
	}

	// Test Set
	bucketSet := "bucket_set"
	txCreateBucket(t, db, DataStructureSet, bucketSet, nil)
	key := testutils.GetTestBytes(0)
	for i := 0; i < 30; i++ {
		txSAdd(t, db, bucketSet, key, testutils.GetTestBytes(i), nil, nil)
	}

	// Test List
	bucketList := "bucket_list"
	txCreateBucket(t, db, DataStructureList, bucketList, nil)
	listKey := testutils.GetTestBytes(0)
	for i := 0; i < 20; i++ {
		txPush(t, db, bucketList, listKey, testutils.GetTestBytes(i), true, nil, nil)
	}

	// Test SortedSet
	bucketZSet := "bucket_zset"
	txCreateBucket(t, db, DataStructureSortedSet, bucketZSet, nil)
	zsetKey := testutils.GetTestBytes(0)
	for i := 0; i < 15; i++ {
		txZAdd(t, db, bucketZSet, zsetKey, testutils.GetTestBytes(i), float64(i), nil, nil)
	}

	// Perform merge to create hint files
	require.NoError(t, db.Merge())

	// Close the database
	require.NoError(t, db.Close())

	// Reopen the database - it should use hint files for fast recovery
	db, err = Open(opts)
	require.NoError(t, err)

	// Verify BTree data
	for i := 0; i < 50; i++ {
		txGet(t, db, bucketBTree, testutils.GetTestBytes(i), testutils.GetTestBytes(i), nil)
	}

	// Verify Set data
	for i := 0; i < 30; i++ {
		txSIsMember(t, db, bucketSet, key, testutils.GetTestBytes(i), true)
	}

	// Verify List data
	txLRange(t, db, bucketList, listKey, 0, -1, 20, nil, nil)

	// Verify SortedSet data
	for i := 0; i < 15; i++ {
		txZScore(t, db, bucketZSet, zsetKey, testutils.GetTestBytes(i), float64(i), nil)
	}

	require.NoError(t, db.Close())
	removeDir(opts.Dir)
}

func TestDB_HintFileDisabled(t *testing.T) {
	bucket := "bucket"
	opts := DefaultOptions
	opts.SegmentSize = KB
	opts.Dir = filepath.Join(t.TempDir(), "test-hintfile-disabled-recovery")
	opts.EnableHintFile = false // Disable hint file

	// Clean the test directory at the start
	removeDir(opts.Dir)

	// Create a database with some data
	db, err := Open(opts)
	require.NoError(t, err)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	// Add some data
	n := 100
	for i := 0; i < n; i++ {
		txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
	}

	// Perform merge - should not create hint files
	require.NoError(t, db.Merge())

	// Close the database
	require.NoError(t, db.Close())

	// Verify no hint files are created
	fileIDs := enumerateDataFilesInDir(opts.Dir)
	for _, fileID := range fileIDs {
		hintPath := getHintPath(fileID, opts.Dir)
		_, err := os.Stat(hintPath)
		if err == nil {
			t.Errorf("Hint file %s should not exist when EnableHintFile is false", hintPath)
		}
	}

	// Reopen the database - it should scan data files
	db, err = Open(opts)
	require.NoError(t, err)

	// Verify all data is correctly recovered
	for i := 0; i < n; i++ {
		txGet(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), nil)
	}

	require.NoError(t, db.Close())
	removeDir(opts.Dir)
}

// func TestDB_Watch(t *testing.T) {
// 	t.Run("db btree watch key and receive message", func(t *testing.T) {
// 		runNutsDBTestWithWatch(t, func(t *testing.T, db *DB) {
// 			bucket := "bucket"
// 			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
// 			key0 := testutils.GetTestBytes(0)
// 			val0 := testutils.GetRandomBytes(24)
// 			done := make(chan struct{})

// 			go func() {
// 				err := db.Watch(bucket, key0, func(msg *Message) error {
// 					assert.Equal(t, bucket, msg.BucketName)
// 					assert.Equal(t, string(key0), msg.Key)
// 					assert.Equal(t, val0, msg.Value)
// 					close(done)
// 					return nil
// 				})

// 				if err != nil {
// 					assert.ErrorIs(t, err, ErrWatchingChannelClosed)
// 					return
// 				}
// 			}()

// 			// Wait for the watching to be started
// 			time.Sleep(100 * time.Millisecond)

// 			// put
// 			txPut(t, db, bucket, key0, val0, Persistent, nil, nil)
// 			select {
// 			case <-done:
// 				t.Log("Received message")
// 			case <-time.After(10 * time.Second):
// 				t.Fatal("Timeout waiting for message")
// 			}
// 		})
// 	})

// 	t.Run("db list watch key and receive message", func(t *testing.T) {
// 		runNutsDBTestWithWatch(t, func(t *testing.T, db *DB) {
// 			bucket := "bucket"
// 			txCreateBucket(t, db, DataStructureList, bucket, nil)
// 			key0 := testutils.GetTestBytes(0)
// 			val0 := testutils.GetRandomBytes(24)
// 			count := 0
// 			expectCount := 6
// 			done := make(chan struct{})

// 			go func() {
// 				err := db.Watch(bucket, key0, func(msg *Message) error {
// 					assert.Equal(t, bucket, msg.BucketName)
// 					assert.Equal(t, string(key0), msg.Key)
// 					if msg.Flag != DataLRemFlag && msg.Flag != DataLRemByIndex {
// 						assert.Equal(t, val0, msg.Value)
// 					}
// 					count++

// 					if count == expectCount {
// 						close(done)
// 					}
// 					return nil
// 				})

// 				if err != nil {
// 					assert.ErrorIs(t, err, ErrWatchingChannelClosed)
// 					return
// 				}
// 			}()

// 			// Wait for the watching to be started
// 			time.Sleep(100 * time.Millisecond)

// 			// put head
// 			txPush(t, db, bucket, key0, val0, false, nil, nil)

// 			// put tail
// 			txPush(t, db, bucket, key0, val0, true, nil, nil)

// 			// remove elements of key
// 			txLRem(t, db, bucket, key0, 0, val0, nil)

// 			// push two elements to the list and lrem by index
// 			txPush(t, db, bucket, key0, val0, true, nil, nil)
// 			txPush(t, db, bucket, key0, val0, true, nil, nil)
// 			txLRemByIndex(t, db, bucket, key0, nil, 0, 1)

// 			// must receive one message
// 			select {
// 			case <-done:
// 				require.Equal(t, count, expectCount, "must receive one message")
// 			case <-time.After(10 * time.Second):
// 				t.Fatal("Timeout waiting for message")
// 			}
// 		})
// 	})

// 	t.Run("db sorted set watch key and receive message", func(t *testing.T) {
// 		runNutsDBTestWithWatch(t, func(t *testing.T, db *DB) {
// 			bucket := "bucket"
// 			txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)
// 			key := []byte("0")
// 			value := []byte(strconv.Itoa(0))
// 			count := atomic.Int32{}
// 			score := 1.0
// 			expectCount := 4
// 			done := make(chan struct{})

// 			go func() {
// 				err := db.Watch(bucket, key, func(msg *Message) error {
// 					assert.Equal(t, bucket, msg.BucketName)
// 					assert.Equal(t, string(key), msg.Key)
// 					if msg.Flag != DataZPopMinFlag && msg.Flag != DataZPopMaxFlag {
// 						assert.Equal(t, value, msg.Value)
// 					}

// 					count.Add(1)
// 					if count.Load() == int32(expectCount) {
// 						close(done)
// 					}
// 					return nil
// 				})

// 				if err != nil {
// 					assert.ErrorIs(t, err, ErrWatchingChannelClosed)
// 					return
// 				}
// 			}()

// 			// wait for the watching to be started
// 			time.Sleep(100 * time.Millisecond)

// 			txZAdd(t, db, bucket, key, value, score, nil, nil)
// 			txZRem(t, db, bucket, key, value, nil)

// 			txZAdd(t, db, bucket, key, value, score, nil, nil)

// 			txZPop(t, db, bucket, key, true, value, score, nil)

// 			select {
// 			case <-done:
// 				require.Equal(t, count.Load(), int32(expectCount), "must receive one message")
// 			case <-time.After(10 * time.Second):
// 				t.Fatal("Timeout waiting for message")
// 			}
// 		})
// 	})

// 	t.Run("db set watch key and receive message", func(t *testing.T) {
// 		runNutsDBTestWithWatch(t, func(t *testing.T, db *DB) {
// 			bucket := "bucket"
// 			txCreateBucket(t, db, DataStructureSet, bucket, nil)
// 			key := testutils.GetTestBytes(0)
// 			val := testutils.GetTestBytes(0)
// 			val1 := testutils.GetTestBytes(1)

// 			count := atomic.Int32{}
// 			expectCount := 3
// 			done := make(chan struct{})

// 			go func() {
// 				err := db.Watch(bucket, key, func(msg *Message) error {
// 					assert.Equal(t, bucket, msg.BucketName)
// 					assert.Equal(t, string(key), msg.Key)
// 					assert.NotNil(t, msg.Value)

// 					count.Add(1)
// 					if count.Load() == int32(expectCount) {
// 						close(done)
// 					}
// 					return nil
// 				})

// 				if err != nil {
// 					assert.ErrorIs(t, err, ErrWatchingChannelClosed)
// 					return
// 				}
// 			}()

// 			// wait for the watching to be started
// 			time.Sleep(100 * time.Millisecond)
// 			txSAdd(t, db, bucket, key, val, nil, nil)

// 			// add duplicate value
// 			txSAdd(t, db, bucket, key, val, nil, nil)

// 			txSAdd(t, db, bucket, key, val1, nil, nil)
// 			txSRem(t, db, bucket, key, val1, nil)

// 			select {
// 			case <-done:
// 				txSRem(t, db, bucket, key, val1, ErrSetMemberNotExist)
// 				require.Equal(t, count.Load(), int32(expectCount), "must receive one message")
// 			case <-time.After(10 * time.Second):
// 				t.Logf("receive count at test set: %+v", count.Load())
// 				t.Fatal("Timeout waiting for message")
// 			}
// 		})
// 	})

// 	// t.Run("db watch and callback failed", func(t *testing.T) {
// 	// 	runNutsDBTestWithWatch(t, func(t *testing.T, db *DB) {
// 	// 		bucket := "bucket"
// 	// 		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
// 	// 		key := testutils.GetTestBytes(0)
// 	// 		val := testutils.GetTestBytes(0)
// 	// 		go func() {
// 	// 			err := db.Watch(bucket, key, func(msg *Message) error {
// 	// 				return ErrWatchingCallbackFailed
// 	// 			})
// 	// 			require.Equal(t, err, ErrWatchingCallbackFailed)
// 	// 		}()

// 	// 		time.Sleep(100 * time.Millisecond)

// 	// 		// put
// 	// 		txPut(t, db, bucket, key, val, Persistent, nil, nil)
// 	// 	})
// 	// })

// 	// t.Run("db watch and callback timeout", func(t *testing.T) {
// 	// 	runNutsDBTestWithWatch(t, func(t *testing.T, db *DB) {
// 	// 		bucket := "bucket"
// 	// 		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
// 	// 		key := testutils.GetTestBytes(0)
// 	// 		val := testutils.GetTestBytes(0)
// 	// 		watchOpts := NewWatchOptions()
// 	// 		watchOpts.WithCallbackTimeout(100 * time.Millisecond)

// 	// 		go func() {
// 	// 			err := db.Watch(bucket, key, func(msg *Message) error {
// 	// 				time.Sleep(200 * time.Millisecond)
// 	// 				return nil
// 	// 			}, *watchOpts)
// 	// 			require.ErrorIs(t, err, ErrWatchingCallbackTimeout)
// 	// 		}()

// 	// 		time.Sleep(100 * time.Millisecond)

// 	// 		txPut(t, db, bucket, key, val, Persistent, nil, nil)
// 	// 	})
// 	// })

// 	// t.Run("db watch with default callback timeout and run long", func(t *testing.T) {
// 	// 	runNutsDBTestWithWatch(t, func(t *testing.T, db *DB) {
// 	// 		bucket := "bucket"
// 	// 		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
// 	// 		key := testutils.GetTestBytes(0)
// 	// 		val := testutils.GetTestBytes(0)
// 	// 		watchOpts := NewWatchOptions()

// 	// 		go func() {
// 	// 			err := db.Watch(bucket, key, func(msg *Message) error {
// 	// 				done := make(chan struct{})

// 	// 				// block the callback
// 	// 				<-done
// 	// 				return nil
// 	// 			}, *watchOpts)
// 	// 			require.ErrorIs(t, err, ErrWatchingCallbackTimeout)
// 	// 		}()

// 	// 		time.Sleep(100 * time.Millisecond)

// 	// 		for i := 0; i < 2; i++ {
// 	// 			txPut(t, db, bucket, key, val, Persistent, nil, nil)
// 	// 		}
// 	// 	})
// 	// })

// 	t.Run("db watch after watch manager closed", func(t *testing.T) {
// 		opts := DefaultOptions
// 		opts.EnableWatch = true
// 		opts.Dir = "/tmp/test-watch-manager-closed/"
// 		removeDir(opts.Dir)

// 		db, err := Open(opts)
// 		require.NoError(t, err)
// 		bucket := "bucket"
// 		key := testutils.GetTestBytes(0)
// 		val := testutils.GetTestBytes(0)
// 		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
// 		txPut(t, db, bucket, key, val, Persistent, nil, nil)

// 		db.wm.close()
// 		require.Equal(t, db.wm.isClosed(), true)
// 		time.Sleep(100 * time.Millisecond)

// 		err = db.Watch(bucket, key, func(msg *Message) error {
// 			return nil
// 		})
// 		require.Equal(t, err, ErrWatchManagerClosed)
// 	})

// 	t.Run("db is watching and watch manager is closing", func(t *testing.T) {
// 		runNutsDBTestWithWatch(t, func(t *testing.T, db *DB) {
// 			bucket := "bucket"
// 			key := testutils.GetTestBytes(0)
// 			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

// 			go func() {
// 				err := db.Watch(bucket, key, func(msg *Message) error {
// 					return nil
// 				})
// 				require.NoError(t, err)
// 			}()

// 			go func() {
// 				time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
// 				db.wm.close()
// 				require.Equal(t, db.wm.isClosed(), true)
// 			}()

// 			ticker := time.NewTicker(10 * time.Millisecond)
// 			defer ticker.Stop()
// 			for i := 0; i < 100; i++ {
// 				val := testutils.GetTestBytes(i)
// 				<-ticker.C
// 				txPut(t, db, bucket, key, val, Persistent, nil, nil)
// 			}

// 		})
// 	})

// 	t.Run("db watch and tx delete", func(t *testing.T) {
// 		runNutsDBTestWithWatch(t, func(t *testing.T, db *DB) {
// 			bucket := "bucket"
// 			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
// 			key := testutils.GetTestBytes(0)
// 			val := testutils.GetTestBytes(0)
// 			done := make(chan struct{})
// 			go func() {
// 				flag := DataSetFlag
// 				err := db.Watch(bucket, key, func(msg *Message) error {
// 					assert.Equal(t, bucket, msg.BucketName)
// 					assert.Equal(t, string(key), msg.Key)
// 					assert.Equal(t, flag, msg.Flag)
// 					if flag != DataSetFlag {
// 						close(done)
// 					}
// 					flag = DataDeleteFlag
// 					return nil
// 				})

// 				if err != nil {
// 					assert.ErrorIs(t, err, ErrWatchingChannelClosed)
// 					return
// 				}
// 			}()

// 			txPut(t, db, bucket, key, val, Persistent, nil, nil)
// 			txDel(t, db, bucket, key, nil)
// 			require.NoError(t, err)

// 			select {
// 			case <-done:
// 				t.Log("Received delete message")
// 			case <-time.After(10 * time.Second):
// 				t.Fatal("Timeout waiting for message")
// 			}
// 		})
// 	})

// 	t.Run("db watch and transaction rollback", func(t *testing.T) {
// 		opts := DefaultOptions
// 		opts.EnableWatch = true
// 		opts.Dir = "/tmp/test-watch-and-transaction-rollback/"
// 		removeDir(opts.Dir)

// 		db, err := Open(opts)

// 		defer func() {
// 			if db != nil {
// 				db.Close()
// 			}
// 		}()

// 		require.NoError(t, err)
// 		bucket := "bucket"
// 		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
// 		key := testutils.GetTestBytes(0)

// 		wg := sync.WaitGroup{}
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			count := 0
// 			err := db.Watch(bucket, key, func(msg *Message) error {
// 				count++
// 				return nil
// 			})

// 			require.NoError(t, err)
// 			require.Equal(t, count, 0, "all actions should be rolled back")
// 			t.Log("watch callback should not be called due to rollback")
// 		}()

// 		time.Sleep(100 * time.Millisecond)

// 		errUpdate := db.Update(func(tx *Tx) error {
// 			for i := 0; i < 10; i++ {
// 				val := testutils.GetTestBytes(i)
// 				currentKey := key

// 				//deliberately set error key to make tx rollback
// 				if i == 9 {
// 					currentKey = []byte("")
// 				}
// 				if err := tx.Put(bucket, currentKey, val, Persistent); err != nil {
// 					if i < 9 {
// 						t.Fatal("check rollback watching failed")
// 					}
// 				}

// 			}
// 			return nil
// 		})

// 		require.NoError(t, errUpdate)
// 		require.NoError(t, db.wm.close())
// 		wg.Wait()
// 	})

// 	t.Run("db watch and txn exceed write limit", func(t *testing.T) {
// 		// Set up options
// 		opts := DefaultOptions
// 		limitCount := int64(100)
// 		opts.MaxWriteRecordCount = limitCount
// 		opts.EnableWatch = true

// 		bucket1 := "bucket1"
// 		bucket2 := "bucket2"
// 		score := 1.0
// 		done := make(chan struct{})

// 		// Iterate over EntryIdxMode options
// 		for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode} {
// 			count := atomic.Int64{} // count the number of messages received
// 			opts.EntryIdxMode = idxMode
// 			opts.Dir = "/tmp/test-watch-and-txn-exceed-write-limit-mode-" + strconv.Itoa(int(idxMode)) + "/"
// 			removeDir(opts.Dir)

// 			db, err := Open(opts)
// 			require.NoError(t, err)

// 			defer func() {
// 				if db != nil {
// 					db.Close()
// 				}
// 			}()

// 			txCreateBucket(t, db, DataStructureBTree, bucket1, nil)
// 			txCreateBucket(t, db, DataStructureList, bucket1, nil)
// 			txCreateBucket(t, db, DataStructureSet, bucket1, nil)
// 			txCreateBucket(t, db, DataStructureSortedSet, bucket1, nil)
// 			txCreateBucket(t, db, DataStructureList, bucket2, nil)
// 			key1 := []byte("key1")
// 			key2 := []byte("key2")
// 			countOfMessages := int64(107)

// 			// Initialize the watcher for bucket2
// 			for i := 0; i < int(limitCount); i++ {
// 				go func(i int) {
// 					key := []byte(strconv.Itoa(i))
// 					err := db.Watch(bucket1, key, func(msg *Message) error {
// 						count.Add(1)
// 						if count.Load() == countOfMessages {
// 							close(done)
// 						}
// 						return nil
// 					})

// 					require.NoError(t, err)
// 				}(i)
// 			}

// 			go func() {
// 				err := db.Watch(bucket1, key1, func(msg *Message) error {
// 					count.Add(1)
// 					if count.Load() == countOfMessages {
// 						close(done)
// 					}
// 					return nil
// 				})
// 				require.NoError(t, err)
// 			}()

// 			keys := [][]byte{key1, key2}
// 			for _, key := range keys {
// 				go func(key []byte) {
// 					err := db.Watch(bucket2, key, func(msg *Message) error {
// 						count.Add(1)
// 						if count.Load() == countOfMessages {
// 							close(done)
// 						}
// 						return nil
// 					})
// 					require.NoError(t, err)
// 				}(key)
// 			}

// 			// Add limitCount records
// 			errUpdate := db.Update(func(tx *Tx) error {
// 				for i := 0; i < int(limitCount); i++ {
// 					value := []byte(strconv.Itoa(i))
// 					key := []byte(strconv.Itoa(i))
// 					err = tx.Put(bucket1, key, value, Persistent)
// 					AssertErr(t, err, nil)
// 				}
// 				return nil
// 			})
// 			require.NoError(t, errUpdate)

// 			// Trigger the limit
// 			txPush(t, db, bucket1, []byte("0"), []byte("value1"), false, nil, ErrTxnExceedWriteLimit)

// 			//  Delete item and add one
// 			txDel(t, db, bucket1, []byte("0"), nil)

// 			// NOTE: after delete the key, the watch of key "0" will be unsubscribed
// 			// so the watch callback will not be called
// 			txPush(t, db, bucket1, []byte("0"), []byte("value1"), false, nil, nil)

// 			// Trigger the limit
// 			txSAdd(t, db, bucket1, []byte("key1"), []byte("value1"), nil, ErrTxnExceedWriteLimit)
// 			// Delete item and add one
// 			txDel(t, db, bucket1, []byte("1"), nil)

// 			txSAdd(t, db, bucket1, key1, []byte("value1"), nil, nil)
// 			// Trigger the limit
// 			txZAdd(t, db, bucket1, key1, []byte("value1"), score, nil, ErrTxnExceedWriteLimit)
// 			// Delete item and add one
// 			txDel(t, db, bucket1, []byte("2"), nil)
// 			txZAdd(t, db, bucket1, key1, []byte("value1"), score, nil, nil)
// 			// Delete bucket
// 			txDeleteBucket(t, db, DataStructureSortedSet, bucket1, nil)

// 			// Add data to another bucket
// 			txPush(t, db, bucket2, key1, []byte("value1"), false, nil, nil)
// 			// Trigger the limit
// 			txPush(t, db, bucket2, key2, []byte("value2"), false, nil, ErrTxnExceedWriteLimit)
// 			time.Sleep(1 * time.Second)

// 			// db.wm.close()
// 			select {
// 			case <-done:
// 				require.Equal(t, count.Load(), countOfMessages, "the watch callback should be called 108 times")
// 			case <-time.After(10 * time.Second):
// 				t.Log("watch callback is called", count.Load(), "times")
// 				t.Fatal("Timeout waiting for message")
// 			}
// 		}
// 	})

// 	t.Run("db watch and watch feature disabled", func(t *testing.T) {
// 		runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
// 			bucket := "bucket"
// 			key := testutils.GetTestBytes(0)
// 			err := db.Watch(bucket, key, func(msg *Message) error {
// 				t.Fatal("Watch feature should be disabled")
// 				return nil
// 			})
// 			require.ErrorIs(t, err, ErrWatchFeatureDisabled)
// 		})
// 	})
// }

// func TestDB_WatchTTL(t *testing.T) {
// 	t.Run("db watch and ttl", func(t *testing.T) {
// 		runNutsDBTestWithWatch(t, func(t *testing.T, db *DB) {
// 			bucket := "bucket"
// 			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
// 			key := testutils.GetTestBytes(0)
// 			done := make(chan struct{})
// 			count := atomic.Int64{}
// 			expectCount := int64(2)

// 			go func() {
// 				err := db.Watch(bucket, key, func(msg *Message) error {
// 					count.Add(1)
// 					if count.Load() == expectCount {
// 						close(done)
// 					}
// 					return nil
// 				})
// 				require.NoError(t, err)
// 			}()

// 			time.Sleep(100 * time.Millisecond)

// 			txPut(t, db, bucket, key, []byte("value"), 1, nil, nil)

// 			select {
// 			case <-done:
// 				t.Log("Received message")
// 			case <-time.After(10 * time.Second):
// 				t.Fatal("Timeout waiting for message")
// 			}
// 		})
// 	})

// 	t.Run("db watch and ttl expired list", func(t *testing.T) {
// 		runNutsDBTestWithWatch(t, func(t *testing.T, db *DB) {
// 			bucket := "bucket"
// 			txCreateBucket(t, db, DataStructureList, bucket, nil)
// 			key := testutils.GetTestBytes(0)
// 			done := make(chan struct{})
// 			count := atomic.Int64{}
// 			expectCount := int64(2)

// 			go func() {
// 				err := db.Watch(bucket, key, func(msg *Message) error {
// 					count.Add(1)
// 					if count.Load() == expectCount {
// 						close(done)
// 					}
// 					return nil
// 				})
// 				require.NoError(t, err)
// 			}()

// 			time.Sleep(100 * time.Millisecond)

// 			txPush(t, db, bucket, key, []byte("value"), false, nil, nil)
// 			txExpireList(t, db, bucket, key, 1, nil)

// 			select {
// 			case <-done:
// 				t.Log("Received message")
// 			case <-time.After(10 * time.Second):
// 				t.Fatal("Timeout waiting for message")
// 			}
// 		})
// 	})
// }

// func TestDB_WatchDeleteBucket(t *testing.T) {
// 	t.Run("db watch and delete bucket", func(t *testing.T) {
// 		runNutsDBTestWithWatch(t, func(t *testing.T, db *DB) {
// 			bucket := "bucket"
// 			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
// 			key := testutils.GetTestBytes(0)
// 			done := make(chan struct{})
// 			count := atomic.Int64{}
// 			expectCount := int64(2)

// 			go func() {
// 				err := db.Watch(bucket, key, func(msg *Message) error {
// 					count.Add(1)
// 					if count.Load() == expectCount {
// 						close(done)
// 					}
// 					return nil
// 				})
// 				require.NoError(t, err)
// 			}()

// 			time.Sleep(100 * time.Millisecond)

// 			txPut(t, db, bucket, key, []byte("value"), 1, nil, nil)

// 			txDeleteBucket(t, db, DataStructureBTree, bucket, nil)
// 			select {
// 			case <-done:
// 				t.Log("Received message")
// 			case <-time.After(10 * time.Second):
// 				t.Fatal("Timeout waiting for message")
// 			}
// 		})
// 	})

// 	t.Run("db watch and delete bucket", func(t *testing.T) {
// 		runNutsDBTestWithWatch(t, func(t *testing.T, db *DB) {
// 			bucket := "bucket"
// 			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
// 			key := testutils.GetTestBytes(0)
// 			done := make(chan struct{})
// 			count := atomic.Int64{}
// 			expectCount := int64(2)

// 			go func() {
// 				err := db.Watch(bucket, key, func(msg *Message) error {
// 					count.Add(1)
// 					if count.Load() == expectCount {
// 						close(done)
// 					}
// 					return nil
// 				})
// 				require.NoError(t, err)
// 			}()

// 			time.Sleep(100 * time.Millisecond)

// 			txPut(t, db, bucket, key, []byte("value"), 1, nil, nil)

// 			txDeleteBucket(t, db, DataStructureBTree, bucket, nil)
// 			select {
// 			case <-done:
// 				t.Log("Received message")
// 			case <-time.After(10 * time.Second):
// 				t.Fatal("Timeout waiting for message")
// 			}
// 		})
// 	})

// 	t.Run("db watch many keys and delete bucket", func(t *testing.T) {
// 		runNutsDBTestWithWatch(t, func(t *testing.T, db *DB) {
// 			bucket := "bucket"
// 			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
// 			keys := make([][]byte, 100)
// 			count := atomic.Int64{}
// 			expectCount := int64(200)
// 			done := make(chan struct{})
// 			for i := 0; i < 100; i++ {
// 				keys[i] = testutils.GetTestBytes(i)
// 			}

// 			for _, key := range keys {
// 				go func(key []byte) {
// 					err := db.Watch(bucket, key, func(msg *Message) error {
// 						assert.NotNil(t, msg)
// 						assert.Equal(t, bucket, msg.BucketName)
// 						count.Add(1)
// 						if count.Load() == expectCount {
// 							close(done)
// 						}
// 						return nil
// 					})
// 					require.NoError(t, err)
// 				}(key)
// 			}

// 			for _, key := range keys {
// 				txPut(t, db, bucket, key, []byte("value"), Persistent, nil, nil)
// 			}

// 			txDeleteBucket(t, db, DataStructureBTree, bucket, nil)

// 			select {
// 			case <-done:
// 				t.Log("Received message")
// 			case <-time.After(10 * time.Second):
// 				t.Log("Received message", count.Load(), "times")
// 				t.Fatal("Timeout waiting for message")
// 			}
// 		})
// 	})

// 	t.Run("db watch different data structures and delete bucket", func(t *testing.T) {
// 		runNutsDBTestWithWatch(t, func(t *testing.T, db *DB) {
// 			// Create separate buckets for each data structure
// 			btreeBucket := "btree_bucket"
// 			listBucket := "list_bucket"
// 			setBucket := "set_bucket"
// 			zsetBucket := "zset_bucket"

// 			txCreateBucket(t, db, DataStructureBTree, btreeBucket, nil)
// 			txCreateBucket(t, db, DataStructureList, listBucket, nil)
// 			txCreateBucket(t, db, DataStructureSet, setBucket, nil)
// 			txCreateBucket(t, db, DataStructureSortedSet, zsetBucket, nil)

// 			// Keys to watch
// 			btreeKey := testutils.GetTestBytes(1)
// 			listKey := testutils.GetTestBytes(2)
// 			setKey := testutils.GetTestBytes(3)
// 			zsetKey := testutils.GetTestBytes(4)

// 			// Track message counts
// 			// Each watcher should receive: 1 data operation + 1 delete = 2 messages
// 			count := atomic.Int64{}
// 			expectCount := int64(8) // 4 data structures × 2 messages each
// 			done := make(chan struct{})

// 			// Watch BTree bucket
// 			go func() {
// 				err := db.Watch(btreeBucket, btreeKey, func(msg *Message) error {
// 					assert.NotNil(t, msg)
// 					assert.Equal(t, core.BucketName(btreeBucket), msg.BucketName)
// 					assert.Equal(t, string(btreeKey), msg.Key)

// 					if count.Add(1) == expectCount {
// 						close(done)
// 					}
// 					return nil
// 				})
// 				require.NoError(t, err)
// 			}()

// 			// Watch List bucket
// 			go func() {
// 				err := db.Watch(listBucket, listKey, func(msg *Message) error {
// 					assert.NotNil(t, msg)
// 					assert.Equal(t, core.BucketName(listBucket), msg.BucketName)
// 					assert.Equal(t, string(listKey), msg.Key)

// 					if count.Add(1) == expectCount {
// 						close(done)
// 					}
// 					return nil
// 				})
// 				require.NoError(t, err)
// 			}()

// 			// Watch Set bucket
// 			go func() {
// 				err := db.Watch(setBucket, setKey, func(msg *Message) error {
// 					assert.NotNil(t, msg)
// 					assert.Equal(t, core.BucketName(setBucket), msg.BucketName)
// 					assert.Equal(t, string(setKey), msg.Key)

// 					if count.Add(1) == expectCount {
// 						close(done)
// 					}
// 					return nil
// 				})
// 				require.NoError(t, err)
// 			}()

// 			// Watch SortedSet bucket
// 			go func() {
// 				err := db.Watch(zsetBucket, zsetKey, func(msg *Message) error {
// 					assert.NotNil(t, msg)
// 					assert.Equal(t, core.BucketName(zsetBucket), msg.BucketName)
// 					assert.Equal(t, string(zsetKey), msg.Key)

// 					if count.Add(1) == expectCount {
// 						close(done)
// 					}
// 					return nil
// 				})
// 				require.NoError(t, err)
// 			}()

// 			// Give watchers time to subscribe
// 			time.Sleep(100 * time.Millisecond)

// 			// BTree: Put key-value
// 			txPut(t, db, btreeBucket, btreeKey, []byte("btree_value"), Persistent, nil, nil)

// 			// List: Push items
// 			txPush(t, db, listBucket, listKey, []byte("list_item_1"), true, nil, nil)

// 			// Set: Add members
// 			txSAdd(t, db, setBucket, setKey, []byte("set_member_1"), nil, nil)

// 			// SortedSet: Add scored members
// 			txZAdd(t, db, zsetBucket, zsetKey, []byte("zset_member_1"), 1.0, nil, nil)

// 			// Give time for messages to be processed
// 			time.Sleep(100 * time.Millisecond)

// 			// Delete all buckets - each watcher should receive a delete notification
// 			txDeleteBucket(t, db, DataStructureBTree, btreeBucket, nil)
// 			txDeleteBucket(t, db, DataStructureList, listBucket, nil)
// 			txDeleteBucket(t, db, DataStructureSet, setBucket, nil)
// 			txDeleteBucket(t, db, DataStructureSortedSet, zsetBucket, nil)

// 			// Wait for all expected messages
// 			select {
// 			case <-done:
// 				t.Logf("Received all %d expected messages (count=%d)", expectCount, count.Load())
// 			case <-time.After(10 * time.Second):
// 				t.Fatalf("Timeout waiting for messages. Received %d/%d messages", count.Load(), expectCount)
// 			}
// 		})
// 	})

// 	t.Run("db watch same bucket name across different data structures", func(t *testing.T) {
// 		runNutsDBTestWithWatch(t, func(t *testing.T, db *DB) {
// 			// Use the SAME bucket name for different data structures
// 			// This tests that the bucket manager correctly handles multiple data structures
// 			bucket := "shared_bucket"

// 			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
// 			txCreateBucket(t, db, DataStructureList, bucket, nil)
// 			txCreateBucket(t, db, DataStructureSet, bucket, nil)
// 			txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)

// 			// Use different keys for each data structure type
// 			btreeKey := testutils.GetTestBytes(10)
// 			listKey := testutils.GetTestBytes(20)
// 			setKey := testutils.GetTestBytes(30)
// 			zsetKey := testutils.GetTestBytes(40)

// 			count := atomic.Int64{}
// 			expectCount := int64(5)
// 			done := make(chan struct{})

// 			// Setup watchers
// 			watcherSetup := []struct {
// 				key  []byte
// 				name string
// 			}{
// 				{btreeKey, "BTree"},
// 				{listKey, "List"},
// 				{setKey, "Set"},
// 				{zsetKey, "ZSet"},
// 			}

// 			for _, ws := range watcherSetup {
// 				go func(key []byte, name string) {
// 					err := db.Watch(bucket, key, func(msg *Message) error {
// 						assert.NotNil(t, msg)
// 						assert.Equal(t, core.BucketName(bucket), msg.BucketName)

// 						if count.Add(1) == expectCount {
// 							close(done)
// 						}
// 						return nil
// 					})
// 					require.NoError(t, err)
// 				}(ws.key, ws.name)
// 			}

// 			time.Sleep(100 * time.Millisecond)

// 			// Perform operations
// 			txPut(t, db, bucket, btreeKey, []byte("value"), Persistent, nil, nil)
// 			txPush(t, db, bucket, listKey, []byte("item"), true, nil, nil)
// 			txSAdd(t, db, bucket, setKey, []byte("member"), nil, nil)
// 			txZAdd(t, db, bucket, zsetKey, []byte("member"), 1.0, nil, nil)

// 			time.Sleep(100 * time.Millisecond)

// 			// Delete all versions of the bucket
// 			// When all the ds bucket are deleted, the bucket in watch manager will be deleted
// 			// it will send the delete bucket message to the subscribers
// 			txDeleteBucket(t, db, DataStructureBTree, bucket, nil)
// 			txDeleteBucket(t, db, DataStructureList, bucket, nil)
// 			txDeleteBucket(t, db, DataStructureSet, bucket, nil)
// 			txDeleteBucket(t, db, DataStructureSortedSet, bucket, nil)

// 			select {
// 			case <-done:
// 				t.Logf("Received all %d expected messages", expectCount)
// 			case <-time.After(10 * time.Second):
// 				t.Fatalf("Timeout. Received %d/%d messages", count.Load(), expectCount)
// 			}
// 		})
// 	})
// }
