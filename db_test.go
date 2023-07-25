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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xujiajun/utils/strconv2"
)

var (
	db  *DB
	opt Options
	err error
)

func removeDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
}

func runNutsDBTest(t *testing.T, opts *Options, test func(t *testing.T, db *DB)) {
	if opts == nil {
		opts = new(Options)
		*opts = DefaultOptions
	}
	if opts.Dir == "" {
		opts.Dir = "/tmp/nutsdb-test"
	}
	defer removeDir(opts.Dir)
	db, err := Open(*opts)
	require.NoError(t, err)
	defer func() {
		if !db.IsClose() {
			require.NoError(t, db.Close())
		}
	}()
	test(t, db)
}

func txPut(t *testing.T, db *DB, bucket string, key, value []byte, ttl uint32, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err = tx.Put(bucket, key, value, ttl)
		if expectErr != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
		}
		return nil
	})
	require.NoError(t, err)
}

func txGet(t *testing.T, db *DB, bucket string, key []byte, expectVal []byte, expectErr error) {
	err := db.View(func(tx *Tx) error {
		e, err := tx.Get(bucket, key)
		if expectErr != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
			require.EqualValuesf(t, expectVal, e.Value, "err Tx Get. got %s want %s", string(e.Value), string(expectVal))
		}
		return nil
	})
	require.NoError(t, err)
}

func txDel(t *testing.T, db *DB, bucket string, key []byte, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err = tx.Delete(bucket, key)
		if expectErr != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
		}
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
		key0 := GetTestBytes(0)
		val0 := GetRandomBytes(24)

		// put
		txPut(t, db, bucket, key0, val0, Persistent, nil)
		txGet(t, db, bucket, key0, val0, nil)

		val1 := GetRandomBytes(24)

		// update
		txPut(t, db, bucket, key0, val1, Persistent, nil)
		txGet(t, db, bucket, key0, val1, nil)

		// del
		txDel(t, db, bucket, key0, nil)
		txGet(t, db, bucket, key0, val1, ErrNotFoundKey)
	})
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
		txDel(t, db, testBucket, GetTestBytes(0), ErrNotFoundBucket)
		txPut(t, db, testBucket, GetTestBytes(1), GetRandomBytes(24), Persistent, nil)
		txDel(t, db, testBucket, GetTestBytes(0), ErrKeyNotFound)
	})
}

func TestDB_BPTSparse(t *testing.T) {
	opts := DefaultOptions
	opts.EntryIdxMode = HintBPTSparseIdxMode
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket1, bucket2 := "AA", "AAB"
		key1, key2 := []byte("BB"), []byte("B")
		val1, val2 := []byte("key1"), []byte("key2")
		txPut(t, db, bucket1, key1, val1, Persistent, nil)
		txPut(t, db, bucket2, key2, val2, Persistent, nil)
		txGet(t, db, bucket1, key1, val1, nil)
		txGet(t, db, bucket2, key2, val2, nil)
	})
}

func TestDB_MergeForString(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 1 * 100
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txPut(t, db, bucket, GetTestBytes(0), GetRandomBytes(24), Persistent, nil)
		txPut(t, db, bucket, GetTestBytes(1), GetRandomBytes(24), Persistent, nil)
		txDel(t, db, bucket, GetTestBytes(1), nil)
		txGet(t, db, bucket, GetTestBytes(1), nil, ErrNotFoundKey)
		validKeyNum := db.BPTreeIdx[bucket].ValidKeyCount
		require.EqualValuesf(t, 1, validKeyNum, "err GetValidKeyCount. got %d want %d", validKeyNum, 1)
		require.NoError(t, db.Merge())
	})
}

func TestDB_MergeRepeated(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 120
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

func txSAdd(t *testing.T, db *DB, bucket string, key, value []byte, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.SAdd(bucket, key, value)
		if expectErr != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
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

func txSRem(t *testing.T, db *DB, bucket string, key, value []byte, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.SRem(bucket, key, value)
		if expectErr != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestDB_MergeForSet(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 100
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

func txZAdd(t *testing.T, db *DB, bucket string, key, value []byte, score float64, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.ZAdd(bucket, key, score, value)
		if err != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
		}
		return nil
	})
	require.NoError(t, err)
}

func txZRem(t *testing.T, db *DB, bucket string, key []byte, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.ZRem(bucket, string(key))
		if err != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
		}
		return nil
	})
	require.NoError(t, err)
}

func txZGetByKey(t *testing.T, db *DB, bucket string, key []byte, expectErr error) {
	err := db.View(func(tx *Tx) error {
		_, err := tx.ZGetByKey(bucket, key)
		if err != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
		}
		return nil
	})
	require.NoError(t, err)
}

func txZRangeByRank(t *testing.T, db *DB, bucket string, start, end int) {
	err := db.Update(func(tx *Tx) error {
		err := tx.ZRemRangeByRank(bucket, 1, 10)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)
}

func TestDB_MergeForZSET(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 100
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "bucket"

		for i := 0; i < 100; i++ {
			score, _ := strconv2.IntToFloat64(i)
			txZAdd(t, db, bucket, GetTestBytes(i), GetTestBytes(i), score, nil)
		}

		for i := 0; i < 100; i++ {
			txZGetByKey(t, db, bucket, GetTestBytes(i), nil)
		}

		for i := 0; i < 50; i++ {
			txZRem(t, db, bucket, GetTestBytes(i), nil)
		}

		for i := 0; i < 50; i++ {
			txZGetByKey(t, db, bucket, GetTestBytes(i), ErrNotFoundKey)
		}

		for i := 50; i < 100; i++ {
			txZGetByKey(t, db, bucket, GetTestBytes(i), nil)
		}

		txZRangeByRank(t, db, bucket, 20, 30)

		require.NoError(t, db.Merge())
	})
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

func txPush(t *testing.T, db *DB, bucket string, key, val []byte, expectErr error, isLeft bool) {
	err := db.Update(func(tx *Tx) error {
		var err error

		if isLeft {
			err = tx.LPush(bucket, key, val)
		} else {
			err = tx.RPush(bucket, key, val)
		}

		if expectErr != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
		}

		return nil
	})
	require.NoError(t, err)
}

func txRange(t *testing.T, db *DB, bucket string, key []byte, start, end, expectLen int) {
	err := db.View(func(tx *Tx) error {
		list, err := tx.LRange(bucket, key, start, end)
		require.NoError(t, err)
		require.Equal(t, expectLen, len(list))
		return nil
	})
	require.NoError(t, err)
}

func TestDB_MergeForList(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 100
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "bucket"
		key := GetTestBytes(0)

		for i := 0; i < 100; i++ {
			txPush(t, db, bucket, key, GetTestBytes(i), nil, true)
		}
		txRange(t, db, bucket, key, 0, 99, 100)

		txPop(t, db, bucket, key, GetTestBytes(99), nil, true)
		txPop(t, db, bucket, key, GetTestBytes(0), nil, false)

		require.NoError(t, db.Merge())
	})
}

func TestDB_GetKeyNotFound(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txGet(t, db, bucket, GetTestBytes(0), nil, ErrBucketNotFound)
		txPut(t, db, bucket, GetTestBytes(1), GetRandomBytes(24), Persistent, nil)
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

func TestDB_GetRecordFromKey(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 120
	opts.EntryIdxMode = HintKeyAndRAMIdxMode
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := []byte("bucket")
		key := []byte("hello")
		val := []byte("world")

		_, err := db.getRecordFromKey(bucket, key)
		require.Equal(t, ErrBucketNotFound, err)

		for i := 0; i < 10; i++ {
			txPut(t, db, string(bucket), key, val, Persistent, nil)
		}

		r, err := db.getRecordFromKey(bucket, key)
		require.NoError(t, err)

		require.Equal(t, 58, int(r.H.DataPos))
		require.Equal(t, int64(4), r.H.FileID)
	})
}

func TestErrWhenBuildListIdx(t *testing.T) {
	ts := []struct {
		err     error
		want    error
		notwant error
	}{
		{
			errors.New("some err"),
			errors.New("when build listIdx err: some err"),
			fmt.Errorf("unexpected error"),
		},
	}

	for _, tc := range ts {
		got := ErrWhenBuildListIdx(tc.err)
		assert.Equal(t, got, tc.want)
		assert.NotEqual(t, got, tc.notwant)
	}
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

	tmpdir, _ := ioutil.TempDir("", "nutsdb")
	opt := DefaultOptions
	opt.Dir = tmpdir
	opt.SegmentSize = 8 * 1024

	withDBOption(t, opt, fn)
}

func withRAMIdxDB(t *testing.T, fn func(t *testing.T, db *DB)) {
	tmpdir, _ := ioutil.TempDir("", "nutsdb")
	opt := DefaultOptions
	opt.Dir = tmpdir
	opt.EntryIdxMode = HintKeyAndRAMIdxMode

	withDBOption(t, opt, fn)
}

func withBPTSpareeIdxDB(t *testing.T, fn func(t *testing.T, db *DB)) {
	tmpdir, _ := ioutil.TempDir("", "nutsdb")
	opt := DefaultOptions
	opt.Dir = tmpdir
	opt.EntryIdxMode = HintKeyAndRAMIdxMode

	withDBOption(t, opt, fn)
}
