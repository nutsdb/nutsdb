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
	"reflect"
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
	db, err = Open(*opts)
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

func TestDb_DeleteANonExistKey(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		testBucket := "test_bucket"
		txDel(t, db, testBucket, GetTestBytes(0), ErrNotFoundBucket)
		txPut(t, db, testBucket, GetTestBytes(1), GetRandomBytes(24), Persistent, nil)
		txDel(t, db, testBucket, GetTestBytes(0), ErrKeyNotFound)
	})
}

func TestDB_BPTSparse(t *testing.T) {
	opt := DefaultOptions
	opt.EntryIdxMode = HintBPTSparseIdxMode
	runNutsDBTest(t, &opt, func(t *testing.T, db *DB) {
		bucket1, bucket2 := "AA", "AAB"
		key1, key2 := []byte("BB"), []byte("B")
		val1, val2 := []byte("key1"), []byte("key2")
		txPut(t, db, bucket1, key1, val1, Persistent, nil)
		txPut(t, db, bucket2, key2, val2, Persistent, nil)
		txGet(t, db, bucket1, key1, val1, nil)
		txGet(t, db, bucket2, key2, val2, nil)
	})
}

func TestDB_Merge_For_string(t *testing.T) {
	fileDir := "/tmp/nutsdb_test_str_for_merge"

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

	db2, err := Open(
		DefaultOptions,
		WithDir(fileDir),
		WithSegmentSize(1*100),
	)

	require.NoError(t, err)
	bucketForString := "test_merge"

	key1 := []byte("key_" + fmt.Sprintf("%07d", 1))
	value1 := []byte("value1value1value1value1value1")
	err = db2.Update(
		func(tx *Tx) error {
			return tx.Put(bucketForString, key1, value1, Persistent)
		})
	assert.NoError(t, err, "initStringDataAndDel,err batch put")

	key2 := []byte("key_" + fmt.Sprintf("%07d", 2))
	value2 := []byte("value2value2value2value2value2")
	err = db2.Update(
		func(tx *Tx) error {
			return tx.Put(bucketForString, key2, value2, Persistent)
		})
	assert.NoError(t, err, "initStringDataAndDel,err batch put")

	err = db2.Update(
		func(tx *Tx) error {
			return tx.Delete(bucketForString, key2)
		})
	assert.NoError(t, err)

	err = db2.View(
		func(tx *Tx) error {
			_, err := tx.Get(bucketForString, key2)
			assert.Error(t, err, "err read data")
			return nil
		})
	require.NoError(t, err)

	//GetValidKeyCount
	validKeyNum := db2.BPTreeIdx[bucketForString].ValidKeyCount
	assert.EqualValuesf(t, 1, validKeyNum, "err GetValidKeyCount. got %d want %d", validKeyNum, 1)

	err = db2.Merge()
	assert.NoError(t, err, "err merge")
}

func Test_MergeRepeated(t *testing.T) {
	InitOpt("", true)
	db, err = Open(
		opt,
		WithSegmentSize(120),
	)
	if err != nil {
		t.Errorf("wanted nil, got %v", err)
	}
	for i := 0; i < 20; i++ {
		err = db.Update(func(tx *Tx) error {
			if err := tx.Put("bucket", []byte("hello"), []byte("world"), Persistent); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Errorf("wanted nil, got %v", err)
		}
	}
	if db.MaxFileID != 9 {
		t.Errorf("wanted fileID: %d, got :%d", 9, db.MaxFileID)
	}
	err = db.View(func(tx *Tx) error {
		e, err := tx.Get("bucket", []byte("hello"))
		if err != nil {
			return err
		}
		if reflect.DeepEqual(e.Value, []byte("value")) {
			return fmt.Errorf("wanted value: %v, got :%v", []byte("value"), e.Value)
		}
		return nil
	})
	if err != nil {
		t.Errorf("wanted nil, got %v", err)
	}
	err = db.Merge()
	if err != nil {
		t.Errorf("wanted nil, got %v", err)
	}
	if db.MaxFileID != 10 {
		t.Errorf("wanted fileID: %d, got :%d", 10, db.MaxFileID)
	}
	err = db.View(func(tx *Tx) error {
		e, err := tx.Get("bucket", []byte("hello"))
		if err != nil {
			return err
		}
		if reflect.DeepEqual(e.Value, []byte("value")) {
			return fmt.Errorf("wanted value: %v, got :%v", []byte("value"), e.Value)
		}
		return nil
	})
	if err != nil {
		t.Errorf("wanted nil, got %v", err)
	}
	err = db.Close()
	if err != nil {
		t.Errorf("wanted nil, got %v", err)
	}
}

func opSAddAndCheckForTestMerge(bucketForSet string, key []byte, t *testing.T) {
	for i := 0; i < 100; i++ {
		if err := db.Update(func(tx *Tx) error {
			val := []byte("setVal" + fmt.Sprintf("%07d", i))
			err := tx.SAdd(bucketForSet, key, val)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 100; i++ {
		if err := db.View(func(tx *Tx) error {
			val := []byte("setVal" + fmt.Sprintf("%07d", i))
			ok, _ := tx.SIsMember(bucketForSet, key, val)
			if !ok {
				t.Error("err read set data ")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDB_Merge_for_Set(t *testing.T) {
	InitOpt("/tmp/nutsdbtestformergeset", true)
	//InitOpt("/tmp/nutsdbtestformergeset", false)
	db, err = Open(opt)

	readFlag := false
	mergeFlag := true

	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}

	bucketForSet := "bucket_for_set_merge_test"
	key := []byte("mySet_for_merge_test")

	if !readFlag {
		opSAddAndCheckForTestMerge(bucketForSet, key, t)
	}

	if !readFlag {
		for i := 0; i < 50; i++ {
			if err := db.Update(func(tx *Tx) error {
				val := []byte("setVal" + fmt.Sprintf("%07d", i))
				err := tx.SRem(bucketForSet, key, val)
				if err != nil {
					return err
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	}

	for i := 0; i < 50; i++ {
		if err := db.View(func(tx *Tx) error {
			val := []byte("setVal" + fmt.Sprintf("%07d", i))
			ok, _ := tx.SIsMember(bucketForSet, key, val)
			if ok {
				t.Error("err read set data ")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 50; i < 100; i++ {
		if err := db.View(func(tx *Tx) error {
			val := []byte("setVal" + fmt.Sprintf("%07d", i))
			ok, _ := tx.SIsMember(bucketForSet, key, val)
			if !ok {
				t.Error("err read set data ")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	//do merge
	if mergeFlag {
		if err = db.Merge(); err != nil {
			t.Error("err merge", err)
		}
	}
}

func initZSetDataForTestMerge(bucketForZSet string, t *testing.T) {
	for i := 0; i < 100; i++ {
		if err := db.Update(func(tx *Tx) error {
			key := []byte("zsetKey" + fmt.Sprintf("%07d", i))
			val := []byte("zsetVal" + fmt.Sprintf("%07d", i))
			score, _ := strconv2.IntToFloat64(i)
			err := tx.ZAdd(bucketForZSet, key, score, val)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 100; i++ {
		if err := db.View(func(tx *Tx) error {
			key := []byte("zsetKey" + fmt.Sprintf("%07d", i))
			_, err := tx.ZGetByKey(bucketForZSet, key)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func remZSetDataForTestMerge(bucketForZSet string, t *testing.T) {
	for i := 0; i < 50; i++ {
		if err := db.Update(func(tx *Tx) error {
			key := []byte("zsetKey" + fmt.Sprintf("%07d", i))
			err := tx.ZRem(bucketForZSet, string(key))
			if err != nil {
				return err
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func checkRemZSetDataForTestMerge(bucketForZSet string, t *testing.T) {
	for i := 0; i < 50; i++ {
		if err := db.View(func(tx *Tx) error {
			key := []byte("zsetKey" + fmt.Sprintf("%07d", i))
			_, err := tx.ZGetByKey(bucketForZSet, key)
			//fmt.Println("get n",n)
			if err == nil {
				t.Error("err read sorted set data ")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 50; i < 100; i++ {
		if err := db.View(func(tx *Tx) error {
			key := []byte("zsetKey" + fmt.Sprintf("%07d", i))
			_, err := tx.ZGetByKey(bucketForZSet, key)
			if err != nil {
				t.Error(err)
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func opZRemRangeByRankForTestMerge(readFlag bool, bucketForZSet string, t *testing.T) {
	if !readFlag {
		if err := db.Update(func(tx *Tx) error {
			err := tx.ZRemRangeByRank(bucketForZSet, 1, 10)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 60; i < 100; i++ {
		if err := db.View(func(tx *Tx) error {
			key := []byte("zsetKey" + fmt.Sprintf("%07d", i))
			_, err := tx.ZGetByKey(bucketForZSet, key)
			if err != nil {
				t.Error(err)
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDB_Merge_For_ZSET(t *testing.T) {
	InitOpt("/tmp/nutsdbtestformergezset", true)
	//InitOpt("/tmp/nutsdbtestformergezset", false)

	readFlag := false
	mergeFlag := true

	bucketForZSet := "bucket_for_zset_merge_test"

	db, err = Open(opt)
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}

	if !readFlag {
		initZSetDataForTestMerge(bucketForZSet, t)
	}

	if !readFlag {
		remZSetDataForTestMerge(bucketForZSet, t)
	}

	checkRemZSetDataForTestMerge(bucketForZSet, t)

	opZRemRangeByRankForTestMerge(readFlag, bucketForZSet, t)

	if !readFlag {
		if err := db.Update(func(tx *Tx) error {
			_, err := tx.ZPopMax(bucketForZSet)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if err := db.Update(func(tx *Tx) error {
			_, err := tx.ZPopMin(bucketForZSet)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	if err := db.View(func(tx *Tx) error {
		key := []byte("zsetKey" + fmt.Sprintf("%07d", 99))
		_, err := tx.ZGetByKey(bucketForZSet, key)
		if err == nil {
			t.Error("err TestDB_Merge_For_ZSET")
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		key := []byte("zsetKey" + fmt.Sprintf("%07d", 60))
		_, err := tx.ZGetByKey(bucketForZSet, key)
		if err == nil {
			t.Error("err TestDB_Merge_For_ZSET")
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if mergeFlag {
		if err = db.Merge(); err != nil {
			t.Error("err merge", err)
		}
	}
}

func opLPopAndRPopForTestMerge(bucketForList string, key []byte, t *testing.T) {
	if err := db.Update(func(tx *Tx) error {
		item, err := tx.LPop(bucketForList, key)
		if err != nil {
			return err
		}
		val := "listVal" + fmt.Sprintf("%07d", 0)
		if string(item) != val {
			t.Error("TestDB_Merge_For_List err")
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		item, err := tx.LPop(bucketForList, key)
		if err != nil {
			return err
		}
		val := "listVal" + fmt.Sprintf("%07d", 1)
		if string(item) != val {
			t.Error("TestDB_Merge_For_List err")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		item, err := tx.RPop(bucketForList, key)
		if err != nil {
			return err
		}

		val := "listVal" + fmt.Sprintf("%07d", 99)
		if string(item) != val {
			t.Error("TestDB_Merge_For_List err")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		item, err := tx.RPop(bucketForList, key)
		if err != nil {
			return err
		}

		val := "listVal" + fmt.Sprintf("%07d", 98)
		if string(item) != val {
			t.Error("TestDB_Merge_For_List err")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func opRPushAndCheckForTestMerge(bucketForList string, key []byte, t *testing.T) {
	for i := 0; i < 100; i++ {
		if err := db.Update(func(tx *Tx) error {
			val := []byte("listVal" + fmt.Sprintf("%07d", i))
			err := tx.RPush(bucketForList, key, val)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	if err := db.View(func(tx *Tx) error {
		list, err := tx.LRange(bucketForList, key, 0, 99)
		if len(list) != 100 {
			t.Error("TestDB_Merge_For_List err: ")
		}

		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDB_Merge_For_List(t *testing.T) {
	InitOpt("/tmp/nutsdbtestformergelist", true)

	readFlag := false
	mergeFlag := true

	bucketForList := "bucket_for_list_merge_test"
	key := []byte("key_for_list_merge_test")

	db, err = Open(opt)
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}

	if !readFlag {
		opRPushAndCheckForTestMerge(bucketForList, key, t)
	}

	opLPopAndRPopForTestMerge(bucketForList, key, t)

	if err := db.Update(func(tx *Tx) error {
		removedNum, err := tx.LRem(bucketForList, key, 1, []byte("listVal"+fmt.Sprintf("%07d", 33)))
		if removedNum != 1 {
			t.Fatal("removedNum err")
		}
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		list, err := tx.LRange(bucketForList, key, 0, 49)
		if len(list) != 50 {
			t.Error("TestDB_Merge_For_List err")
		}

		if err != nil {
			t.Error(err)
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if mergeFlag {
		if err = db.Merge(); err != nil {
			t.Error("err merge", err)
		}
	}
}

func TestTx_Get_KeyNotFound(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txGet(t, db, bucket, GetTestBytes(0), nil, ErrBucketNotFound)
		txPut(t, db, bucket, GetTestBytes(1), GetRandomBytes(24), Persistent, nil)
		txGet(t, db, bucket, GetTestBytes(0), nil, ErrKeyNotFound)
	})
}

func opStrDataForTestOpen(t *testing.T) {
	strBucket := "myStringBucket"
	if err := db.Update(func(tx *Tx) error {
		err := tx.Put(strBucket, []byte("key"), []byte("val"), Persistent)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func opRPushForTestOpen(listBucket string, t *testing.T) {
	if err := db.Update(func(tx *Tx) error {
		err := tx.RPush(listBucket, []byte("myList"), []byte("val1"))
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		err := tx.RPush(listBucket, []byte("myList"), []byte("val2"))
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		err := tx.RPush(listBucket, []byte("myList"), []byte("val3"))
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func opLPushAndLPopForTestOpen(listBucket string, t *testing.T) {
	if err := db.Update(func(tx *Tx) error {
		err := tx.LPush(listBucket, []byte("key"), []byte("val"))
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		item, err := tx.LPop(listBucket, []byte("key"))
		if string(item) != "val" {
			t.Error("TestOpen err")
		}
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func opListDataForTestOpen(t *testing.T) {
	listBucket := "myListBucket"

	opLPushAndLPopForTestOpen(listBucket, t)

	opRPushForTestOpen(listBucket, t)

	if err := db.Update(func(tx *Tx) error {
		_, err := tx.LRem(listBucket, []byte("myList"), 1, []byte("val1"))
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		err := tx.LTrim(listBucket, []byte("myList"), 0, 1)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		err := tx.RPush(listBucket, []byte("myList"), []byte("val4"))
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		item, err := tx.RPop(listBucket, []byte("myList"))
		if string(item) != "val4" || err != nil {
			t.Error("TestOpen err")
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		err := tx.RPush(listBucket, []byte("myList"), []byte("val5"))
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		err := tx.LSet(listBucket, []byte("myList"), 0, []byte("newVal"))
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func opZSetDataForTestOpen(t *testing.T) {
	zSetBucket := "myZSetBucket"
	if err := db.Update(func(tx *Tx) error {
		err := tx.ZAdd(zSetBucket, []byte("key1"), 1, []byte("val1"))
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		err := tx.ZAdd(zSetBucket, []byte("key2"), 2, []byte("val2"))
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		n, err := tx.ZPopMax(zSetBucket)
		if err != nil {
			return err
		}

		if string(n.Value) != "val2" {
			t.Error("TestOpen err")
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		n, err := tx.ZPopMin(zSetBucket)
		if err != nil {
			return err
		}

		if string(n.Value) != "val1" {
			t.Error("TestOpen err")
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		err := tx.ZAdd(zSetBucket, []byte("key3"), 3, []byte("val3"))
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		err := tx.ZRem(zSetBucket, "key3")
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func opSetDataForTestOpen(t *testing.T) {
	setBucket := "mySetBucket"
	key := []byte("myList")
	if err := db.Update(func(tx *Tx) error {
		err := tx.SAdd(setBucket, key, []byte("val"))
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		ok, err := tx.SIsMember(setBucket, key, []byte("val"))
		if err != nil || !ok {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		item, err := tx.SPop(setBucket, key)
		if err != nil || item != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		err := tx.SAdd(setBucket, key, []byte("val1"))
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		err := tx.SRem(setBucket, key, []byte("val1"))
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestOpen(t *testing.T) {
	InitOpt("/tmp/nutsdbtestfordbopen", true)

	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	opStrDataForTestOpen(t)

	opListDataForTestOpen(t)

	opZSetDataForTestOpen(t)

	opSetDataForTestOpen(t)

	db.Close()

	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDB_Backup(t *testing.T) {
	InitOpt("", false)
	db, err = Open(opt)
	defer db.Close()
	dir := "/tmp/nutsdbtest_backup"
	err = db.Backup(dir)
	if err != nil {
		t.Error("err TestDB_Backup")
	}
}

func TestDB_BackupTarGZ(t *testing.T) {
	InitOpt("", false)
	db, err = Open(opt)
	defer db.Close()
	if err != nil {
		t.Fatal("err TestDB_BackupTarGZ")
	}
	path := "/tmp/nutsdbtest_backup.tar.gz"
	f, _ := os.Create(path)
	defer f.Close()
	err = db.BackupTarGZ(f)
	if err != nil {
		t.Error("err TestDB_Backup")
	}
}

func TestDB_Close(t *testing.T) {
	InitOpt("", false)
	db, err = Open(opt)

	err = db.Close()
	if err != nil {
		t.Error("err TestDB_Close")
	}

	err = db.Close()
	if err == nil {
		t.Error("err TestDB_Close")
	}
}

func Test_getRecordFromKey(t *testing.T) {
	InitOpt("", true)
	db, err = Open(opt,
		WithSegmentSize(120),
		WithEntryIdxMode(HintKeyAndRAMIdxMode),
	)
	if err != nil {
		t.Errorf("wanted nil, got %v", err)
	}
	_, err = db.getRecordFromKey([]byte("bucket"), []byte("hello"))
	if err != ErrBucketNotFound {
		t.Errorf("wanted ErrBucketNotFound, got %v", err)
	}
	for i := 0; i < 10; i++ {
		err = db.Update(func(tx *Tx) error {
			if err := tx.Put("bucket", []byte("hello"), []byte("world"), Persistent); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Errorf("wanted nil, got %v", err)
		}
	}
	r, err := db.getRecordFromKey([]byte("bucket"), []byte("hello"))
	if err != nil {
		t.Errorf("wanted nil, got %v", err)
	}
	if r.H.DataPos != 58 || r.H.FileID != 4 {
		t.Errorf("wanted fileID: %d, got: %d\nwanted dataPos: %d, got: %d", 4, r.H.FileID, 58, r.H.DataPos)
	}
	err = db.Close()
	if err != nil {
		t.Errorf("wanted nil, got %v", err)
	}
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
	InitOpt("", true)
	db, err = Open(opt)
	require.NoError(t, err)

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
