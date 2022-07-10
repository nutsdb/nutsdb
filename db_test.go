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
	"os"
	"reflect"
	"strconv"
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
	InitOpt("", true)
	db, err = Open(opt)
	defer db.Close()

	require.NoError(t, err)

	bucket := "bucket1"
	key := []byte("key1")
	val := []byte("val1")

	//put
	err = db.Update(
		func(tx *Tx) error {
			return tx.Put(bucket, key, val, Persistent)
		})
	require.NoError(t, err)

	//get
	err = db.View(
		func(tx *Tx) error {
			e, err := tx.Get(bucket, key)
			if assert.NoError(t, err) {
				assert.EqualValuesf(t, val, e.Value, "err Tx Get. got %s want %s", string(e.Value), string(val))
			}
			return nil
		})
	require.NoError(t, err)

	// delete
	err = db.Update(
		func(tx *Tx) error {
			err := tx.Delete(bucket, key)
			require.NoError(t, err)
			return nil
		})
	require.NoError(t, err)

	err = db.View(
		func(tx *Tx) error {
			_, err := tx.Get(bucket, key)
			assert.Error(t, err, "err Tx Get.")
			return nil
		})
	require.NoError(t, err)

	//update
	val = []byte("val001")
	err = db.Update(
		func(tx *Tx) error {
			return tx.Put(bucket, key, val, Persistent)
		})
	require.NoError(t, err)

	err := db.View(
		func(tx *Tx) error {
			e, err := tx.Get(bucket, key)
			if assert.NoError(t, err) {
				assert.EqualValuesf(t, val, e.Value, "err Tx Get. got %s want %s", string(e.Value), string(val))
			}
			return nil
		})
	require.NoError(t, err)
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

	opt := DefaultOptions
	opt.Dir = fileDir
	opt.SegmentSize = 1 * 100

	db2, err := Open(opt)

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
	opt.SegmentSize = 120
	db, err = Open(opt)
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

func TestTx_Get_NotFound(t *testing.T) {
	InitOpt("", false)
	db, err = Open(opt)
	defer db.Close()

	if err != nil {
		t.Fatal(err)
	}
	bucket := "bucketfoo"
	key := []byte("keyfoo")
	//get
	if err := db.View(
		func(tx *Tx) error {
			e, err := tx.Get(bucket, key)
			if err == nil {
				t.Error("err TestTx_Get_Err")
			}
			if e != nil {
				t.Error("err TestTx_Get_Err")
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}

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

	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}
}

type SetMeta struct {
	value     []byte
	Timestamp uint64
	TTL       uint32
	Flag      uint16
}

func bulkPutSet(t *testing.T, bucket string, key []byte, data []SetMeta) {
	for _, meta := range data {
		if err := db.Update(func(tx *Tx) error {
			err := tx.put(bucket, key, meta.value, meta.TTL, meta.Flag, meta.Timestamp, DataStructureSet)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDB_OpenForSet_Case1(t *testing.T) {
	InitOpt("/tmp/nutsdbtestfordbopen", true)

	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	setBucket := "mySetBucket"
	key := []byte("mySetKey")
	now := uint64(time.Now().Unix())
	oneMinAgo := now - 60
	data := []SetMeta{
		{value: []byte("val1"), Timestamp: oneMinAgo, TTL: Persistent, Flag: DataSetFlag},
		{value: []byte("val2"), Timestamp: oneMinAgo + 30, TTL: Persistent, Flag: DataSetFlag},
		{value: []byte(strconv.Itoa(2)), Timestamp: oneMinAgo + 49, TTL: 10, Flag: DataSetExpireFlag}, // expired
	}
	bulkPutSet(t, setBucket, key, data)

	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, len(db.SetIdx[setBucket].M[string(key)]), 0)
	_, ok := db.SetIdx[setBucket].T[string(key)]
	assert.False(t, ok)
}

func TestDB_OpenForSet_Case2(t *testing.T) {
	InitOpt("/tmp/nutsdbtestfordbopen", true)

	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	setBucket := "mySetBucket"
	key := []byte("mySetKey")
	now := uint64(time.Now().Unix())
	oneMinAgo := now - 60
	data := []SetMeta{
		{value: []byte("val1"), Timestamp: oneMinAgo, TTL: Persistent, Flag: DataSetFlag},
		{value: []byte("val2"), Timestamp: oneMinAgo + 30, TTL: Persistent, Flag: DataSetFlag},
		{value: []byte(strconv.Itoa(2)), Timestamp: oneMinAgo + 49, TTL: 20, Flag: DataSetExpireFlag}, // still fresh
	}
	bulkPutSet(t, setBucket, key, data)

	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(db.SetIdx[setBucket].M[string(key)]))
	tsAndTtl, ok := db.SetIdx[setBucket].T[string(key)]
	assert.True(t, ok)
	assert.Equal(t, uint32(20), tsAndTtl.TTL)
	assert.Equal(t, oneMinAgo+49, tsAndTtl.Timestamp)
}

func TestDB_OpenForSet_Case3(t *testing.T) {
	InitOpt("/tmp/nutsdbtestfordbopen", true)

	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	setBucket := "mySetBucket"
	key := []byte("mySetKey")
	now := uint64(time.Now().Unix())
	oneMinAgo := now - 60
	data := []SetMeta{
		{value: []byte("val1"), Timestamp: oneMinAgo, TTL: Persistent, Flag: DataSetFlag},
		{value: []byte("val2"), Timestamp: oneMinAgo + 30, TTL: Persistent, Flag: DataSetFlag},
		{value: []byte(strconv.Itoa(2)), Timestamp: oneMinAgo + 40, TTL: 10, Flag: DataSetExpireFlag}, // expired
		{value: []byte(strconv.Itoa(2)), Timestamp: oneMinAgo + 50, TTL: 20, Flag: DataSetExpireFlag}, // reset to fresh
	}
	bulkPutSet(t, setBucket, key, data)

	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, len(db.SetIdx[setBucket].M[string(key)]), 2)
	tsAndTtl, ok := db.SetIdx[setBucket].T[string(key)]
	assert.True(t, ok)
	assert.Equal(t, uint32(20), tsAndTtl.TTL)
	assert.Equal(t, oneMinAgo+50, tsAndTtl.Timestamp)
}

func TestDB_OpenForSet_Case4(t *testing.T) {
	InitOpt("/tmp/nutsdbtestfordbopen", true)

	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	setBucket := "mySetBucket"
	key := []byte("mySetKey")
	now := uint64(time.Now().Unix())
	oneMinAgo := now - 60
	data := []SetMeta{
		{value: []byte("val1"), Timestamp: oneMinAgo, TTL: Persistent, Flag: DataSetFlag},
		{value: []byte("val2"), Timestamp: oneMinAgo + 30, TTL: Persistent, Flag: DataSetFlag},
		{value: []byte(strconv.Itoa(2)), Timestamp: oneMinAgo + 40, TTL: 10, Flag: DataSetExpireFlag}, // expired
		{value: []byte("val3"), Timestamp: oneMinAgo + 43, TTL: 7, Flag: DataSetFlag},
	}
	bulkPutSet(t, setBucket, key, data)

	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, len(db.SetIdx[setBucket].M[string(key)]), 0)
	_, ok := db.SetIdx[setBucket].T[string(key)]
	assert.False(t, ok)
}

func TestDB_OpenForSet_Case5(t *testing.T) {
	InitOpt("/tmp/nutsdbtestfordbopen", true)

	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	setBucket := "mySetBucket"
	key := []byte("mySetKey")
	now := uint64(time.Now().Unix())
	oneMinAgo := now - 60
	data := []SetMeta{
		{value: []byte("val1"), Timestamp: oneMinAgo, TTL: Persistent, Flag: DataSetFlag},
		{value: []byte("val2"), Timestamp: oneMinAgo + 30, TTL: Persistent, Flag: DataSetFlag},
		{value: []byte(strconv.Itoa(2)), Timestamp: oneMinAgo + 40, TTL: 10, Flag: DataSetExpireFlag}, // expired
		{value: []byte("val3"), Timestamp: oneMinAgo + 43, TTL: 7, Flag: DataSetFlag},
		{value: []byte(strconv.Itoa(3)), Timestamp: oneMinAgo + 59, TTL: Persistent, Flag: DataSetExpireFlag}, // reset to fresh
	}
	bulkPutSet(t, setBucket, key, data)

	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, len(db.SetIdx[setBucket].M[string(key)]), 3)
	_, ok := db.SetIdx[setBucket].T[string(key)]
	assert.False(t, ok)
}

func TestDB_OpenForSet_Complex_Case(t *testing.T) {
	InitOpt("/tmp/nutsdbtestfordbopen", true)

	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	setBucket := "mySetBucket"
	key := []byte("mySetKey")
	now := uint64(time.Now().Unix())
	tenMinAgo := now - 10*60
	data := []SetMeta{
		{value: []byte("val1"), Timestamp: tenMinAgo, TTL: Persistent, Flag: DataSetFlag},
		{value: []byte("val2"), Timestamp: tenMinAgo + 1, TTL: Persistent, Flag: DataSetFlag},
		{value: []byte(strconv.Itoa(2)), Timestamp: tenMinAgo + 2, TTL: 58, Flag: DataSetExpireFlag}, // expired, the set was deleted

		{value: []byte("val3"), Timestamp: tenMinAgo + 60, TTL: Persistent, Flag: DataSetFlag}, // create a new set
		{value: []byte("val4"), Timestamp: tenMinAgo + 61, TTL: Persistent, Flag: DataSetFlag},
		{value: []byte(strconv.Itoa(2)), Timestamp: tenMinAgo + 120, TTL: 60, Flag: DataSetExpireFlag}, // set to expired
		{value: []byte("val5"), Timestamp: tenMinAgo + 121, TTL: 59, Flag: DataSetFlag},
		{value: []byte("val6"), Timestamp: tenMinAgo + 122, TTL: 58, Flag: DataSetFlag},
		{value: []byte("val3"), Timestamp: tenMinAgo + 170, TTL: Persistent, Flag: DataDeleteFlag},             // delete a record
		{value: []byte(strconv.Itoa(3)), Timestamp: tenMinAgo + 179, TTL: Persistent, Flag: DataSetExpireFlag}, // reset to persistent
		{value: []byte("val7"), Timestamp: tenMinAgo + 200, TTL: Persistent, Flag: DataSetFlag},
		{value: []byte("val8"), Timestamp: tenMinAgo + 300, TTL: Persistent, Flag: DataSetFlag},
		{value: []byte("val6"), Timestamp: tenMinAgo + 301, TTL: Persistent, Flag: DataDeleteFlag},      // delete a record
		{value: []byte(strconv.Itoa(4)), Timestamp: tenMinAgo + 360, TTL: 600, Flag: DataSetExpireFlag}, // still fresh
		{value: []byte("val9"), Timestamp: tenMinAgo + 370, TTL: 590, Flag: DataSetFlag},
	}
	bulkPutSet(t, setBucket, key, data)

	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, len(db.SetIdx[setBucket].M[string(key)]), 5)
	tsAndTtl, ok := db.SetIdx[setBucket].T[string(key)]
	assert.True(t, ok)
	assert.Equal(t, uint32(600), tsAndTtl.TTL)
	assert.Equal(t, tenMinAgo+360, tsAndTtl.Timestamp)
}

func TestDB_Backup(t *testing.T) {
	InitOpt("", false)
	db, err = Open(opt)
	dir := "/tmp/nutsdbtest_backup"
	err = db.Backup(dir)
	if err != nil {
		t.Error("err TestDB_Backup")
	}
}

func TestDB_BackupTarGZ(t *testing.T) {
	InitOpt("", false)
	db, err = Open(opt)
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
	opt.SegmentSize = 120
	opt.EntryIdxMode = HintKeyAndRAMIdxMode
	db, err = Open(opt)
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
