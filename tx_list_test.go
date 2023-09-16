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
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func InitForList() {
	fileDir := "/tmp/nutsdbtestlisttx"
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

	opt = DefaultOptions
	opt.EntryIdxMode = HintKeyAndRAMIdxMode
	opt.Dir = fileDir
	opt.SegmentSize = 8 * 1024
	return
}

func TestTx_RPush(t *testing.T) {
	InitForList()
	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "myBucket"
	key := []byte("myList")

	if err := tx.RPush(bucket, []byte("myList"+SeparatorForListKey), []byte("a"), []byte("b"), []byte("c"), []byte("d")); err == nil {
		_ = tx.Rollback()
		t.Fatal("TestTx_RPush err")
	}

	if err := tx.RPush(bucket, key, []byte("a"), []byte("b"), []byte("c"), []byte("d")); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}

	if err := tx.RPush(bucket, []byte(""), []byte("a"), []byte("b"), []byte("c"), []byte("d")); err == nil {
		_ = tx.Rollback()
		t.Fatal("TestTx_RPush err")
	}

	_ = tx.Commit()

	err = tx.RPush(bucket, key, []byte("e"))
	if err == nil {
		t.Error("TestTx_RPush err")
	}

	checkPushResult(bucket, key, t)
}

func TestTx_LPush(t *testing.T) {
	InitForList()
	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "myBucket"
	key := []byte("myList")
	if err := tx.LPush(bucket, []byte("myList"+SeparatorForListKey), []byte("d"), []byte("c"), []byte("b"), []byte("a")); err == nil {
		t.Error("TestTx_LPush err")
	}

	if err := tx.LPush(bucket, key, []byte("d"), []byte("c"), []byte("b"), []byte("a")); err != nil {
		t.Fatal(err)
	}

	_ = tx.Commit()

	err = tx.LPush(bucket, key, []byte("e"))
	if err == nil {
		t.Error("TestTx_LPush err")
	}

	checkPushResult(bucket, key, t)
}

func checkPushResult(bucket string, key []byte, t *testing.T) {
	expectResult := []string{"a", "b", "c", "d"}
	for i := 0; i < len(expectResult); i++ {
		tx, err = db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}

		item, err := tx.LPop(bucket, key)
		if err != nil || string(item) != expectResult[i] {
			_ = tx.Rollback()
			t.Error("TestTx_LPush err")
		} else {
			_ = tx.Commit()
		}
	}
}

func TestTx_LPop(t *testing.T) {
	InitForList()
	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "myBucket"
	key := []byte("myList")

	_, err := tx.LPop(bucket, key)
	if err == nil {
		t.Error("TestTx_LPop err")
	}
	_ = tx.Rollback()

	InitDataForList(bucket, key, t)
	tx, err = db.Begin(true)
	if err != nil {
		t.Error(err)
	}
	item, err := tx.LPop(bucket, key)
	if err != nil || string(item) != "a" {
		_ = tx.Rollback()
		t.Error("TestTx_LPop err")
	} else {
		_ = tx.Commit()
	}

	tx, _ = db.Begin(true)
	item, err = tx.LPop(bucket, key)
	if err != nil || string(item) != "b" {
		_ = tx.Rollback()
		t.Error("TestTx_LPop err")
	} else {
		_ = tx.Commit()
		item, err = tx.LPop(bucket, key)
		if err == nil || item != nil {
			t.Error("TestTx_LPop err")
		}
	}

	tx, _ = db.Begin(true)
	item, err = tx.LPop(bucket, key)
	if err != nil || string(item) != "c" {
		_ = tx.Rollback()
		t.Error("TestTx_LPop err")
	} else {
		_ = tx.Commit()
	}

	tx, _ = db.Begin(true)
	item, err = tx.LPop(bucket, key)
	if err == nil || item != nil {
		_ = tx.Rollback()
		t.Fatal("TestTx_LPop err")
	}

	_ = tx.Commit()
}

func TestTx_LRange(t *testing.T) {
	InitForList()
	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "myBucket"
	key := []byte("myList")
	list, err := tx.LRange(bucket, key, 0, -1)
	if err == nil || list != nil {
		t.Error("TestTx_LRange err")
	}

	_ = tx.Rollback()

	InitDataForList(bucket, key, t)

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	list, err = tx.LRange(bucket, key, 0, -1)
	expectResult := []string{"a", "b", "c"}
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}

	for i := 0; i < len(expectResult); i++ {
		if string(list[i]) != expectResult[i] {
			t.Error("TestTx_LRange err")
		}
	}

	_ = tx.Commit()

	_, err = tx.LRange(bucket, key, 0, -1)
	if err == nil {
		t.Error("TestTx_LRange err")
	}
}

func TestTx_LRem(t *testing.T) {
	InitForList()
	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "myBucket"
	key := []byte("myList")
	err := tx.LRem(bucket, key, 1, []byte("val"))
	if err == nil {
		t.Fatal(err)
	}
	_ = tx.Rollback()
}

func TestTx_LRem2(t *testing.T) {
	InitForList()
	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	bucket := "myBucket"
	key := []byte("myList")

	InitDataForList(bucket, key, t)

	{
		tx, err = db.Begin(false)
		if err != nil {
			t.Fatal(err)
		}

		if list, err := tx.LRange(bucket, key, 0, -1); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		} else {
			expectResult := []string{"a", "b", "c"}
			for i := 0; i < len(expectResult); i++ {
				if string(list[i]) != expectResult[i] {
					t.Error("TestTx_LRem err")
				}
			}
			_ = tx.Commit()
		}
	}

	{
		tx, err = db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		if err = tx.LRem(bucket, []byte("fake_key"), 1, []byte("fake_val")); err == nil {
			t.Fatal("TestTx_LRem err")
		} else {
			_ = tx.Rollback()
		}
	}

	{
		tx, err = db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		if err = tx.LRem(bucket, key, 1, []byte("a")); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		} else {
			_ = tx.Commit()
		}

		tx, err = db.Begin(false)
		if err != nil {
			t.Fatal(err)
		}
		if list, err := tx.LRange(bucket, key, 0, -1); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		} else {
			expectResult := []string{"b", "c"}
			for i := 0; i < len(expectResult); i++ {
				if string(list[i]) != expectResult[i] {
					t.Error("TestTx_LRem err")
				}
			}
			_ = tx.Commit()
		}
	}
}

func TestTx_LRem3(t *testing.T) {
	InitForList()
	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	bucket := "myBucket"
	key := []byte("myList")

	InitDataForList(bucket, []byte("myList2"), t)

	{
		tx, err = db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}

		err := tx.LRem(bucket, key, 4, []byte("b"))
		if err == nil {
			t.Error("TestTx_LRem err")
		}
		_ = tx.Rollback()
	}

	{
		tx, err = db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}

		err := tx.LRem(bucket, []byte("myList2"), 4, []byte("b"))
		if err == nil {
			t.Error("TestTx_LRem err")
		}
		_ = tx.Rollback()
	}

	{
		tx, err = db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}

		err := tx.LRem(bucket, []byte("myList2"), 1, []byte("b"))
		if err != nil {
			t.Error("TestTx_LRem err")
		}
		_ = tx.Rollback()
	}

	InitDataForList(bucket, []byte("myList3"), t)

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	err = tx.RPush(bucket, []byte("myList3"), []byte("b"))
	if err != nil {
		_ = tx.Rollback()
		t.Error(err)
	} else {
		_ = tx.Commit()
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	err := tx.LRem(bucket, []byte("myList3"), 0, []byte("b"))
	if err != nil {
		t.Error("TestTx_LRem err")
	}
	_ = tx.Rollback()
}

func InitDataForList(bucket string, key []byte, t *testing.T) {
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	err = tx.RPush(bucket, key, []byte("a"), []byte("b"), []byte("c"))
	if err != nil {
		_ = tx.Rollback()
		t.Error(err)
	} else {
		_ = tx.Commit()
	}
}

func TestTx_LSet(t *testing.T) {
	InitForList()
	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	bucket := "myBucket"
	key := []byte("myList")
	err = tx.LSet("fake_bucket", key, 0, []byte("foo"))
	if err == nil {
		t.Error("TestTx_LSet err")
	}
	_ = tx.Rollback()

	InitDataForList(bucket, key, t)
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.LSet(bucket, key, -1, []byte("a1"))
	if err == nil {
		t.Error("TestTx_LSet err")
	}

	err = tx.LSet(bucket, []byte("fake_key"), 0, []byte("a1"))
	if err == nil {
		t.Error("TestTx_LSet err")
	}

	err = tx.LSet(bucket, key, 0, []byte("a1"))
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	} else {
		_ = tx.Commit()
		err = tx.LSet(bucket, key, 0, []byte("a1"))
		if err == nil {
			t.Error("TestTx_LSet err")
		}
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	list, err := tx.LRange(bucket, key, 0, -1)

	expectResult := []string{"a1", "b", "c"}
	for i := 0; i < len(expectResult); i++ {
		if string(list[i]) != expectResult[i] {
			t.Error("TestTx_LSet err")
		}
	}

	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	} else {
		_ = tx.Commit()
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.LSet(bucket, key, 100, []byte("a1"))
	_ = tx.Rollback()
	if err == nil {
		t.Fatal("TestTx_LSet err")
	}
}

func TestTx_LTrim(t *testing.T) {
	InitForList()
	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)

	bucket := "myBucket"
	key := []byte("myList")

	err := tx.LTrim(bucket, key, 0, 1)
	if err == nil {
		t.Error("TestTx_LTrim err")
	}
	_ = tx.Rollback()

	InitDataForList(bucket, key, t)

	tx, _ = db.Begin(true)

	err = tx.LTrim(bucket, []byte("fake_key"), 0, 1)
	if err == nil {
		t.Error("TestTx_LTrim err")
	}

	err = tx.LTrim(bucket, key, 0, 1)
	if err != nil {
		_ = tx.Rollback()
		t.Error("TestTx_LTrim err")
	}

	_ = tx.Commit()

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	list, err := tx.LRange(bucket, key, 0, -1)
	if err != nil {
		t.Fatal(err)
	}

	_ = tx.Commit()

	err = tx.LTrim(bucket, key, 0, 1)
	if err == nil {
		t.Error("TestTx_LTrim err")
	}

	expectResult := []string{"a", "b"}
	for i := 0; i < len(expectResult); i++ {
		if string(list[i]) != expectResult[i] {
			t.Error("TestTx_LTrim err")
		}
	}

	key = []byte("myList2")
	InitDataForList(bucket, key, t)

	tx, _ = db.Begin(true)

	err = tx.LTrim(bucket, key, 0, -10)
	if err == nil {
		t.Error("TestTx_LTrim err")
	}
	_ = tx.Rollback()
}

func TestTx_RPop(t *testing.T) {
	InitForList()
	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)

	bucket := "myBucket"
	key := []byte("myList")

	item, err := tx.RPop(bucket, key)
	if err == nil || item != nil {
		t.Error("TestTx_RPop err")
	}
	_ = tx.Rollback()

	InitDataForList(bucket, key, t)

	tx, _ = db.Begin(true)

	item, err = tx.RPop(bucket, []byte("fake_key"))
	if err == nil || item != nil {
		t.Error("TestTx_RPop err")
	}

	item, err = tx.RPop(bucket, key)
	if err != nil || string(item) != "c" {
		t.Error(err)
	}

	_ = tx.Commit()

	item, err = tx.RPop(bucket, key)
	if err == nil || item != nil {
		t.Error("TestTx_RPop err")
	}
}

func TestTx_LSize(t *testing.T) {
	InitForList()
	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "myBucket"
	key := []byte("myList")

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	size, err := tx.LSize(bucket, key)
	if err == nil || size != 0 {
		t.Error("TestTx_LSize err")
	}
	_ = tx.Rollback()

	InitDataForList(bucket, key, t)

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	size, err = tx.LSize(bucket, key)
	if err != nil {
		_ = tx.Rollback()
		t.Error(err)
	} else {
		if size != 3 {
			t.Error("TestTx_LSize err")
		}
		_ = tx.Commit()

		size, err = tx.LSize(bucket, key)
		if size != 0 || err == nil {
			t.Error("TestTx_LSize err")
		}
	}
}

func TestTx_LRemByIndex(t *testing.T) {
	InitForList()
	assertions := assert.New(t)
	db, err = Open(opt)
	assertions.NoError(err, "TestTx_LRemByIndex")

	tx, err = db.Begin(true)

	bucket := "myBucket"
	key := []byte("myList")

	err := tx.LRemByIndex(bucket, []byte("fake_key"))
	assertions.NoError(err)
	_ = tx.Rollback()

	InitDataForList(bucket, key, t)

	tx, _ = db.Begin(true)

	err = tx.LRemByIndex(bucket, key, 1, 0, 8, -8)
	assertions.NoError(err, "TestTx_LRemByIndex")

	err = tx.LRemByIndex(bucket, key, 88, -88)
	assertions.NoError(err, "TestTx_LRemByIndex")

	_ = tx.Commit()

	err = tx.LRemByIndex(bucket, key, 1, 0, 8, -8)
	assertions.Error(err, "TestTx_LRemByIndex")
}

func TestTx_ExpireList(t *testing.T) {
	InitForList()
	assertions := assert.New(t)
	db, err = Open(opt)
	assertions.NoError(err, "TestTx_ExpireList")
	tx, err = db.Begin(true)

	bucket := "myBucket"
	key := []byte("myList")
	if err := tx.LPush(bucket, key, []byte("d"), []byte("c"), []byte("b"), []byte("a")); err != nil {
		t.Error("TestTx_ExpireList err")
	}
	_ = tx.Commit()

	tx, _ = db.Begin(true)
	_, err := tx.LRange(bucket, key, 0, -1)
	if err != nil {
		t.Error("TestTx_ExpireList err")
	}
	err = tx.ExpireList(bucket, key, 1)
	if err != nil {
		t.Error("TestTx_ExpireList err")
	}
	_ = tx.Commit()

	time.Sleep(time.Second)

	tx, _ = db.Begin(false)
	_, err = tx.LRange(bucket, key, 0, -1)
	if err == nil {
		t.Error("TestTx_ExpireList err")
	}
	_ = tx.Commit()

	tx, _ = db.Begin(true)
	if err := tx.LPush(bucket, key, []byte("d"), []byte("c"), []byte("b"), []byte("a")); err != nil {
		t.Error("TestTx_ExpireList err")
	}
	_ = tx.Commit()

	tx, _ = db.Begin(true)
	err = tx.ExpireList(bucket, key, Persistent)
	if err != nil {
		t.Error("TestTx_ExpireList err")
	}
	_ = tx.Commit()

	time.Sleep(time.Second)

	tx, _ = db.Begin(true)
	_, err = tx.LRange(bucket, key, 0, -1)
	if err != nil {
		t.Error("TestTx_ExpireList err")
	}
	_ = tx.Commit()
}

func TestTx_LKeys(t *testing.T) {
	InitForList()
	assertions := assert.New(t)
	db, err = Open(opt)
	assertions.NoError(err, "TestTx_LKeys")
	bucket := "myBucket"
	InitDataForList(bucket, []byte("hello"), t)
	InitDataForList(bucket, []byte("hello1"), t)
	InitDataForList(bucket, []byte("hello12"), t)
	InitDataForList(bucket, []byte("hello123"), t)

	tx, err = db.Begin(false)

	var keys []string
	err = tx.LKeys(bucket, "*", func(key string) bool {
		keys = append(keys, key)
		return true
	})
	assertions.NoError(err, "TestTx_LKeys")
	assertions.Equal(4, len(keys), "TestTx_LKeys")

	keys = []string{}
	err = tx.LKeys(bucket, "*", func(key string) bool {
		keys = append(keys, key)
		return len(keys) != 2
	})
	assertions.NoError(err, "TestTx_LKeys")
	assertions.Equal(2, len(keys), "TestTx_LKeys")

	keys = []string{}
	err = tx.LKeys(bucket, "hello1*", func(key string) bool {
		keys = append(keys, key)
		return true
	})
	assertions.NoError(err, "TestTx_LKeys")
	assertions.Equal(3, len(keys), "TestTx_LKeys")

	_ = tx.Commit()
}

func TestTx_GetListTTL(t *testing.T) {
	InitForList()
	assertions := assert.New(t)
	db, err = Open(opt)
	assertions.NoError(err, "TestTx_GetListTTL")
	tx, err = db.Begin(true)

	bucket := "myBucket"
	key := []byte("myList")
	if err := tx.LPush(bucket, key, []byte("d"), []byte("c"), []byte("b"), []byte("a")); err != nil {
		t.Error("TestTx_GetListTTL err")
	}
	_ = tx.Commit()

	tx, _ = db.Begin(true)
	ttl, err := tx.GetListTTL(bucket, key)
	if err != nil {
		t.Error("TestTx_GetListTTL err")
	}
	// test for TLL is 0
	assertions.Equal(uint32(0), ttl)

	wantTLL := uint32(1)
	err = tx.ExpireList(bucket, key, wantTLL)
	if err != nil {
		t.Error("TestTx_GetListTTL err")
	}
	_ = tx.Commit()

	tx, _ = db.Begin(true)
	ttl, err = tx.GetListTTL(bucket, key)
	if err != nil {
		t.Error("TestTx_GetListTTL err")
	}
	// test while remain bigger than zero
	assertions.Equal(wantTLL, ttl)

	time.Sleep(3 * time.Second)
	ttl, err = tx.GetListTTL(bucket, key)

	// test for TLL is expired
	assertions.Equal(uint32(0), ttl)

	_ = tx.Commit()
}

func TestTx_ListEntryIdxMode_HintKeyValAndRAMIdxMode(t *testing.T) {
	bucket := "bucket"
	key := GetTestBytes(0)

	opts := DefaultOptions
	opts.EntryIdxMode = HintKeyValAndRAMIdxMode

	// HintKeyValAndRAMIdxMode
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		err := db.Update(func(tx *Tx) error {
			err := tx.LPush(bucket, key, []byte("d"), []byte("c"), []byte("b"), []byte("a"))
			require.NoError(t, err)

			return nil
		})
		require.NoError(t, err)

		listIdx := db.Index.list.getWithDefault(bucket)
		item, ok := listIdx.Items[string(key)].PopMin()
		r := item.r
		require.True(t, ok)
		require.NotNil(t, r.V)
		require.Equal(t, []byte("a"), r.V)
	})
}

func TestTx_ListEntryIdxMode_HintKeyAndRAMIdxMode(t *testing.T) {
	bucket := "bucket"
	key := GetTestBytes(0)

	opts := &DefaultOptions
	opts.EntryIdxMode = HintKeyAndRAMIdxMode

	// HintKeyAndRAMIdxMode
	runNutsDBTest(t, opts, func(t *testing.T, db *DB) {
		err := db.Update(func(tx *Tx) error {
			err := tx.LPush(bucket, key, []byte("d"), []byte("c"), []byte("b"), []byte("a"))
			require.NoError(t, err)

			return nil
		})
		// require.NoError(t, err)

		listIdx := db.Index.list.getWithDefault(bucket)
		item, ok := listIdx.Items[string(key)].PopMin()
		r := item.r
		require.True(t, ok)
		require.Nil(t, r.V)

		val, err := db.getValueByRecord(r)
		require.NoError(t, err)
		require.Equal(t, []byte("a"), val)
	})
}
