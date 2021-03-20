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
		tx.Rollback()
		t.Fatal("TestTx_RPush err")
	}

	if err := tx.RPush(bucket, key, []byte("a"), []byte("b"), []byte("c"), []byte("d")); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}

	if err := tx.RPush(bucket, []byte(""), []byte("a"), []byte("b"), []byte("c"), []byte("d")); err == nil {
		tx.Rollback()
		t.Fatal("TestTx_RPush err")
	}

	tx.Commit()

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

	tx.Commit()

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
			tx.Rollback()
			t.Error("TestTx_LPush err")
		} else {
			tx.Commit()
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
	tx.Rollback()

	InitDataForList(bucket, key, t)
	tx, err = db.Begin(true)
	if err != nil {
		t.Error(err)
	}
	item, err := tx.LPop(bucket, key)
	if err != nil || string(item) != "a" {
		tx.Rollback()
		t.Error("TestTx_LPop err")
	} else {
		tx.Commit()
	}

	tx, _ = db.Begin(true)
	item, err = tx.LPop(bucket, key)
	if err != nil || string(item) != "b" {
		tx.Rollback()
		t.Error("TestTx_LPop err")
	} else {
		tx.Commit()
		item, err = tx.LPop(bucket, key)
		if err == nil || item != nil {
			t.Error("TestTx_LPop err")
		}
	}

	tx, _ = db.Begin(true)
	item, err = tx.LPop(bucket, key)
	if err != nil || string(item) != "c" {
		tx.Rollback()
		t.Error("TestTx_LPop err")
	} else {
		tx.Commit()
	}

	tx, _ = db.Begin(true)
	item, err = tx.LPop(bucket, key)
	if err == nil || item != nil {
		tx.Rollback()
		t.Fatal("TestTx_LPop err")
	}

	tx.Commit()
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

	tx.Rollback()

	InitDataForList(bucket, key, t)

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	list, err = tx.LRange(bucket, key, 0, -1)
	expectResult := []string{"a", "b", "c"}
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}

	for i := 0; i < len(expectResult); i++ {
		if string(list[i]) != string(expectResult[i]) {
			t.Error("TestTx_LRange err")
		}
	}

	tx.Commit()

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
	_, err := tx.LRem(bucket, key, 1, []byte("val"))
	if err == nil {
		t.Fatal(err)
	}
	tx.Rollback()
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
			tx.Rollback()
			t.Fatal(err)
		} else {
			expectResult := []string{"a", "b", "c"}
			for i := 0; i < len(expectResult); i++ {
				if string(list[i]) != expectResult[i] {
					t.Error("TestTx_LRem err")
				}
			}
			tx.Commit()
		}
	}

	{
		tx, err = db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.LRem(bucket, []byte("fake_key"), 1, []byte("fake_val")); err == nil {
			t.Fatal("TestTx_LRem err")
		} else {
			tx.Rollback()
		}
	}

	{
		tx, err = db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.LRem(bucket, key, 1, []byte("a")); err != nil {
			tx.Rollback()
			t.Fatal(err)
		} else {
			tx.Commit()
		}

		tx, err = db.Begin(false)
		if err != nil {
			t.Fatal(err)
		}
		if list, err := tx.LRange(bucket, key, 0, -1); err != nil {
			tx.Rollback()
			t.Fatal(err)
		} else {
			expectResult := []string{"b", "c"}
			for i := 0; i < len(expectResult); i++ {
				if string(list[i]) != expectResult[i] {
					t.Error("TestTx_LRem err")
				}
			}
			tx.Commit()
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

		_, err := tx.LRem(bucket, key, 4, []byte("b"))
		if err == nil {
			t.Error("TestTx_LRem err")
		}
		tx.Rollback()
	}

	{
		tx, err = db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}

		_, err := tx.LRem(bucket, []byte("myList2"), 4, []byte("b"))
		if err == nil {
			t.Error("TestTx_LRem err")
		}
		tx.Rollback()
	}

	{
		tx, err = db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}

		num, err := tx.LRem(bucket, []byte("myList2"), 1, []byte("b"))
		if err != nil || num != 1 {
			t.Error("TestTx_LRem err")
		}
		tx.Rollback()
	}

	InitDataForList(bucket, []byte("myList3"), t)

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	err = tx.RPush(bucket, []byte("myList3"), []byte("b"))
	if err != nil {
		tx.Rollback()
		t.Error(err)
	} else {
		tx.Commit()
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	num, err := tx.LRem(bucket, []byte("myList3"), 0, []byte("b"))
	if err != nil || num != 2 {
		t.Error("TestTx_LRem err")
	}
	tx.Rollback()
}

func InitDataForList(bucket string, key []byte, t *testing.T) {
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	err = tx.RPush(bucket, key, []byte("a"), []byte("b"), []byte("c"))
	if err != nil {
		tx.Rollback()
		t.Error(err)
	} else {
		tx.Commit()
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
	tx.Rollback()

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
		tx.Rollback()
		t.Fatal(err)
	} else {
		tx.Commit()
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
		if string(list[i]) != string(expectResult[i]) {
			t.Error("TestTx_LSet err")
		}
	}

	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	} else {
		tx.Commit()
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.LSet(bucket, key, 100, []byte("a1"))
	tx.Rollback()
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
	tx.Rollback()

	InitDataForList(bucket, key, t)

	tx, _ = db.Begin(true)

	err = tx.LTrim(bucket, []byte("fake_key"), 0, 1)
	if err == nil {
		t.Error("TestTx_LTrim err")
	}

	err = tx.LTrim(bucket, key, 0, 1)
	if err != nil {
		tx.Rollback()
		t.Error("TestTx_LTrim err")
	}

	tx.Commit()

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	list, err := tx.LRange(bucket, key, 0, -1)
	if err != nil {
		t.Fatal(err)
	}

	tx.Commit()

	err = tx.LTrim(bucket, key, 0, 1)
	if err == nil {
		t.Error("TestTx_LTrim err")
	}

	expectResult := []string{"a", "b"}
	for i := 0; i < len(expectResult); i++ {
		if string(list[i]) != string(expectResult[i]) {
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
	tx.Rollback()
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
	tx.Rollback()

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

	tx.Commit()

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
	tx.Rollback()

	InitDataForList(bucket, key, t)

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	size, err = tx.LSize(bucket, key)
	if err != nil {
		tx.Rollback()
		t.Error(err)
	} else {
		if size != 3 {
			t.Error("TestTx_LSize err")
		}
		tx.Commit()

		size, err = tx.LSize(bucket, key)
		if size != 0 || err == nil {
			t.Error("TestTx_LSize err")
		}
	}
}
