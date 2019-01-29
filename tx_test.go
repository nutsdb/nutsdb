// Copyright 2019 The nutsdb Authors. All rights reserved.
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
	"testing"

	"github.com/xujiajun/utils/strconv2"
)

func Init() {
	fileDir := "/tmp/nutsdbtesttx"
	files, _ := ioutil.ReadDir(fileDir)
	for _, f := range files {
		name := f.Name()
		if name != "" {
			err := os.Remove(fileDir + "/" + name)
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

func TestTx_PutAndGet(t *testing.T) {
	Init()
	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	//write tx begin
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket1"
	key := []byte("key1")
	val := []byte("val1")

	if err = tx.Put(bucket, key, val, Persistent); err != nil {
		//tx rollback
		err = tx.Rollback()
		t.Fatal(err)
	} else {
		//tx commit
		tx.Commit()
	}

	//read tx begin
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	if e, err := tx.Get(bucket, key); err != nil {
		//tx rollback
		err = tx.Rollback()
	} else {
		//tx commit
		tx.Commit()

		if string(e.Value) != string(val) {
			t.Errorf("err tx.Get. got %s want %s", string(e.Value), string(val))
		}
	}

	//test db close
	db.Close()

	if err = tx.Put(bucket, key, val, Persistent); err != nil {
		err = tx.Rollback()
	} else {
		t.Error("err tx.Put")
	}

}

func TestTx_BatchPutsAndScans(t *testing.T) {
	db, err = Open(opt)
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}

	//write tx begin
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket2"

	for i := 0; i <= 10000; i++ {
		key := []byte("key_" + fmt.Sprintf("%07d", i))
		val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
		if err = tx.Put(bucket, key, val, Persistent); err != nil {
			//tx rollback
			err = tx.Rollback()
			t.Fatal(err)
		}
	}
	//tx commit
	tx.Commit()

	//range scan error
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	start := []byte("key_0010001")
	end := []byte("key_0010010")
	if _, err := tx.RangeScan(bucket, start, end); err != nil {
		//tx rollback
		err = tx.Rollback()
	} else {
		t.Error("err range scan")
	}

	//range scan ok
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	start = []byte("key_0000001")
	end = []byte("key_0000010")
	if entries, err := tx.RangeScan(bucket, start, end); err != nil {
		//tx rollback
		err = tx.Rollback()
	} else {
		keys, _ := SortedEntryKeys(entries)
		j := 0
		for i := 1; i <= 10; i++ {
			key := []byte("key_" + fmt.Sprintf("%07d", i))
			if string(key) != keys[j] {
				t.Errorf("err tx RangeScan. got %s want %s", keys[j], string(key))
			}
			j++
		}
	}
	//tx commit
	tx.Commit()

	//prefix scan
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	prefix := []byte("key_")
	if entries, err := tx.PrefixScan(bucket, prefix, 10); err != nil {
		//tx rollback
		err = tx.Rollback()
	} else {
		keys, _ := SortedEntryKeys(entries)
		j := 0
		for i := 0; i < 10; i++ {
			key := []byte("key_" + fmt.Sprintf("%07d", i))
			if string(key) != keys[j] {
				t.Errorf("err tx RangeScan. got %s want %s", keys[j], string(key))
			}
			j++
		}
	}
	//tx commit
	tx.Commit()
}

func TestTx_DeleteAndGet(t *testing.T) {
	InitOpt(false)
	db, err = Open(opt)

	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}

	//write tx begin
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket_delete_test"

	for i := 0; i <= 10; i++ {
		key := []byte("key_" + fmt.Sprintf("%07d", i))
		val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
		if err = tx.Put(bucket, key, val, Persistent); err != nil {
			//tx rollback
			err = tx.Rollback()
			t.Fatal(err)
		}
	}
	//tx commit
	tx.Commit()

	//read tx begin
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	bucket = "bucket_delete_test"

	for i := 0; i <= 10; i++ {
		key := []byte("key_" + fmt.Sprintf("%07d", i))
		if entry, err := tx.Get(bucket, key); err != nil {
			//tx rollback
			err = tx.Rollback()
			t.Fatal(err)
		} else {
			val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
			if string(val) != string(entry.Value) {
				t.Error("err read tx ")
			}
		}
	}

	//tx commit
	tx.Commit()

	//delete

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= 5; i++ {
		key := []byte("key_" + fmt.Sprintf("%07d", i))
		if err = tx.Delete(bucket, key); err != nil {
			//tx rollback
			err = tx.Rollback()
			t.Fatal(err)
		}
	}

	//tx commit
	tx.Commit()

	for i := 0; i <= 5; i++ {
		tx, err = db.Begin(false)
		if err != nil {
			t.Fatal(err)
		}
		key := []byte("key_" + fmt.Sprintf("%07d", i))
		if _, err := tx.Get(bucket, key); err != nil {
			//tx rollback
			err = tx.Rollback()

		} else {
			t.Error("err read tx ")
		}
	}

	for i := 5; i <= 10; i++ {
		tx, err = db.Begin(false)
		if err != nil {
			t.Fatal(err)
		}
		key := []byte("key_" + fmt.Sprintf("%07d", i))
		if entry, err := tx.Get(bucket, key); err != nil {
			//tx rollback
			err = tx.Rollback()
		} else {
			val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
			if string(val) != string(entry.Value) {
				t.Error("err read tx ")
			}
			//tx commit
			tx.Commit()
		}
	}
}

func TestTx_GetAndScansFromMmap(t *testing.T) {
	Init()
	opt.EntryIdxMode = HintAndMemoryMapIdxMode
	db, err = Open(opt)
	defer db.Close()

	if err != nil {
		t.Fatal(err)
	}
	//write tx begin
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket_get_test"

	for i := 0; i <= 10; i++ {
		key := []byte("key_" + fmt.Sprintf("%07d", i))
		val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
		if err = tx.Put(bucket, key, val, Persistent); err != nil {
			//tx rollback
			err = tx.Rollback()
			t.Fatal(err)
		}
	}
	//tx commit
	tx.Commit()

	for i := 0; i <= 10; i++ {
		tx, err = db.Begin(false)
		if err != nil {
			t.Fatal(err)
		}
		key := []byte("key_" + fmt.Sprintf("%07d", i))
		if entry, err := tx.Get(bucket, key); err != nil {
			//tx rollback
			err = tx.Rollback()

		} else {
			val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
			if string(val) != string(entry.Value) {
				t.Error("err read tx ")
			}
		}
		//tx commit
		tx.Commit()
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	start := []byte("key_0000001")
	end := []byte("key_0000010")
	if entries, err := tx.RangeScan(bucket, start, end); err != nil {
		//tx rollback
		err = tx.Rollback()
	} else {
		keys, _ := SortedEntryKeys(entries)
		j := 0
		for i := 1; i <= 10; i++ {
			key := []byte("key_" + fmt.Sprintf("%07d", i))
			if string(key) != keys[j] {
				t.Errorf("err tx RangeScan. got %s want %s", keys[j], string(key))
			}
			j++
		}
	}
	//tx commit
	tx.Commit()
}

func TestTx_Put_Err(t *testing.T) {
	Init()
	db, err = Open(opt)
	defer db.Close()

	if err != nil {
		t.Fatal(err)
	}
	//write tx begin err setting here
	tx, err := db.Begin(false) //tx not writable
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket_get_test2"

	key := []byte("key_" + fmt.Sprintf("%07d", 0))
	val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 0))
	if err = tx.Put(bucket, key, val, Persistent); err != nil {
		//tx rollback
		err = tx.Rollback()
	} else {
		t.Fatal("err TestTx_Put_Err")
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket = "bucket_get_test2"

	key = []byte("") //key cannot be empty
	val = []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 0))
	if err = tx.Put(bucket, key, val, Persistent); err != nil {
		//tx rollback
		err = tx.Rollback()
	} else {
		t.Fatal("err TestTx_Put_Err")
	}

	//too big size
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	key = []byte("key_bigone")
	var bigVal string
	for i := 1; i <= 9*1024; i++ {
		bigVal += "val" + strconv2.IntToStr(i)
	}

	tx.Put(bucket, key, []byte(bigVal), Persistent)

	if err = tx.Commit(); err != nil {
		err = tx.Rollback()
	} else {
		t.Error("err put too big val")
	}

}

func TestTx_PrefixScan_NotFound(t *testing.T) {
	Init()
	db, err = Open(opt)
	defer db.Close()
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	prefix := []byte("key_")
	if entries, err := tx.PrefixScan("foobucket", prefix, 10); err != nil {
		//tx rollback
		if entries != nil {
			t.Error("err TestTx_PrefixScan_NotFound")
		}
		err = tx.Rollback()
	} else {
		t.Error("err TestTx_PrefixScan_NotFound")
	}
	//tx commit
	tx.Commit()

	//write tx begin
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket_prefix_scan_test"

	for i := 0; i <= 10; i++ {
		key := []byte("key_" + fmt.Sprintf("%07d", i))
		val := []byte("val" + fmt.Sprintf("%07d", i))
		if err = tx.Put(bucket, key, val, Persistent); err != nil {
			//tx rollback
			err = tx.Rollback()
			t.Fatal(err)
		}
	}
	//tx commit
	tx.Commit()

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	prefix = []byte("key_foo")
	if entries, err := tx.PrefixScan(bucket, prefix, 10); err != nil {
		//tx rollback
		if entries != nil {
			t.Error("err TestTx_PrefixScan_NotFound")
		}
		err = tx.Rollback()
	} else {
		t.Error("err TestTx_PrefixScan_NotFound")
	}
}

func TestTx_RangeScan_NotFound(t *testing.T) {
	Init()
	db, err = Open(opt)
	defer db.Close()

	//write tx begin
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket_range_scan_test"

	for i := 0; i <= 10; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val" + fmt.Sprintf("%03d", i))
		if err = tx.Put(bucket, key, val, Persistent); err != nil {
			//tx rollback
			err = tx.Rollback()
			t.Fatal(err)
		}
	}
	//tx commit
	tx.Commit()

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	start := []byte("key_011")
	end := []byte("key_012")
	_, err = tx.RangeScan(bucket, start, end)
	if err != nil {
		tx.Rollback()
	} else {
		tx.Commit()
		t.Error("err TestTx_RangeScan_NotFound")
	}
}

func TestTx_Rollback(t *testing.T) {
	Init()
	db, err = Open(opt)
	defer db.Close()

	tx, err := db.Begin(true)
	bucket := "bucket_rollbuck_test"

	for i := 0; i < 10; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_" + fmt.Sprintf("%03d", i))
		if i == 7 {
			key = []byte("") // set error key to make tx rollback
		}
		if err = tx.Put(bucket, key, val, Persistent); err != nil {
			//tx rollback
			err = tx.Rollback()
			if i < 7 {
				t.Fatal("err TestTx_Rollback")
			}
		}
	}

	// no one found
	for i := 0; i <= 10; i++ {
		tx, err = db.Begin(false)
		if err != nil {
			t.Fatal(err)
		}
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		if _, err := tx.Get(bucket, key); err != nil {
			//tx rollback
			err = tx.Rollback()
		} else {
			t.Fatal("err TestTx_Rollback")
		}
	}
}

func TestTx_Begin(t *testing.T) {
	Init()
	db, err = Open(opt)
	tx, err := db.Begin(false)
	if err != nil {
		t.Error("err when tx begin")
	}

	tx.Rollback()

	err = db.Close()
	if err != nil {
		t.Error("err when db close")
	}

	err = db.Close()
	if err == nil {
		t.Error("err when db close")
	}

	_, err = db.Begin(false)
	if err == nil {
		t.Error("err when db closed and tx begin")
	}

	//error
	Init()
	opt.NodeNum = -1
	db, err = Open(opt)
	if err != nil {
		fmt.Println("err", err)
	}
	tx, err = db.Begin(false)
	if err == nil {
		t.Error("err when tx begin")
	}
}

func TestTx_Close(t *testing.T) {
	Init()
	db, err = Open(opt)
	tx, err := db.Begin(false)
	if err != nil {
		t.Error("err when tx begin")
	}

	tx.Rollback()
	bucket := "bucket_tx_close_test"

	_, err = tx.Get(bucket, []byte("foo"))
	if err == nil {
		t.Error("err TestTx_Close")
	}

	_, err = tx.RangeScan(bucket, []byte("foo0"), []byte("foo1"))
	if err == nil {
		t.Error("err TestTx_Close")
	}

	_, err = tx.PrefixScan(bucket, []byte("foo"), 1)
	if err == nil {
		t.Error("err TestTx_Close")
	}
}
