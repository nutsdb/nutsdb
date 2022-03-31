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
	"testing"
)

func TestTx_Rollback(t *testing.T) {
	Init()
	db, err = Open(opt)
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	bucket := "bucket_rollback_test"

	for i := 0; i < 10; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_" + fmt.Sprintf("%03d", i))
		if i == 7 {
			key = []byte("") // set error key to make tx rollback
		}
		if err = tx.Put(bucket, key, val, Persistent); err != nil {
			//tx rollback
			tx.Rollback()
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
			tx.Rollback()
		} else {
			t.Fatal("err TestTx_Rollback")
		}
	}
}

func TestTx_Begin(t *testing.T) {
	fileDir := "/tmp/nutsdbtesttx"
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
	opt.NodeNum = -1

	db, err = Open(opt)
	tx, err = db.Begin(false)
	if err == nil {
		t.Error("err when tx begin")
	}

	opt.NodeNum = 1
	db, err = Open(opt)
	tx, err = db.Begin(false)
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

	_, _, err = tx.PrefixScan(bucket, []byte("foo"), 0, 1)
	if err == nil {
		t.Error("err TestTx_Close")
	}

	_, _, err = tx.PrefixSearchScan(bucket, []byte("f"), "oo", 0, 1)
	if err == nil {
		t.Error("err TestTx_Close")
	}

}

func TestTx_CommittedStatus(t *testing.T) {

	tmpdir, _ := ioutil.TempDir("", "nutsdbtesttx_committed")
	opt := DefaultOptions
	opt.Dir = tmpdir
	opt.EntryIdxMode = HintKeyAndRAMIdxMode

	db, err := Open(opt)
	if err != nil {
		t.Fatalf("open db error: %v", err)
	}
	defer func() {
		os.RemoveAll(tmpdir)
		db.Close()
	}()

	bucket := "bucket_committed_status"

	{ // setup data

		tx, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}

		if err = tx.Put(bucket, []byte("key1"), []byte("value1"), 0); err != nil {
			t.Errorf("put key error: %v", err)
		}
		if err = tx.Put(bucket, []byte("key2"), []byte("value2"), 0); err != nil {
			t.Errorf("put key error: %v", err)
		}

		if err = tx.Commit(); err != nil {
			t.Errorf("commit error: %v", err)
		}
	}

	{ // check data

		tx, _ := db.Begin(false)

		entry1, err := tx.Get(bucket, []byte("key1"))
		if err != nil {
			t.Errorf("get entry error: %v", err)
		}
		if entry1.Meta.Status != UnCommitted {
			t.Error("not the last entry should be uncommitted")
		}

		entry2, err := tx.Get(bucket, []byte("key2"))
		if err != nil {
			t.Errorf("get entry error: %v", err)
		}
		if entry2.Meta.Status != Committed {
			t.Error("the last entry should be committed")
		}

		// check committedTxIds
		txID := entry1.Meta.TxID
		if _, ok := tx.db.committedTxIds[txID]; !ok {
			t.Error("should be committed tx")
		}

		if err = tx.Commit(); err != nil {
			t.Errorf("commit error: %v", err)
		}

	}

}
