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
	bucket := "bucket_rollback_test"

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

	_, err = tx.PrefixScan(bucket, []byte("foo"), 1)
	if err == nil {
		t.Error("err TestTx_Close")
	}
}
