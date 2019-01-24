package nutsdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

var (
	db  *DB
	opt Options
	err error
)

func InitOpt(isRemoveFiles bool) {
	fileDir := "/tmp/nutsdbtest"
	if isRemoveFiles {
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
	}

	opt = DefaultOptions
	opt.Dir = fileDir
	opt.SegmentSize = 8 * 1024
}

func TestDB_Basic(t *testing.T) {
	InitOpt(true)
	db, err = Open(opt)
	defer db.Close()

	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket1"
	key := []byte("key1")
	val := []byte("val1")

	//put
	if err := db.Update(
		func(tx *Tx) error {
			if err := tx.Put(bucket, key, val, Persistent); err != nil {
				return err
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}

	//get
	if err := db.View(
		func(tx *Tx) error {
			e, err := tx.Get(bucket, key)
			if err == nil {
				if string(e.Value) != string(val) {
					t.Errorf("err Tx Get. got %s want %s", string(e.Value), string(val))
				}
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}

	// delete
	if err := db.Update(
		func(tx *Tx) error {
			err := tx.Delete(bucket, key)
			if err != nil {
				t.Fatal(err)
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(
		func(tx *Tx) error {
			_, err := tx.Get(bucket, key)
			if err == nil {
				t.Errorf("err Tx Get.")
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}

	//update
	val = []byte("val001")
	if err := db.Update(
		func(tx *Tx) error {
			if err := tx.Put(bucket, key, val, Persistent); err != nil {
				return err
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(
		func(tx *Tx) error {
			e, err := tx.Get(bucket, key)
			if err == nil {
				if string(e.Value) != string(val) {
					t.Errorf("err Tx Get. got %s want %s", string(e.Value), string(val))
				}
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}
}

func TestDB_Merge(t *testing.T) {
	InitOpt(false)
	db, err = Open(opt)
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}

	bucket := "test_merge"

	//init batch put data
	for i := 0; i < 10000; i++ {
		if err := db.Update(
			func(tx *Tx) error {
				key := []byte("key_" + fmt.Sprintf("%07d", i))
				val := []byte("val" + fmt.Sprintf("%07d", i))
				if err := tx.Put(bucket, key, val, Persistent); err != nil {
					return err
				}
				return nil
			}); err != nil {
			t.Fatal(err)
		}
	}

	//init batch delete data
	for i := 0; i < 5000; i++ {
		if err := db.Update(
			func(tx *Tx) error {
				key := []byte("key_" + fmt.Sprintf("%07d", i))
				if err := tx.Delete(bucket, key); err != nil {
					t.Fatal(err)
					return err
				}
				return nil
			}); err != nil {
			t.Fatal(err)
		}
	}

	//check data
	for i := 0; i < 5000; i++ {
		if err := db.View(
			func(tx *Tx) error {
				key := []byte("key_" + fmt.Sprintf("%07d", i))
				if _, err := tx.Get(bucket, key); err == nil {
					t.Error("err read data ")
				}
				return nil
			}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 5000; i < 10000; i++ {
		if err := db.View(
			func(tx *Tx) error {
				key := []byte("key_" + fmt.Sprintf("%07d", i))
				if _, err := tx.Get(bucket, key); err != nil {
					t.Error("err read data ")
				}
				return nil
			}); err != nil {
			t.Fatal(err)
		}
	}

	//merge
	if err = db.Merge(); err != nil {
		t.Error("err merge", err)
	}

	//GetValidKeyCount
	validKeyNum, err := db.GetValidKeyCount(bucket)
	if err != nil {
		t.Fatal(err)
	}

	if validKeyNum != 5000 {
		t.Errorf("err GetValidKeyCount. got %d want %d", validKeyNum, 5000)
	}
}

func TestTx_Get_NotFound(t *testing.T) {
	InitOpt(false)
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

func TestOpen_Err(t *testing.T) {
	InitOpt(false)
	opt.Dir = ":/xx"
	db, err = Open(opt)
	if err == nil {
		t.Error("err TestOpen")
	}
}

func TestDB_Backup(t *testing.T) {
	InitOpt(false)
	db, err = Open(opt)
	dir := "/tmp/nutsdbtest_backup"
	err = db.Backup(dir)
	if err != nil {
		t.Error("err TestDB_Backup")
	}
}

func TestDB_Close(t *testing.T) {
	InitOpt(false)
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