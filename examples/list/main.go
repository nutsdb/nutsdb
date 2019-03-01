package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/xujiajun/nutsdb"
)

var (
	db     *nutsdb.DB
	bucket string
	err    error
)

func init() {
	opt := nutsdb.DefaultOptions
	fileDir := "/tmp/nutsdb_example"

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
	opt.Dir = fileDir
	opt.SegmentSize = 1024 * 1024 // 1MB
	db, err = nutsdb.Open(opt)
	if err != nil {
		panic(err)
	}
	bucket = "bucketForList"
}

func main() {
	testRPushAndLPush()

	testLRange()

	testLPop()

	testRPop()

	testRPushItems()

	testLRem()

	testLRange()

	testLSet()

	testLPeek()

	testRPeek()

	testLTrim()

	testLRange()

	testLSize()
}

func testRPushAndLPush() {
	fmt.Println("RPushAndLPush init data")
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			val := []byte("val2")
			return tx.RPush(bucket, key, val)
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			val := []byte("val1")
			return tx.LPush(bucket, key, val)
		}); err != nil {
		log.Fatal(err)
	}
}

func testLRange() {
	fmt.Println("LRange:")
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			items, err := tx.LRange(bucket, key, 0, -1)
			if err != nil {
				return err
			}

			for _, item := range items {
				fmt.Println(string(item))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testLPop() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			item, err := tx.LPop(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("LPop item:", string(item)) //val1
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}
func testRPop() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			item, err := tx.RPop(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("RPop item:", string(item)) //val2
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testRPushItems() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			val := []byte("val1")
			return tx.RPush(bucket, key, val)
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			val := []byte("val2")
			return tx.RPush(bucket, key, val)
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			val := []byte("val3")
			return tx.RPush(bucket, key, val)
		}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("RPushItems 3 items ok")
}

func testLRem() {
	fmt.Println("LRem count 1: ")
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			return tx.LRem(bucket, key, 1)
		}); err != nil {
		log.Fatal(err)
	}
}

func testLSet() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			err := tx.LSet(bucket, key, 0, []byte("val11"))
			if err != nil {
				return err
			}
			fmt.Println("LSet ok, index 0 item value => val11")
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testLPeek() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			item, err := tx.LPeek(bucket, key)
			if err != nil {
				return err
			}

			fmt.Println("LPeek item:", string(item)) //val11
			return nil
		}); err != nil {
		log.Fatal(err)
	}

}

func testRPeek() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			item, err := tx.RPeek(bucket, key)
			if err != nil {
				return err
			}

			fmt.Println("RPeek item:", string(item)) //val2
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testLTrim() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			return tx.LTrim(bucket, key, 0, 1)
		}); err != nil {
		log.Fatal(err)
	}
}

func testLSize() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			size, err := tx.LSize(bucket, key)
			if err != nil {
				return err
			}

			fmt.Println("myList size is ", size)
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}
