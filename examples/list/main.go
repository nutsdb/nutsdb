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
	RPushAndLPush()

	LRange()

	LPop()

	RPop()

	RPushItems()

	LRem()

	LRange()

	LSet()

	LPeek()

	RPeek()

	LTrim()

	LRange()

	LSize()
}

func RPushAndLPush() {
	fmt.Println("RPushAndLPush init data")
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			val := []byte("val2")
			if err := tx.RPush(bucket, key, val); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			val := []byte("val1")
			if err := tx.LPush(bucket, key, val); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func LRange() {
	fmt.Println("LRange:")
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			if items, err := tx.LRange(bucket, key, 0, -1); err != nil {
				return err
			} else {
				//fmt.Println(items)
				for _, item := range items {
					fmt.Println(string(item))
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func LPop() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			if item, err := tx.LPop(bucket, key); err != nil {
				return err
			} else {
				fmt.Println("LPop item:", string(item)) //val1
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}
func RPop() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			if item, err := tx.RPop(bucket, key); err != nil {
				return err
			} else {
				fmt.Println("RPop item:", string(item)) //val2
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func RPushItems() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			val := []byte("val1")
			if err := tx.RPush(bucket, key, val); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			val := []byte("val2")
			if err := tx.RPush(bucket, key, val); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			val := []byte("val3")
			if err := tx.RPush(bucket, key, val); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("RPushItems 3 items ok")
}

func LRem() {
	fmt.Println("LRem count 1: ")
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			if err := tx.LRem(bucket, key, 1); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func LSet() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			if err := tx.LSet(bucket, key, 0, []byte("val11")); err != nil {
				return err
			} else {
				fmt.Println("LSet ok, index 0 item value => val11")
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func LPeek() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			if item, err := tx.LPeek(bucket, key); err != nil {
				return err
			} else {
				fmt.Println("LPeek item:", string(item)) //val11
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

}

func RPeek() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			if item, err := tx.RPeek(bucket, key); err != nil {
				return err
			} else {
				fmt.Println("RPeek item:", string(item)) //val2
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func LTrim() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			if err := tx.LTrim(bucket, key, 0, 1); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func LSize() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			if size, err := tx.LSize(bucket, key); err != nil {
				return err
			} else {
				fmt.Println("myList size is ", size)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}
