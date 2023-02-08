package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/nutsdb/nutsdb"
)

var (
	db     *nutsdb.DB
	bucket string
	err    error
)

func init() {
	fileDir := "/tmp/nutsdb_example"

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
	db, _ = nutsdb.Open(
		nutsdb.DefaultOptions,
		nutsdb.WithDir(fileDir),
		nutsdb.WithSegmentSize(1024*1024), // 1MB
	)
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

	testLRemByIndex()

	testLKeys()
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
			val := []byte("val4")
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
			fmt.Println("LPop item:", string(item)) // val1
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
			fmt.Println("RPop item:", string(item)) // val2
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testRPushItems() {
	val1 := []byte("val1")
	val2 := []byte("val2")
	val3 := []byte("val3")
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			return tx.RPush(bucket, key, val1)
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			return tx.RPush(bucket, key, val2)
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			return tx.RPush(bucket, key, val3)
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			return tx.RPush(bucket, key, val2)
		}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("RPushItems 4 items: ", string(val1), string(val2), string(val3), string(val2))
}

func testLRem() {
	value := []byte("val2")
	count := -1
	// count := 1
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			num, err := tx.LRem(bucket, key, count, value)
			fmt.Println("removed num: ", num)
			return err
		}); err != nil {
		log.Fatal(err)
	}
	if count < 0 {
		count = -count
	}
	fmt.Println("LRem count : ", count, string(value))
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

			fmt.Println("LPeek item:", string(item)) // val11
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

			fmt.Println("RPeek item:", string(item)) // val2
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testLTrim() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			return tx.LTrim(bucket, key, 0, 2)
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

func testLRemByIndex() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("myList")
			removedNum, err := tx.LRemByIndex(bucket, key, 0)
			fmt.Printf("removed num %d\n", removedNum)
			return err
		}); err != nil {
		log.Fatal(err)
	}
}

func testLKeys() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			var keys []string
			err := tx.LKeys(bucket, "*", func(key string) bool {
				keys = append(keys, key)
				// true: continue, false: break
				return true
			})
			fmt.Printf("keys: %v\n", keys)
			return err
		}); err != nil {
		log.Fatal(err)
	}
}
