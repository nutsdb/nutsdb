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
	bucket = "bucketForSet"
}

func main() {
	testSAdd()

	testSAreMembers()

	testSCard()

	testSDiffByOneBucket()

	testSDiffByTwoBuckets()

	testSHasKey()

	testSIsMember()

	testSMembers()

	testSMoveByOneBucket()

	testSMoveByTwoBuckets()

	testSPop()

	testSRem()

	testSUnionByOneBucket()

	testSUnionByTwoBucket()
}

func testSAdd() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("mySet")
			return tx.SAdd(bucket, key, []byte("a"), []byte("b"), []byte("c"))
		}); err != nil {
		log.Fatal(err)
	}
}

func testSAreMembers() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("mySet")
			ok, err := tx.SAreMembers(bucket, key, []byte("a"), []byte("b"), []byte("c"))
			if err != nil {
				return err
			}
			fmt.Println("SAreMembers:", ok)
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testSCard() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("mySet")
			num, err := tx.SCard(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("SCard:", num)
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testSDiffByOneBucket() {
	key1 := []byte("mySet1")
	key2 := []byte("mySet2")

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.SAdd(bucket, key1, []byte("a"), []byte("b"), []byte("c"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.SAdd(bucket, key2, []byte("c"), []byte("d"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			items, err := tx.SDiffByOneBucket(bucket, key1, key2)
			if err != nil {
				return err
			}
			fmt.Println("SDiffByOneBucket:", items)
			for _, item := range items {
				fmt.Println("item", string(item))
			}
			//item a
			//item b
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testSDiffByTwoBuckets() {
	bucket1 := "bucket1"
	key1 := []byte("mySet1")

	bucket2 := "bucket2"
	key2 := []byte("mySet2")

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.SAdd(bucket1, key1, []byte("a"), []byte("b"), []byte("c"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.SAdd(bucket2, key2, []byte("c"), []byte("d"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			items, err := tx.SDiffByTwoBuckets(bucket1, key1, bucket2, key2)
			if err != nil {
				return err
			}
			fmt.Println("SDiffByTwoBuckets:", items)
			for _, item := range items {
				fmt.Println("item", string(item))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

}

func testSHasKey() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			ok, err := tx.SHasKey(bucket, []byte("fakeSet"))
			if err != nil {
				return err
			}
			fmt.Println("SHasKey", ok)
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			ok, err := tx.SHasKey(bucket, []byte("mySet"))
			if err != nil {
				return err
			}
			fmt.Println("SHasKey", ok)
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testSIsMember() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			ok, err := tx.SIsMember(bucket, []byte("mySet"), []byte("d"))
			if err != nil {
				fmt.Println("SIsMember", false)
			}
			fmt.Println("SIsMember", ok)
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			ok, err := tx.SIsMember(bucket, []byte("mySet"), []byte("a"))
			if err != nil {
				return err
			}
			fmt.Println("SIsMember", ok)
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testSMembers() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			items, err := tx.SMembers(bucket, []byte("mySet"))
			if err != nil {
				return err
			}
			fmt.Println("SMembers", items)
			for _, item := range items {
				fmt.Println("item", string(item))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testSMoveByOneBucket() {
	bucket3 := "bucket3"
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.SAdd(bucket3, []byte("mySet1"), []byte("a"), []byte("b"), []byte("c"))
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.SAdd(bucket3, []byte("mySet2"), []byte("c"), []byte("d"), []byte("e"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			ok, err := tx.SMoveByOneBucket(bucket3, []byte("mySet1"), []byte("mySet2"), []byte("a"))
			if err != nil {
				return err
			}
			fmt.Println("SMoveByOneBucket", ok)
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			items, err := tx.SMembers(bucket3, []byte("mySet1"))
			if err != nil {
				return err
			}

			fmt.Println("after SMoveByOneBucket bucket3 mySet1 SMembers", items)
			for _, item := range items {
				fmt.Println("item", string(item))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			items, err := tx.SMembers(bucket3, []byte("mySet2"))
			if err != nil {
				return err
			}
			fmt.Println("after SMoveByOneBucket bucket3 mySet2 SMembers", items)
			for _, item := range items {
				fmt.Println("item", string(item))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testSMoveByTwoBuckets() {
	bucket4 := "bucket4"
	bucket5 := "bucket5"
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.SAdd(bucket4, []byte("mySet1"), []byte("a"), []byte("b"), []byte("c"))
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.SAdd(bucket5, []byte("mySet2"), []byte("c"), []byte("d"), []byte("e"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			ok, err := tx.SMoveByTwoBuckets(bucket4, []byte("mySet1"), bucket5, []byte("mySet2"), []byte("a"))
			if err != nil {
				return err
			}
			fmt.Println("SMoveByTwoBuckets", ok)
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			items, err := tx.SMembers(bucket4, []byte("mySet1"))
			if err != nil {
				return err
			}

			fmt.Println("after SMoveByTwoBuckets bucket4 mySet1 SMembers", items)
			for _, item := range items {
				fmt.Println("item", string(item))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			items, err := tx.SMembers(bucket5, []byte("mySet2"))
			if err != nil {
				return err
			}

			fmt.Println("after SMoveByTwoBuckets bucket5 mySet2 SMembers", items)
			for _, item := range items {
				fmt.Println("item", string(item))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testSPop() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("mySet")
			item, err := tx.SPop(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("SPop item from mySet:", string(item))
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testSRem() {
	bucket6 := "bucket6"
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.SAdd(bucket6, []byte("mySet"), []byte("a"), []byte("b"), []byte("c"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			if err := tx.SRem(bucket6, []byte("mySet"), []byte("a")); err != nil {
				return err
			}
			fmt.Println("SRem ok")
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			items, err := tx.SMembers(bucket6, []byte("mySet"))
			if err != nil {
				return err
			}
			fmt.Println("SMembers items:", items)
			for _, item := range items {
				fmt.Println("item:", string(item))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testSUnionByOneBucket() {
	bucket7 := "bucket1"
	key1 := []byte("mySet1")
	key2 := []byte("mySet2")

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.SAdd(bucket7, key1, []byte("a"), []byte("b"), []byte("c"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.SAdd(bucket7, key2, []byte("c"), []byte("d"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			items, err := tx.SUnionByOneBucket(bucket7, key1, key2)
			if err != nil {
				return err
			}
			fmt.Println("SUnionByOneBucket:", items)
			for _, item := range items {
				fmt.Println("item", string(item))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testSUnionByTwoBucket() {
	bucket8 := "bucket1"
	key1 := []byte("mySet1")

	bucket9 := "bucket2"
	key2 := []byte("mySet2")

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.SAdd(bucket8, key1, []byte("a"), []byte("b"), []byte("c"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.SAdd(bucket9, key2, []byte("c"), []byte("d"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			items, err := tx.SUnionByTwoBuckets(bucket8, key1, bucket9, key2)
			if err != nil {
				return err
			}
			fmt.Println("SUnionByTwoBucket:", items)
			for _, item := range items {
				fmt.Println("item", string(item))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}
