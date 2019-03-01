package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/xujiajun/nutsdb"
)

var (
	db  *nutsdb.DB
	err error
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

}

func main() {
	testZAdd()

	testZCard()

	testZCount()

	testZGetByKey()

	testZMembers()

	testZPeekMax()

	testZPeekMin()

	testZPopMax()

	testZPopMin()

	testZRangeByRank()

	testZRangeByScore()

	testZRank()

	testZRem()

	testZRemRangeByRank()

	testZScore()

	testZRevRank()
}

func testZAdd() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			return tx.ZAdd(bucket, key, 1, []byte("val1"))
		}); err != nil {
		log.Fatal(err)
	}
}

func testZCard() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			num, err := tx.ZCard(bucket)
			if err != nil {
				return err
			}
			fmt.Println("ZCard num", num)
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZCount() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key2 := []byte("key2")
			return tx.ZAdd(bucket, key2, 1, []byte("val2"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			num, err := tx.ZCount(bucket, 0, 1, nil)
			if err != nil {
				return err
			}
			fmt.Println("ZCount num", num)
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZGetByKey() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			node, err := tx.ZGetByKey(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("ZGetByKey key1 val:", string(node.Value))
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key2")
			node, err := tx.ZGetByKey(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("ZGetByKey key2 val:", string(node.Value))
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZMembers() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			nodes, err := tx.ZMembers(bucket)
			if err != nil {
				return err
			}
			fmt.Println("ZMembers:", nodes)
			for _, node := range nodes {
				fmt.Println("member:", node.Key(), string(node.Value))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZPeekMax() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key3 := []byte("key3")
			return tx.ZAdd(bucket, key3, 3, []byte("val3"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			node, err := tx.ZPeekMax(bucket)
			if err != nil {
				return err
			}
			fmt.Println("ZPeekMax:", string(node.Value)) //val3
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZPeekMin() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			node, err := tx.ZPeekMin(bucket)
			if err != nil {
				return err
			}
			fmt.Println("ZPeekMin:", string(node.Value)) //val1
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZPopMax() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			node, err := tx.ZPopMax(bucket)
			if err != nil {
				return err
			}
			fmt.Println("ZPopMax:", string(node.Value)) //val3
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZPopMin() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			node, err := tx.ZPopMin(bucket)
			if err != nil {
				return err
			}
			fmt.Println("ZPopMin:", string(node.Value)) //val1
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZRangeByRank() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet2"
			key1 := []byte("key1")
			return tx.ZAdd(bucket, key1, 1, []byte("val1"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet2"
			key2 := []byte("key2")
			return tx.ZAdd(bucket, key2, 2, []byte("val2"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet2"
			key3 := []byte("key3")
			return tx.ZAdd(bucket, key3, 3, []byte("val3"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet2"
			nodes, err := tx.ZRangeByRank(bucket, 1, 2)
			if err != nil {
				return err
			}
			fmt.Println("ZRangeByRank nodes :", nodes)
			for _, node := range nodes {
				fmt.Println("item:", node.Key(), node.Score())
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZRangeByScore() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet3"
			key1 := []byte("key1")
			return tx.ZAdd(bucket, key1, 70, []byte("val1"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet3"
			key2 := []byte("key2")
			return tx.ZAdd(bucket, key2, 90, []byte("val2"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet3"
			key3 := []byte("key3")
			return tx.ZAdd(bucket, key3, 86, []byte("val3"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet3"
			nodes, err := tx.ZRangeByScore(bucket, 80, 100, nil)
			if err != nil {
				return err
			}
			fmt.Println("ZRangeByScore nodes :", nodes)
			for _, node := range nodes {
				fmt.Println("item:", node.Key(), node.Score())
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZRank() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet4"
			key1 := []byte("key1")
			return tx.ZAdd(bucket, key1, 70, []byte("val1"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet4"
			key2 := []byte("key2")
			return tx.ZAdd(bucket, key2, 90, []byte("val2"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet4"
			key3 := []byte("key3")
			return tx.ZAdd(bucket, key3, 86, []byte("val3"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet4"
			key3 := []byte("key3")
			rank, err := tx.ZRank(bucket, key3)
			if err != nil {
				return err
			}
			fmt.Println("key3 ZRank :", rank)
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet4"
			key2 := []byte("key2")
			rank, err := tx.ZRank(bucket, key2)
			if err != nil {
				return err
			}
			fmt.Println("key2 ZRank :", rank)
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet4"
			key1 := []byte("key1")
			rank, err := tx.ZRank(bucket, key1)
			if err != nil {
				return err
			}
			fmt.Println("key1 ZRank :", rank)
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZRem() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet5"
			key1 := []byte("key1")
			return tx.ZAdd(bucket, key1, 10, []byte("val1"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet5"
			key2 := []byte("key2")
			return tx.ZAdd(bucket, key2, 20, []byte("val2"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet5"
			nodes, err := tx.ZMembers(bucket)
			if err != nil {
				return err
			}

			fmt.Println("before ZRem key1, ZMembers nodes", nodes)
			for _, node := range nodes {
				fmt.Println("item:", node.Key(), node.Score())
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet5"
			return tx.ZRem(bucket, "key1")
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet5"
			nodes, err := tx.ZMembers(bucket)
			if err != nil {
				return err
			}
			fmt.Println("after ZRem key1, ZMembers nodes", nodes)
			for _, node := range nodes {
				fmt.Println("item:", node.Key(), node.Score())
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZRemRangeByRank() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet6"
			key1 := []byte("key1")
			return tx.ZAdd(bucket, key1, 10, []byte("val1"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet6"
			key2 := []byte("key2")
			return tx.ZAdd(bucket, key2, 20, []byte("val2"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet6"
			key3 := []byte("key3")
			return tx.ZAdd(bucket, key3, 30, []byte("val3"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet6"
			nodes, err := tx.ZMembers(bucket)
			if err != nil {
				return err
			}

			fmt.Println("before ZRemRangeByRank, ZMembers nodes", nodes)
			for _, node := range nodes {
				fmt.Println("item:", node.Key(), node.Score())
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet6"
			return tx.ZRemRangeByRank(bucket, 1, 2)
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet6"
			nodes, err := tx.ZMembers(bucket)
			if err != nil {
				return err
			}
			fmt.Println("after ZRemRangeByRank, ZMembers nodes", nodes)
			for _, node := range nodes {
				fmt.Println("item:", node.Key(), node.Score())
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZScore() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet7"
			key1 := []byte("key1")
			return tx.ZAdd(bucket, key1, 10, []byte("val1"))
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet7"
			score, err := tx.ZScore(bucket, []byte("key1"))
			if err != nil {
				return err
			}
			fmt.Println("ZScore key1 score:", score)
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZRevRank() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet8"
			key1 := []byte("key1")
			return tx.ZAdd(bucket, key1, 10, []byte("val1"))
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet8"
			key2 := []byte("key2")
			return tx.ZAdd(bucket, key2, 20, []byte("val2"))
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet8"
			key3 := []byte("key3")
			return tx.ZAdd(bucket, key3, 30, []byte("val3"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet8"
			rank, err := tx.ZRevRank(bucket, []byte("key1"))
			if err != nil {
				return err
			}
			fmt.Println("ZRevRank key1 rank:", rank) //ZRevRank key1 rank: 3
			return nil
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet8"
			rank, err := tx.ZRevRank(bucket, []byte("key2"))
			if err != nil {
				return err
			}
			fmt.Println("ZRevRank key2 rank:", rank) //ZRevRank key2 rank: 2
			return nil
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet8"
			rank, err := tx.ZRevRank(bucket, []byte("key3"))
			if err != nil {
				return err
			}
			fmt.Println("ZRevRank key3 rank:", rank) //ZRevRank key3 rank: 1
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}
