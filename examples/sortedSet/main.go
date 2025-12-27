package main

import (
	"fmt"
	"log"
	"os"

	"github.com/nutsdb/nutsdb"
)

var (
	db  *nutsdb.DB
	err error
)

func init() {
	fileDir := "/tmp/nutsdb_example"

	files, _ := os.ReadDir(fileDir)
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

}

func main() {
	testZAdd()

	testZScore()

	testZCard()

	testZCount()

	testZMembers()

	testZPeekMax()

	testZPeekMin()

	testZPopMax()

	testZPopMin()

	testZRangeByRank()

	testZRangeByScore()

	testZRank()

	testZRevRank()

	testZRem()

	testZRemRangeByRank()
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
			key := []byte("key1")
			num, err := tx.ZCard(bucket, key)
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
			key := []byte("key1")
			return tx.ZAdd(bucket, key, 1, []byte("val2"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			num, err := tx.ZCount(bucket, key, 0, 1, nil)
			if err != nil {
				return err
			}
			fmt.Println("ZCount num", num)
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZScore() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			score, err := tx.ZScore(bucket, key, []byte("val1"))
			if err != nil {
				return err
			}
			fmt.Println("val1 score: ", score)
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZMembers() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			nodes, err := tx.ZMembers(bucket, key)
			if err != nil {
				return err
			}
			for node := range nodes {
				fmt.Println("member:", node.Score, string(node.Value))
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
			key := []byte("key1")
			return tx.ZAdd(bucket, key, 3, []byte("val3"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			node, err := tx.ZPeekMax(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("ZPeekMax:", node.Score) // val3
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZPeekMin() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			node, err := tx.ZPeekMin(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("ZPeekMin:", node.Score) // val1
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZPopMax() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			node, err := tx.ZPopMax(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("ZPopMax:", node.Score) // val3
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZPopMin() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			node, err := tx.ZPopMin(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("ZPopMin:", node.Score) // val1
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZRangeByRank() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			return tx.ZAdd(bucket, key, 1, []byte("val1"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			return tx.ZAdd(bucket, key, 2, []byte("val2"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			return tx.ZAdd(bucket, key, 3, []byte("val3"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			nodes, err := tx.ZRangeByRank(bucket, key, 1, 3)
			if err != nil {
				return err
			}
			for _, node := range nodes {
				fmt.Println("item:", string(node.Value), node.Score)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZRangeByScore() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			return tx.ZAdd(bucket, key, 70, []byte("val1"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			return tx.ZAdd(bucket, key, 90, []byte("val2"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			return tx.ZAdd(bucket, key, 86, []byte("val3"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			nodes, err := tx.ZRangeByScore(bucket, key, 80, 100, nil)
			if err != nil {
				return err
			}
			for _, node := range nodes {
				fmt.Println("item:", string(node.Value), node.Score)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZRank() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			rank, err := tx.ZRank(bucket, key, []byte("val1"))
			if err != nil {
				return err
			}
			fmt.Println("val1 ZRank :", rank)
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			rank, err := tx.ZRank(bucket, key, []byte("val2"))
			if err != nil {
				return err
			}
			fmt.Println("val2 ZRank :", rank)
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			rank, err := tx.ZRank(bucket, key, []byte("val3"))
			if err != nil {
				return err
			}
			fmt.Println("val3 ZRank :", rank)
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZRevRank() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			rank, err := tx.ZRank(bucket, key, []byte("val1"))
			if err != nil {
				return err
			}
			fmt.Println("ZRevRank val1 rank:", rank) // ZRevRank key1 rank: 3
			return nil
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			rank, err := tx.ZRank(bucket, key, []byte("val2"))
			if err != nil {
				return err
			}
			fmt.Println("ZRevRank val2 rank:", rank) // ZRevRank key2 rank: 2
			return nil
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			rank, err := tx.ZRank(bucket, key, []byte("val3"))
			if err != nil {
				return err
			}
			fmt.Println("ZRevRank val3 rank:", rank) // ZRevRank key3 rank: 1
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZRem() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			return tx.ZRem(bucket, key, []byte("val3"))
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			nodes, err := tx.ZMembers(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("after ZRem key1, ZMembers nodes")
			for node := range nodes {
				fmt.Println("item:", node.Score, string(node.Value))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func testZRemRangeByRank() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			return tx.ZRemRangeByRank(bucket, key, 1, 2)
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			nodes, err := tx.ZMembers(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("after ZRemRangeByRank, ZMembers nodes is 0")
			for node := range nodes {
				fmt.Println("item:", node.Score, string(node.Value))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}
