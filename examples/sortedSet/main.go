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
	ZAdd()

	ZCard()

	ZCount()

	ZGetByKey()

	ZMembers()

	ZPeekMax()

	ZPeekMin()

	ZPopMax()

	ZPopMin()

	ZRangeByRank()

	ZRangeByScore()

	ZRank()

	ZRem()

	ZRemRangeByRank()

	ZScore()

	ZRevRank()
}

func ZAdd() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			if err := tx.ZAdd(bucket, key, 1, []byte("val1")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func ZCard() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			if num, err := tx.ZCard(bucket); err != nil {
				return err
			} else {
				fmt.Println("ZCard num", num)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func ZCount() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key2 := []byte("key2")
			if err := tx.ZAdd(bucket, key2, 1, []byte("val2")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			if num, err := tx.ZCount(bucket, 0, 1, nil); err != nil {
				return err
			} else {
				fmt.Println("ZCount num", num)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func ZGetByKey() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key1")
			if node, err := tx.ZGetByKey(bucket, key); err != nil {
				return err
			} else {
				fmt.Println("ZGetByKey key1 val:", string(node.Value))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key := []byte("key2")
			if node, err := tx.ZGetByKey(bucket, key); err != nil {
				return err
			} else {
				fmt.Println("ZGetByKey key2 val:", string(node.Value))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func ZMembers() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			if nodes, err := tx.ZMembers(bucket); err != nil {
				return err
			} else {
				fmt.Println("ZMembers:", nodes)

				for _, node := range nodes {
					fmt.Println("member:", node.Key(), string(node.Value))
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func ZPeekMax() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			key3 := []byte("key3")
			if err := tx.ZAdd(bucket, key3, 3, []byte("val3")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			if node, err := tx.ZPeekMax(bucket); err != nil {
				return err
			} else {
				fmt.Println("ZPeekMax:", string(node.Value)) //val3
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func ZPeekMin() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			if node, err := tx.ZPeekMin(bucket); err != nil {
				return err
			} else {
				fmt.Println("ZPeekMin:", string(node.Value)) //val1
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func ZPopMax() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			if node, err := tx.ZPopMax(bucket); err != nil {
				return err
			} else {
				fmt.Println("ZPopMax:", string(node.Value)) //val3
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func ZPopMin() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet1"
			if node, err := tx.ZPopMin(bucket); err != nil {
				return err
			} else {
				fmt.Println("ZPopMin:", string(node.Value)) //val1
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func ZRangeByRank() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet2"
			key1 := []byte("key1")
			if err := tx.ZAdd(bucket, key1, 1, []byte("val1")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet2"
			key2 := []byte("key2")
			if err := tx.ZAdd(bucket, key2, 2, []byte("val2")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet2"
			key3 := []byte("key3")
			if err := tx.ZAdd(bucket, key3, 3, []byte("val3")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet2"
			if nodes, err := tx.ZRangeByRank(bucket, 1, 2); err != nil {
				return err
			} else {
				fmt.Println("ZRangeByRank nodes :", nodes)
				for _, node := range nodes {
					fmt.Println("item:", node.Key(), node.Score())
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func ZRangeByScore() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet3"
			key1 := []byte("key1")
			if err := tx.ZAdd(bucket, key1, 70, []byte("val1")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet3"
			key2 := []byte("key2")
			if err := tx.ZAdd(bucket, key2, 90, []byte("val2")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet3"
			key3 := []byte("key3")
			if err := tx.ZAdd(bucket, key3, 86, []byte("val3")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet3"
			if nodes, err := tx.ZRangeByScore(bucket, 80, 100, nil); err != nil {
				return err
			} else {
				fmt.Println("ZRangeByScore nodes :", nodes)
				for _, node := range nodes {
					fmt.Println("item:", node.Key(), node.Score())
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func ZRank() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet4"
			key1 := []byte("key1")
			if err := tx.ZAdd(bucket, key1, 70, []byte("val1")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet4"
			key2 := []byte("key2")
			if err := tx.ZAdd(bucket, key2, 90, []byte("val2")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet4"
			key3 := []byte("key3")
			if err := tx.ZAdd(bucket, key3, 86, []byte("val3")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet4"
			key3 := []byte("key3")
			if rank, err := tx.ZRank(bucket, key3); err != nil {
				return err
			} else {
				fmt.Println("key3 ZRank :", rank)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet4"
			key2 := []byte("key2")
			if rank, err := tx.ZRank(bucket, key2); err != nil {
				return err
			} else {
				fmt.Println("key2 ZRank :", rank)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet4"
			key1 := []byte("key1")
			if rank, err := tx.ZRank(bucket, key1); err != nil {
				return err
			} else {
				fmt.Println("key1 ZRank :", rank)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func ZRem() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet5"
			key1 := []byte("key1")
			if err := tx.ZAdd(bucket, key1, 10, []byte("val1")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet5"
			key2 := []byte("key2")
			if err := tx.ZAdd(bucket, key2, 20, []byte("val2")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet5"
			if nodes, err := tx.ZMembers(bucket); err != nil {
				return err
			} else {
				fmt.Println("before ZRem key1, ZMembers nodes", nodes)
				for _, node := range nodes {
					fmt.Println("item:", node.Key(), node.Score())
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet5"
			if err := tx.ZRem(bucket, "key1"); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet5"
			if nodes, err := tx.ZMembers(bucket); err != nil {
				return err
			} else {
				fmt.Println("after ZRem key1, ZMembers nodes", nodes)
				for _, node := range nodes {
					fmt.Println("item:", node.Key(), node.Score())
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func ZRemRangeByRank() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet6"
			key1 := []byte("key1")
			if err := tx.ZAdd(bucket, key1, 10, []byte("val1")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet6"
			key2 := []byte("key2")
			if err := tx.ZAdd(bucket, key2, 20, []byte("val2")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet6"
			key3 := []byte("key3")
			if err := tx.ZAdd(bucket, key3, 30, []byte("val2")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet6"
			if nodes, err := tx.ZMembers(bucket); err != nil {
				return err
			} else {
				fmt.Println("before ZRemRangeByRank, ZMembers nodes", nodes)
				for _, node := range nodes {
					fmt.Println("item:", node.Key(), node.Score())
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet6"
			if err := tx.ZRemRangeByRank(bucket, 1, 2); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet6"
			if nodes, err := tx.ZMembers(bucket); err != nil {
				return err
			} else {
				fmt.Println("after ZRemRangeByRank, ZMembers nodes", nodes)
				for _, node := range nodes {
					fmt.Println("item:", node.Key(), node.Score())
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func ZScore() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet7"
			key1 := []byte("key1")
			if err := tx.ZAdd(bucket, key1, 10, []byte("val1")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet7"
			if score, err := tx.ZScore(bucket, []byte("key1")); err != nil {
				return err
			} else {
				fmt.Println("ZScore key1 score:", score)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func ZRevRank()  {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet8"
			key1 := []byte("key1")
			if err := tx.ZAdd(bucket, key1, 10, []byte("val1")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet8"
			key2 := []byte("key2")
			if err := tx.ZAdd(bucket, key2, 20, []byte("val2")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet8"
			key3 := []byte("key3")
			if err := tx.ZAdd(bucket, key3, 30, []byte("val3")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet8"
			if rank, err := tx.ZRevRank(bucket, []byte("key1")); err != nil {
				return err
			} else {
				fmt.Println("ZScore key1 rank:", rank) //ZScore key1 rank: 3
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet8"
			if rank, err := tx.ZRevRank(bucket, []byte("key2")); err != nil {
				return err
			} else {
				fmt.Println("ZScore key1 rank:", rank) //ZScore key2 rank: 2
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			bucket := "myZSet8"
			if rank, err := tx.ZRevRank(bucket, []byte("key3")); err != nil {
				return err
			} else {
				fmt.Println("ZScore key1 rank:", rank) //ZScore key3 rank: 1
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

