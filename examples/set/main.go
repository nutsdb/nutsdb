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
	SAdd()

	SAreMembers()

	SCard()

	SDiffByOneBucket()

	SDiffByTwoBuckets()

	SHasKey()

	SIsMember()

	SMembers()

	SMoveByOneBucket()

	SMoveByTwoBuckets()

	SPop()

	SRem()

	SUnionByOneBucket()

	SUnionByTwoBucket()
}

func SAdd() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("mySet")
			if err := tx.SAdd(bucket, key, []byte("a"), []byte("b"), []byte("c")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func SAreMembers() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("mySet")
			if ok, err := tx.SAreMembers(bucket, key, []byte("a"), []byte("b"), []byte("c")); err != nil {
				return err
			} else {
				fmt.Println("SAreMembers:", ok)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func SCard() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("mySet")
			if num, err := tx.SCard(bucket, key); err != nil {
				return err
			} else {
				fmt.Println("SCard:", num)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func SDiffByOneBucket() {
	key1 := []byte("mySet1")
	key2 := []byte("mySet2")

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			if err := tx.SAdd(bucket, key1, []byte("a"), []byte("b"), []byte("c")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			if err := tx.SAdd(bucket, key2, []byte("c"), []byte("d")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			if items, err := tx.SDiffByOneBucket(bucket, key1, key2); err != nil {
				return err
			} else {
				fmt.Println("SDiffByOneBucket:", items)
				for _, item := range items {
					fmt.Println("item", string(item))
				}
				//item a
				//item b
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func SDiffByTwoBuckets() {
	bucket1 := "bucket1"
	key1 := []byte("mySet1")

	bucket2 := "bucket2"
	key2 := []byte("mySet2")

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			if err := tx.SAdd(bucket1, key1, []byte("a"), []byte("b"), []byte("c")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			if err := tx.SAdd(bucket2, key2, []byte("c"), []byte("d")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			if items, err := tx.SDiffByTwoBuckets(bucket1, key1, bucket2, key2); err != nil {
				return err
			} else {
				fmt.Println("SDiffByTwoBuckets:", items)
				for _, item := range items {
					fmt.Println("item", string(item))
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

}

func SHasKey() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			if ok, err := tx.SHasKey(bucket, []byte("fakeSet")); err != nil {
				return err
			} else {
				fmt.Println("SHasKey", ok)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			if ok, err := tx.SHasKey(bucket, []byte("mySet")); err != nil {
				return err
			} else {
				fmt.Println("SHasKey", ok)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func SIsMember() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			if ok, err := tx.SIsMember(bucket, []byte("mySet"), []byte("d")); err != nil {
				fmt.Println("SIsMember", false)
			} else {
				fmt.Println("SIsMember", ok)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			if ok, err := tx.SIsMember(bucket, []byte("mySet"), []byte("a")); err != nil {
				return err
			} else {
				fmt.Println("SIsMember", ok)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func SMembers() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			if items, err := tx.SMembers(bucket, []byte("mySet")); err != nil {
				return err
			} else {
				fmt.Println("SMembers", items)
				for _, item := range items {
					fmt.Println("item", string(item))
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func SMoveByOneBucket() {
	bucket3 := "bucket3"
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			if err := tx.SAdd(bucket3, []byte("mySet1"), []byte("a"), []byte("b"), []byte("c")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			if err := tx.SAdd(bucket3, []byte("mySet2"), []byte("c"), []byte("d"), []byte("e")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			if ok, err := tx.SMoveByOneBucket(bucket3, []byte("mySet1"), []byte("mySet2"), []byte("a")); err != nil {
				return err
			} else {
				fmt.Println("SMoveByOneBucket", ok)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			if items, err := tx.SMembers(bucket3, []byte("mySet1")); err != nil {
				return err
			} else {
				fmt.Println("after SMoveByOneBucket bucket3 mySet1 SMembers", items)
				for _, item := range items {
					fmt.Println("item", string(item))
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			if items, err := tx.SMembers(bucket3, []byte("mySet2")); err != nil {
				return err
			} else {
				fmt.Println("after SMoveByOneBucket bucket3 mySet2 SMembers", items)
				for _, item := range items {
					fmt.Println("item", string(item))
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func SMoveByTwoBuckets() {
	bucket4 := "bucket4"
	bucket5 := "bucket5"
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			if err := tx.SAdd(bucket4, []byte("mySet1"), []byte("a"), []byte("b"), []byte("c")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			if err := tx.SAdd(bucket5, []byte("mySet2"), []byte("c"), []byte("d"), []byte("e")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			if ok, err := tx.SMoveByTwoBuckets(bucket4, []byte("mySet1"), bucket5, []byte("mySet2"), []byte("a")); err != nil {
				return err
			} else {
				fmt.Println("SMoveByTwoBuckets", ok)
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			if items, err := tx.SMembers(bucket4, []byte("mySet1")); err != nil {
				return err
			} else {
				fmt.Println("after SMoveByTwoBuckets bucket4 mySet1 SMembers", items)
				for _, item := range items {
					fmt.Println("item", string(item))
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			if items, err := tx.SMembers(bucket5, []byte("mySet2")); err != nil {
				return err
			} else {
				fmt.Println("after SMoveByTwoBuckets bucket5 mySet2 SMembers", items)
				for _, item := range items {
					fmt.Println("item", string(item))
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func SPop() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("mySet")
			if item, err := tx.SPop(bucket, key); err != nil {
				return err
			} else {
				fmt.Println("SPop item from mySet:", string(item))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func SRem() {
	bucket6 := "bucket6"
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			if err := tx.SAdd(bucket6, []byte("mySet"), []byte("a"), []byte("b"), []byte("c")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			if err := tx.SRem(bucket6, []byte("mySet"), []byte("a")); err != nil {
				return err
			} else {
				fmt.Println("SRem ok")
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			if items, err := tx.SMembers(bucket6, []byte("mySet")); err != nil {
				return err
			} else {
				fmt.Println("SMembers items:", items)
				for _, item := range items {
					fmt.Println("item:", string(item))
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func SUnionByOneBucket() {
	bucket7 := "bucket1"
	key1 := []byte("mySet1")
	key2 := []byte("mySet2")

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			if err := tx.SAdd(bucket7, key1, []byte("a"), []byte("b"), []byte("c")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			if err := tx.SAdd(bucket7, key2, []byte("c"), []byte("d")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			if items, err := tx.SUnionByOneBucket(bucket7, key1, key2); err != nil {
				return err
			} else {
				fmt.Println("SUnionByOneBucket:", items)
				for _, item := range items {
					fmt.Println("item", string(item))
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func SUnionByTwoBucket() {
	bucket8 := "bucket1"
	key1 := []byte("mySet1")

	bucket9 := "bucket2"
	key2 := []byte("mySet2")

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			if err := tx.SAdd(bucket8, key1, []byte("a"), []byte("b"), []byte("c")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			if err := tx.SAdd(bucket9, key2, []byte("c"), []byte("d")); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			if items, err := tx.SUnionByTwoBuckets(bucket8, key1, bucket9, key2); err != nil {
				return err
			} else {
				fmt.Println("SUnionByTwoBucket:", items)
				for _, item := range items {
					fmt.Println("item", string(item))
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}
