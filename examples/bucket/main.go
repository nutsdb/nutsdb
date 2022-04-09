package main

import (
	"fmt"
	"log"

	"github.com/xujiajun/nutsdb"
)

var (
	db *nutsdb.DB
)

func init() {
	opt := nutsdb.DefaultOptions
	// opt.RWMode = nutsdb.MMap
	// opt.SyncEnable = false
	opt.Dir = "/tmp/nutsdbexample/example_bucket"
	db, _ = nutsdb.Open(opt)
}

func main() {
	for i := 1; i <= 10; i++ {
		bucket := "bucket_" + fmt.Sprintf("%03d", i)
		if err := db.Update(
			func(tx *nutsdb.Tx) error {
				key := []byte("name1")
				val := []byte("val1")
				return tx.Put(bucket, key, val, 0)
			}); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("before iterate buckets")
	iterateBuckets()

	// delete bucket
	bucket := "bucket_003"
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.DeleteBucket(nutsdb.DataStructureBPTree, bucket)
		}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("after iterate buckets")
	iterateBuckets()
}

func iterateBuckets() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			return tx.IterateBuckets(nutsdb.DataStructureBPTree, func(bucket string) {
				fmt.Println("bucket: ", bucket)
			})
		}); err != nil {
		log.Fatal(err)
	}
}
