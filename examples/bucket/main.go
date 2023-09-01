package main

import (
	"fmt"
	"log"

	"github.com/nutsdb/nutsdb"
)

var (
	db *nutsdb.DB
)

func init() {
	db, _ = nutsdb.Open(
		nutsdb.DefaultOptions,
		nutsdb.WithDir("/tmp/nutsdbexample/example_bucket"),
		// nutsdb.WithRWMode(nutsdb.MMap),
		// nutsdb.WithSyncEnable(false),
	)
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
			return tx.DeleteBucket(nutsdb.DataStructureBTree, bucket)
		}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("after iterate buckets")
	iterateBuckets()
}

func iterateBuckets() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			return tx.IterateBuckets(nutsdb.DataStructureBTree, "*", func(bucket string) bool {
				fmt.Println("bucket: ", bucket)
				return true
			})
		}); err != nil {
		log.Fatal(err)
	}
}
