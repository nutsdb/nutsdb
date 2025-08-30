package main

import (
	"log"

	"github.com/nutsdb/nutsdb"
)

// Currently, nutsdb will persist empty bucket.
// So there will no bucket not found when rerun
// this binary.

func main() {
	db, err := nutsdb.Open(
		nutsdb.DefaultOptions,
		nutsdb.WithDir("./nutsdb"),
	)

	if err != nil {
		log.Fatalln("Open nutsdb :", err)
	}
	defer db.Close()
	bucket := "test_bucket"
	ds := nutsdb.DataStructureBTree
	key := "key"

	if err := db.Update(func(tx *nutsdb.Tx) error {
		if !tx.ExistBucket(ds, bucket) {
			err = tx.NewBucket(ds, bucket)
			if err != nil {
				return err
			}
			log.Printf("success init bucket %s", bucket)
			return nil
		}
		return nil
	}); err != nil {
		log.Fatalln("NewBucket error:", err)
	}

	if err := db.View(func(tx *nutsdb.Tx) error {
		_, err := tx.Get(bucket, []byte(key))
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		// always key not found
		log.Println("get error:", err)
	}
}
