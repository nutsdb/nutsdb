package main

import (
	"fmt"
	"log"

	"github.com/xujiajun/nutsdb"
	"github.com/xujiajun/utils/time2"
)

var (
	db     *nutsdb.DB
	bucket string
)

func init() {
	opt := nutsdb.DefaultOptions
	opt.Dir = "testdata/example_batch"
	db, _ = nutsdb.Open(opt)
	bucket = "bucket1"
}

func main() {

	time2.Start()

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("key9999")
			val := []byte("val9999")
			if err := tx.Put(bucket, key, val, 0); err != nil {
				return err
			}
			e, _ := tx.Get(bucket, key)
			if e != nil {
				fmt.Println("before read", string(e.Value))
			}

			val = []byte("val6666")
			if err := tx.Put(bucket, key, val, 0); err != nil {
				return err
			}
			e, _ = tx.Get(bucket, key)
			if e != nil {
				fmt.Println("after read", string(e.Value))
			}

			//if err := tx.Delete(bucket, key); err != nil {
			//	return err
			//}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("key9999")
			e, err := tx.Get(bucket, key)
			if err != nil {
				log.Println(err)
				return err
			}
			if e != nil {
				fmt.Println("read", string(e.Value))
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	fmt.Println(time2.End())
}
