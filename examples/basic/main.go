package main

import (
	"fmt"
	"log"

	"github.com/xujiajun/nutsdb"
)

var (
	db     *nutsdb.DB
	bucket string
)

func init() {
	opt := nutsdb.DefaultOptions
	opt.Dir = "testdata/example"
	db, _ = nutsdb.Open(opt)
	bucket = "bucket1"
}

func main() {
	//insert put
	put()
	//read
	read()
	//delete
	delete()
	//read
	read()
	//update put
	put()
	//read
	read()
}

func delete() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("name1")
			if err := tx.Delete(bucket, key); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func put() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("name1")
			val := []byte("val1")
			if err := tx.Put(bucket, key, val, 0); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func read() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("name1")
			if e, err := tx.Get(bucket, key); err != nil {
				return err
			} else {
				fmt.Println("val:", string(e.Value))
			}
			return nil
		}); err != nil {
		log.Println(err)
	}
}
