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
	opt.Dir = "testdata/example"
	db, _ = nutsdb.Open(opt)

}

func main() {
	bucket001 := "bucket001"
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("name001")
			val := []byte("bucket001val1")
			if err := tx.Put(bucket001, key, val, 0); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	bucket002 := "bucket002"
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("name001")
			val := []byte("bucket002val1")
			if err := tx.Put(bucket002, key, val, 0); err != nil {
				return err
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("name001")
			if e, err := tx.Get(bucket001, key); err != nil {
				return err
			} else {
				fmt.Println("val:", string(e.Value))
			}
			return nil
		}); err != nil {
		log.Println(err)
	}

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("name001")
			if e, err := tx.Get(bucket002, key); err != nil {
				return err
			} else {
				fmt.Println("val:", string(e.Value))
			}
			return nil
		}); err != nil {
		log.Println(err)
	}
}
