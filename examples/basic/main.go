package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/nutsdb/nutsdb"
)

var (
	db     *nutsdb.DB
	bucket string
)

func init() {
	fileDir := "/tmp/nutsdb_example"

	files, _ := ioutil.ReadDir(fileDir)
	for _, f := range files {
		name := f.Name()
		if name != "" {
			fmt.Println(fileDir + "/" + name)
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
	bucket = "bucketForString"
}

func main() {
	// create bucket first
	createBucket()

	// insert
	put()
	// read
	read()

	// delete
	delete()
	// read
	read()

	// insert
	put()
	// read
	read()

	// update
	put2()
	// read
	read()

	// get value length
	valueLen()
}

func createBucket() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.NewBucket(nutsdb.DataStructureBTree, bucket)
		}); err != nil {
		log.Fatal(err)
	}
}

func delete() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("name1")
			return tx.Delete(bucket, key)
		}); err != nil {
		log.Fatal(err)
	}
}

func put() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("name1")
			val := []byte("val1")
			return tx.Put(bucket, key, val, 0)
		}); err != nil {
		log.Fatal(err)
	}
}
func put2() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("name1")
			val := []byte("val2")
			return tx.Put(bucket, key, val, 0)
		}); err != nil {
		log.Fatal(err)
	}
}
func read() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("name1")
			value, err := tx.Get(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("val:", string(value))

			return nil
		}); err != nil {
		log.Println(err)
	}
}

func valueLen() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("name1")
			value, err := tx.ValueLen(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("value length:", value)

			return nil
		}); err != nil {
		log.Println(err)
	}
}
