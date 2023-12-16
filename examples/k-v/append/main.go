package main

import (
	"github.com/nutsdb/nutsdb"
	"io/ioutil"
	"log"
	"os"
)

var (
	db     *nutsdb.DB
	bucket string
	err    error
)

func init() {
	fileDir := "/tmp/nutsdb_example"

	files, _ := ioutil.ReadDir(fileDir)
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
	bucket = "bucketForString"

	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.NewBucket(nutsdb.DataStructureBTree, bucket)
	}); err != nil {
		log.Fatal(err)
	}
}

func main() {
	// If we use tx.Append on a non-exist key, it will create a pair of K-V and use the appendage as its value.
	append("non-exist", "something")
	get("non-exist") // get value: 'something'

	// If we use tx.Append on an exist key, it will append the given appendage to the existed value.
	put("key", "value")
	append("key", "more value")
	get("key") // get value: 'valuemore value'
}

func get(key string) {
	if err := db.View(func(tx *nutsdb.Tx) error {
		value, err := tx.Get(bucket, []byte(key))
		if err != nil {
			return err
		}
		log.Printf("get value: '%s'", string(value))
		return nil
	}); err != nil {
		log.Println(err)
	}
}

func put(key, value string) {
	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.Put(bucket, []byte(key), []byte(value), nutsdb.Persistent)
	}); err != nil {
		log.Println(err)
	}
}

func append(key, appendage string) {
	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.Append(bucket, []byte(key), []byte(appendage))
	}); err != nil {
		log.Println(err)
	}
}
