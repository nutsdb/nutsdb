package main

import (
	"log"
	"os"

	"github.com/nutsdb/nutsdb"
)

var (
	db     *nutsdb.DB
	bucket string
	err    error
)

func init() {
	fileDir := "/tmp/nutsdb_example"

	files, _ := os.ReadDir(fileDir)
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
	// When we use tx.MSet, we must use even number of []byte as its parameters.
	// When i is an even number, the no.i arg and the no.i+1 arg will be a pair of K-V.
	// If there're odd number of args, it will throw an error
	mSet([]byte("1")) //  parameters is used to represent key value pairs and cannot be odd numbers

	// Normally, we use tx.MSet and tx.MGet to do some multiple operations.
	mSet([]byte("1"), []byte("one"), []byte("2"), []byte("two"))
	// get value by MGet, the  0  value is: 'one'
	// get value by MGet, the  1  value is: 'two'
	mGet([]byte("1"), []byte("2"))
}

func mGet(key ...[]byte) {
	if err := db.View(func(tx *nutsdb.Tx) error {
		values, err := tx.MGet(bucket, key...)
		if err != nil {
			return err
		}
		for i, value := range values {
			log.Printf("get value by MGet, the %d value is '%s'", i, string(value))
		}
		return nil
	}); err != nil {
		log.Println(err)
	}
}

func mSet(args ...[]byte) {
	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.MSet(bucket, nutsdb.Persistent, args...)
	}); err != nil {
		log.Println(err)
	}
}
