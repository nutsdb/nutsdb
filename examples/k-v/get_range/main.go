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
	// If we use tx.GetRange on a non-exist key, it will throw an error about key not found
	getRange("non-exist", 0, 0) // key not found

	put("key", "This is a test value")
	// If we use tx.GetRange with a pair of start and end which start is greater than end, it will throw an error.
	getRange("key", 5, 3) // start is greater than end
	// When start < end but start is greater than the size of value, it will return an empty value.
	getRange("key", 100, 120) // got value: ''
	// When start is less than the size of value, but end is greater. It will use value[start:] as its return.
	getRange("key", 10, 100) // got value: 'test value'
	// When end is less than the size of value. It will use value[start:end+1] as its return. This is same with Redis.
	getRange("key", 10, 13) // got value: 'test'
}

func put(key, value string) {
	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.Put(bucket, []byte(key), []byte(value), nutsdb.Persistent)
	}); err != nil {
		log.Println(err)
	}
}

func getRange(key string, start, end int) {
	if err := db.View(func(tx *nutsdb.Tx) error {
		value, err := tx.GetRange(bucket, []byte(key), start, end)
		if err != nil {
			return err
		}
		log.Printf("got value: '%s'", string(value))
		return nil
	}); err != nil {
		log.Println(err)
	}
}
