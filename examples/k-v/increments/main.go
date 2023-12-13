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
	// If we call Incr, Decr, IncrBy and DecrBy on a key that does not exist, we will get an error with 'key not found'
	incr("non-exist")
	decr("non-exist")
	incrBy("non-exist", 1)
	decrBy("non-exist", 2)

	// If we call Incr, Decr, IncrBy and DecrBy on key that exist but the corresponding value not an integer, we will get an error with 'value is not an integer'
	put("key1", "value1")
	incr("key1")
	decr("key1")
	incrBy("key1", 1)
	decrBy("key1", 1)

	put("key2", "12")

	// Incr will increase the value by 1
	incr("key2")
	get("key2") // get value: 13

	// Decr will decrease the value by 1
	decr("key2")
	get("key2") // get value: 12

	// IncrBy will increase the value by given value
	incrBy("key2", 2)
	get("key2") // get value: 14

	// DecrBy will decrease the value by given value
	decrBy("key2", 2)
	get("key2") // get value: 12
}

func get(key string) {
	if err := db.View(func(tx *nutsdb.Tx) error {
		value, err := tx.Get(bucket, []byte(key))
		if err != nil {
			return err
		}
		log.Println("get value: ", value)
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

func incr(key string) {
	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.Incr(bucket, []byte(key))
	}); err != nil {
		log.Println(err)
	}
}

func decr(key string) {
	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.Decr(bucket, []byte(key))
	}); err != nil {
		log.Println(err)
	}
}

func incrBy(key string, value int64) {
	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.IncrBy(bucket, []byte(key), value)
	}); err != nil {
		log.Println(err)
	}
}

func decrBy(key string, value int64) {
	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.DecrBy(bucket, []byte(key), value)
	}); err != nil {
		log.Println(err)
	}
}
