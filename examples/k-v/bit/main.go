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
	// If we use `GetBit` on a key that does not exist, we will get an error with 'key not found'
	getBit("non-exist", 12)

	// If we `SetBit` on an exist k-v, we will update the corresponding bit of value.
	put("key1", "value1")
	setBit("key1", 5, 2)
	getBit("key1", 5) // get bit: 2

	// If we `SetBit` on a non-exist k-v or length of value is less than given offset, we will expand the value.
	// The expanded value will be filled with 0. Until we can modify the corresponding bit with given offset.
	setBit("key2", 5, 1)
	getBit("key2", 5) // get bit: 1
	get("key2")       // get value: [0, 0, 0, 0, 0, 1]
}

func get(key string) {
	if err := db.View(func(tx *nutsdb.Tx) error {
		value, err := tx.Get(bucket, []byte(key))
		if err != nil {
			return err
		}
		log.Println("get value: ", string(value))
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

func getBit(key string, offset int) {
	if err := db.View(func(tx *nutsdb.Tx) error {
		bit, err := tx.GetBit(bucket, []byte(key), offset)
		if err != nil {
			return err
		}
		log.Println("get bit:", bit)
		return nil
	}); err != nil {
		log.Println(err)
	}
}

func setBit(key string, offset int, bit byte) {
	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.SetBit(bucket, []byte(key), offset, bit)
	}); err != nil {
		log.Println(err)
	}
}
