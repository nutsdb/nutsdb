package main

import (
	"fmt"
	"log"

	"github.com/nutsdb/nutsdb/inmemory"
)

var (
	db     *inmemory.DB
	bucket string
)

func init() {
	bucket = "bucket1"
	opts := inmemory.DefaultOptions
	db, _ = inmemory.Open(opts)
}

func main() {
	put()
	get()
	prefixScan()
}

func put() {
	key := []byte("key1")
	val := []byte("val1")
	err := db.Put(bucket, key, val, 0)
	if err != nil {
		log.Fatal(err)
	}
}

func get() {
	key := []byte("key1")
	entry, err := db.Get(bucket, key)
	if err != nil {
		log.Fatal(err)
		return
	}
	fmt.Println("entry.Key", string(entry.Key))     // entry.Key key1
	fmt.Println("entry.Value", string(entry.Value)) // entry.Value val1
}

func prefixScan() {
	_ = db.Put(bucket, []byte("key2-0"), []byte("value2-0"), 0)
	_ = db.Put(bucket, []byte("key2-1"), []byte("value2-1"), 0)
	_ = db.Put(bucket, []byte("key2-2"), []byte("value2-2"), 0)
	_ = db.Put(bucket, []byte("key2-3"), []byte("value2-3"), 0)
	_ = db.Put(bucket, []byte("key2-4"), []byte("value2-4"), 0)
	_ = db.Put(bucket, []byte("key2-5"), []byte("value2-5"), 0)
	// entries: key2-2 key2-3
	entries, _, err := db.PrefixScan(bucket, []byte("key2"), 2, 2)
	if err != nil {
		log.Fatal(err)
		return
	}
	for i, entry := range entries {
		fmt.Printf("PrefixScan entry[%d] key: %s value: %s\n", i, entry.Key, entry.Value)
	}
}
