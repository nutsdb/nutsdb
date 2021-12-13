package main

import (
	"fmt"

	"github.com/xujiajun/nutsdb/inmemory"
)

func main() {
	opts := inmemory.DefaultOptions
	db, err := inmemory.Open(opts)
	if err != nil {
		panic(err)
	}
	bucket := "bucket1"
	key := []byte("key1")
	val := []byte("val1")
	err = db.Put(bucket, key, val, 0)
	if err != nil {
		fmt.Println("err", err)
	}

	entry, err := db.Get(bucket, key)
	if err != nil {
		fmt.Println("err", err)
	}

	fmt.Println("entry.Key", string(entry.Key))     // entry.Key key1
	fmt.Println("entry.Value", string(entry.Value)) // entry.Value val1
}
