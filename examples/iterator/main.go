package main

import (
	"fmt"
	"github.com/xujiajun/nutsdb/consts"

	"github.com/xujiajun/nutsdb"
)

var (
	db     *nutsdb.DB
	bucket = "bucket_iterator_demo"
)

func init() {
	db, _ = nutsdb.Open(
		nutsdb.DefaultOptions,
		nutsdb.WithDir("/tmp/nutsdbexample/example_iterator"),
	)
}

func main() {
	tx, err := db.Begin(true)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_" + fmt.Sprintf("%03d", i))
		if err = tx.Put(bucket, key, val, consts.Persistent); err != nil {
			// tx rollback
			tx.Rollback()
			fmt.Printf("rollback ok, err %v:", err)
		}
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	// forward iteration
	forwardIteration()
	// reverse iterative
	reverseIterative()
}

func forwardIteration() {
	fmt.Println("--------begin forwardIteration--------")
	tx, err := db.Begin(false)
	iterator := nutsdb.NewIterator(tx, bucket, nutsdb.IteratorOptions{Reverse: false})
	i := 0
	for i < 10 {
		ok, err := iterator.SetNext()
		fmt.Println("ok, err", ok, err)
		fmt.Println("Key: ", string(iterator.Entry().Key))
		fmt.Println("Value: ", string(iterator.Entry().Value))
		fmt.Println()
		i++
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	fmt.Println("--------end forwardIteration--------")
}

func reverseIterative() {
	fmt.Println("--------start reverseIterative--------")
	tx, err := db.Begin(false)
	iterator := nutsdb.NewIterator(tx, bucket, nutsdb.IteratorOptions{Reverse: true})
	i := 0
	for i < 10 {
		ok, err := iterator.SetNext()
		fmt.Println("ok, err", ok, err)
		fmt.Println("Key: ", string(iterator.Entry().Key))
		fmt.Println("Value: ", string(iterator.Entry().Value))
		fmt.Println()
		i++
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	fmt.Println("--------end reverseIterative--------")
}
