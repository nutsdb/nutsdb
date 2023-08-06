package main

import (
	"fmt"
	"github.com/nutsdb/nutsdb"
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
		if err = tx.Put(bucket, key, val, nutsdb.Persistent); err != nil {
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
	for {
		value, _ := iterator.Value()
		fmt.Println("Key: ", string(iterator.Key()))
		fmt.Println("Value: ", string(value))
		fmt.Println()
		if !iterator.Next() {
			break
		}
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
	for {
		value, _ := iterator.Value()
		fmt.Println("Key: ", string(iterator.Key()))
		fmt.Println("Value: ", string(value))
		fmt.Println()
		if !iterator.Next() {
			break
		}
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	fmt.Println("--------end reverseIterative--------")
}
