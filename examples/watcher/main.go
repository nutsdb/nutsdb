package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/nutsdb/nutsdb"
)

var (
	db     *nutsdb.DB
	bucket = "bucket_watcher_demo"
	dir    = "/tmp/nutsdbexample/example_watcher"
)

func init() {
	var err error
	os.RemoveAll(dir)

	db, err = nutsdb.Open(
		nutsdb.DefaultOptions,
		nutsdb.WithDir(dir),
		nutsdb.WithEnableWatch(true),
	)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	// Create bucket first
	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.NewBucket(nutsdb.DataStructureBTree, bucket)
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("=== NutsDB Watcher Example ===")

	// basic watching
	basicWatchExample()

	// watch with multiple operations
	multipleOperationsExample()

	// watch with TTL expiration
	watchWithTTLExample()

	if err := db.Close(); err != nil {
		log.Printf("Error closing database: %v\n", err)
	}
}

func basicWatchExample() {
	fmt.Println("--- Example 1: Basic Watch ---")
	key := []byte("watched_key")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var wg sync.WaitGroup

	messageCount := 0
	watcher, err := db.Watch(ctx, bucket, key, func(msg *nutsdb.Message) error {
		messageCount++
		fmt.Printf("[Watch] Message #%d received:\n", messageCount)
		fmt.Printf("  Bucket: %s\n", msg.BucketName)
		fmt.Printf("  Key: %s\n", msg.Key)
		fmt.Printf("  Value: %s\n", string(msg.Value))
		fmt.Printf("  Flag: %d\n", msg.Flag)
		fmt.Printf("  Timestamp: %d\n", msg.Timestamp)

		switch msg.Flag {
		case nutsdb.DataSetFlag:
			fmt.Println("  Type: SET operation")
		case nutsdb.DataDeleteFlag:
			fmt.Println("  Type: DELETE operation")
		}
		fmt.Println("")

		if messageCount >= 1 {
			cancel()
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := watcher.Run()
		if err != nil {
			log.Fatal(err)
		}
	}()

	if err := watcher.WaitReady(10 * time.Second); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.Put(bucket, key, []byte("Hello, Watcher!"), nutsdb.Persistent)
	}); err != nil {
		log.Fatal(err)
	}

	wg.Wait()
	fmt.Println("")
}

func multipleOperationsExample() {
	fmt.Println("--- Example 2: Watch Multiple Operations ---")
	key := []byte("multi_op_key")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	var wg sync.WaitGroup

	watcher, err := db.Watch(ctx, bucket, key, func(msg *nutsdb.Message) error {
		messageCount := 0
		messageCount++
		fmt.Printf("[Watch] Operation #%d: ", messageCount)

		switch msg.Flag {
		case nutsdb.DataSetFlag:
			fmt.Printf("SET - Value: %s\n", string(msg.Value))
		case nutsdb.DataDeleteFlag:
			fmt.Printf("DELETE - Key: %s\n", msg.Key)
		}

		if messageCount >= 3 {
			cancel()
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := watcher.Run(); err != nil {
			log.Fatal(err)
		}
	}()

	if err := watcher.WaitReady(10 * time.Second); err != nil {
		log.Fatal(err)
	}

	// Perform multiple operations
	operations := []struct {
		action string
		value  string
	}{
		{"put", "First value"},
		{"put", "Updated value"},
		{"delete", ""},
	}

	for _, op := range operations {
		switch op.action {
		case "put":
			if err := db.Update(func(tx *nutsdb.Tx) error {
				return tx.Put(bucket, key, []byte(op.value), nutsdb.Persistent)
			}); err != nil {
				log.Fatal(err)
			}
		case "delete":
			if err := db.Update(func(tx *nutsdb.Tx) error {
				return tx.Delete(bucket, key)
			}); err != nil {
				log.Fatal(err)
			}
		}
	}

	wg.Wait()
	fmt.Println("")
}

func watchWithTTLExample() {
	fmt.Println("--- Example 3: Watch with TTL ---")
	key := []byte("ttl_key")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	var wg sync.WaitGroup
	wg.Add(1)

	messageCount := 0
	watcher, err := db.Watch(ctx, bucket, key, func(msg *nutsdb.Message) error {
		messageCount++
		fmt.Printf("[Watch] Message #%d:\n", messageCount)

		switch msg.Flag {
		case nutsdb.DataSetFlag:
			fmt.Printf("  SET - Value: %s (with 2 second TTL)\n", string(msg.Value))
		case nutsdb.DataDeleteFlag:
			fmt.Printf("  DELETE - Key expired and auto-deleted\n")
		}

		if messageCount >= 2 {
			cancel()
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	go func() {
		defer wg.Done()
		if err := watcher.Run(); err != nil {
			log.Fatal(err)
		}
	}()

	if err := watcher.WaitReady(10 * time.Second); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.Put(bucket, key, []byte("Temporary value"), 2)
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Waiting for TTL expiration...")

	time.Sleep(3 * time.Second)

	wg.Wait()
	fmt.Println("")
}
