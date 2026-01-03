
# Watch Key Changes

## Overview

NutsDB provides a **Watch** feature that enables real-time monitoring of key changes. This allows applications to react immediately to data modifications (Put, Delete, etc.) across all data structures (BTree, List, Set, SortedSet).

## Key Features

- **Universal Support**: Watch a key once, and it applies to all data structure buckets with that name.
- **Graceful Shutdown**: The watch function guarantees that all pending messages in the pipeline are processed before returning, ensuring no data loss during shutdown.
- **High Performance**: Uses a buffered channel design to ensure the database performance is not impacted by slow watchers.

## Use Cases

1. **Cache Invalidation**: Automatically invalidate cache entries when data changes
2. **Real-Time Dashboards**: Update UI when underlying data is modified
3. **Audit Logging**: Track all changes to sensitive keys
4. **Event-Driven Workflows**: Trigger business logic based on data changes
5. **Data Synchronization**: Replicate changes to external systems
6. **Monitoring & Alerting**: Detect and alert on specific data patterns

## Configuration

To use the watch feature, you must enable it when opening the database:

```go
opts := nutsdb.DefaultOptions
opts.EnableWatch = true // Enable watch feature

db, err := nutsdb.Open(opts, nutsdb.WithDir("/tmp/nutsdb"))
```

## Usage

The `Watch` function blocks and listens for changes to a specific key in a bucket.

```go
func (db *DB) Watch(ctx context.Context, bucket string, key []byte, cb func(message *Message) error, opts ...WatchOptions) (*Watcher, error)
```

## Parameters
- **ctx**: Pass context as a argument to allow user to cancel Watch Function manually. 
- **bucket**: The bucket name to watch.
- **key**: The key to watch.
- **cb**: A callback function that handles the incoming `*Message`. Return an error to stop watching, or `nil` to continue. The callback is executed sequentially for each message with a timeout.
- **opts**: Optional settings for the watch:
  - **CallbackTimeout**: The timeout for each callback execution (default: 1 second). If a callback takes longer than this timeout, watching will stop with `ErrWatchingCallbackTimeout`.

## Return Values
- **watcher**: A `*Watcher` object that must be started by calling `watcher.Run()` in a goroutine. 
- **error**: Returns an error if the watch cannot be started (e.g., `ErrWatchFeatureDisabled` if watching is not enabled, or `ErrWatchManagerClosed` if the watch manager is closed).

## Watcher Object

The `Watcher` object returned by `db.Watch()` provides methods to control the watch lifecycle:

### `Run() error`

Starts the watcher and begins listening for messages. This is a blocking call that must be executed in a goroutine.

- **Returns**: An error if the watch is stopped due to callback failure, timeout, or context cancellation.
- **Behavior**: 
  - Signals readiness by closing the internal ready channel
  - Processes incoming messages and calls the callback function
  - Returns when the context is done or an error occurs

**Usage**:
```go
go func() {
    if err := watcher.Run(); err != nil {
        log.Printf("Watcher stopped: %v", err)
    }
}()
```

### `WaitReady(timeout time.Duration) error`

Blocks until the watcher is ready to receive messages or the timeout expires.

- **Parameters**: 
  - `timeout`: Maximum time to wait for the watcher to be ready
- **Returns**: 
  - `nil` if the watcher is ready
  - An error if the timeout is reached before the watcher is ready
  - It should be called after `watcher.Run()`

**Usage**:
```go
go func() {
    if err := watcher.Run(); err != nil {
        log.Printf("Watcher stopped: %v", err)
    }
}()

// After the func Run() has been called, we should wait for ready to ensure that the watcher is available to receive messages
if err := watcher.WaitReady(5 * time.Second); err != nil {
    log.Fatal("Watcher not ready:", err)
}
```

## Example

```go
package main

import (
	"fmt"
	"log"
	"os"
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
	key := []byte("myKey")
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	// Create bucket
	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.NewBucket(nutsdb.DataStructureBTree, bucket)
	}); err != nil {
		log.Fatal(err)
	}

	watcher, _ := db.Watch(ctx, bucket, key, func(msg *nutsdb.Message) error {
		fmt.Printf("Received: Key=%s, Value=%s, Flag=%d\n", msg.Key, msg.Value, msg.Flag)

		//signal that we received the message
		close(done)
		return nil
	})

	go func() {
		if err := watcher.Run(); err != nil {
			log.Fatal(err)
		}
	}()

	// wait for the watcher to be ready
	if err := watcher.WaitReady(5 * time.Second); err != nil {
		log.Fatal(err)
	}

	// Perform operations
	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.Put(bucket, key, []byte("hello world"), nutsdb.Persistent)
	}); err != nil {
		log.Fatal(err)
	}

	select {
	case <-done:
		fmt.Println("Received message")
		//manually cancel the watcher
		cancel()
		fmt.Println("Watcher cancelled")
	case <-time.After(10 * time.Second):
		fmt.Println("Timeout")
	}

	fmt.Println("Done")
}
```

## Watch Manager Architecture

For those interested in the internal architecture:

```
                          ┌─────────────────┐
                          │   DB Operations │
                          │  (Put/Delete)   │
                          └────────┬────────┘
                                   │
                                   ▼
                          ┌─────────────────┐
                          │   watchChan     │◄─── Buffer: 1024 messages
                          │  (Hub Channel)  │
                          └────────┬────────┘
                                   │
                                   ▼
                          ┌─────────────────┐
                          │    Collector    │◄─── Batches up to 1024 msgs
                          │   Goroutine     │
                          └────────┬────────┘
                                   │
                                   ▼
                          ┌─────────────────┐
                          │ distributeChan  │◄─── Buffer: 128 batches
                          │ (Batch Channel) │
                          └────────┬────────┘
                                   │
                                   ▼
                          ┌─────────────────┐
                          │  Distributor    │◄─── Routes to subscribers
                          │   Goroutine     │     & sends bucket deletes
                          └────────┬────────┘     to victimChan
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼              ▼
            ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
            │ Subscriber 1 │ │ Subscriber 2 │ │ Subscriber N │
            │ receiveChan  │ │ receiveChan  │ │ receiveChan  │
            │ (1024 msgs)  │ │ (1024 msgs)  │ │ (1024 msgs)  │
            └──────────────┘ └──────────────┘ └──────────────┘

                          ┌─────────────────┐
                          │   victimChan    │◄─── Buffer: 128 victim buckets
                          │ (Bucket Delete) │     (from Distributor)
                          └────────┬────────┘
                                   │
                                   ▼
                          ┌─────────────────┐
                          │ Victim Collector│◄─── Handles bucket deletions
                          │   Goroutine     │     & notifies subscribers
                          └─────────────────┘
```

**Key Design Decisions:**

1. **Three-Goroutine Design**: 
   - **Collector**: Batches incoming messages (up to 1024) from watchChan
   - **Distributor**: Routes batched messages to subscribers and identifies bucket deletions
   - **Victim Collector**: Asynchronously handles bucket deletion notifications and subscriber cleanup
2. **Large Buffers**: 1024-message buffers minimize drop rates even under high load
3. **Batching**: Processes messages in batches (up to 1024) for better throughput
4. **Non-Blocking Sends**: Prevents slow subscribers from blocking the entire system
5. **Bucket Deletion Handling**: When a bucket is deleted, all affected subscribers receive a deletion notification and are gracefully shut down via the victimChan pipeline

This architecture ensures high performance and reliability even under heavy load.
### Important Notes

1. **Cross-Data Structure**: Watching a key works regardless of the data structure (BTree, List, Set, etc.). A change in any data structure for that bucket/key will trigger the callback.
2. **Canceling a Watch**: To stop watching a key, you must return an error from your callback function. Alternatively, the watch will be automatically canceled when the database is closed (`db.Close()`).
3. **Bucket Deletion**: If a bucket is completely deleted, all watchers subscribed to keys within that bucket will be shut down automatically.
4. **Graceful Exit**: When `db.Close()` is called or the watch is stopped, the system ensures ongoing batches are processed before the watch loop exits.

### Future Supports

| Feature | Description |
|---------|-------------|
| **Robust Message Handling** | Implement robust mechanism to ensure consistency, correct ordering of missing messages, and reliable synchronization among subscribers. |
| **Prefix Watch** | Support watching keys by prefix pattern. |
| **Range Key Watch** | Support watching a range of keys (startKey to endKey). |
| **Sharded Watcher** | Shard the watcher implementation by data structure type for better scalability. |