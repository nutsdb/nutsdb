# More Operation

## Using TTL(Time To Live)

NusDB supports TTL(Time to Live) for keys, you can use `tx.Put` function with a `ttl` parameter.

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
    key := []byte("name1")
    val := []byte("val1")
    bucket := "bucket1"
    
    // If set ttl = 0 or Persistent, this key will never expired.
    // Set ttl = 60 , after 60 seconds, this key will expired.
    if err := tx.Put(bucket, key, val, 60); err != nil {
        return err
    }
    return nil
}); err != nil {
    log.Fatal(err)
}
```

## Merge Operation

In order to maintain high-performance writing, NutsDB will write multiple copies of the same key. If your service has multiple updates or deletions to the same key, and you want to merge the same key, you can use NutsDB to provide `db.Merge()`method. This method requires you to write a merge strategy according to the actual situation. Once executed, it will block normal write requests, so it is best to avoid peak periods, such as scheduled execution in the middle of the night.

Of course, if you don't have too many updates or deletes for the same key, it is recommended not to use the Merge() function.

```go
err := db.Merge()
if err != nil {
    ...
}
```

Notice: the `HintBPTSparseIdxMode` mode does not support the merge operation of the current version.

## Database backup

NutsDB is easy to backup. You can use the `db.Backup()` function at given dir, call this function from a read-only transaction, and it will perform a hot backup and not block your other database reads and writes.

```go
err = db.Backup(dir)
if err != nil {
   ...
}
```



NutsDB also provides gzip to compress backups. You can use the `db.BackupTarGZ()` function.

```go
f, _ := os.Create(path)
defer f.Close()
err = db.BackupTarGZ(f)
if err != nil {
   ...
}
    
```

## Using in memory mode

In-memory mode is supported since nutsdb 0.7.0.

Run memory mode, after restarting the service, the data will be lost.

```go
opts := inmemory.DefaultOptions
db, err := inmemory.Open(opts)
if err != nil {
    ...
}

bucket := "bucket1"
key := []byte("key1")
val := []byte("val1")
err = db.Put(bucket, key, val, 0)
if err != nil {
    ...
}

entry, err := db.Get(bucket, key)
if err != nil {
    ...
}

fmt.Println("entry.Key", string(entry.Key))     // entry.Key key1
fmt.Println("entry.Value", string(entry.Value)) // entry.Value val1

```

In memory mode, there are some non-memory mode APIs that have not yet been implemented. If you need, you can submit an issue and explain your request.


## Watch Key Changes

### Overview

NutsDB provides a **Watch** feature that enables real-time monitoring of key changes. This allows applications to react immediately to data modifications (Put, Delete, etc.) across all data structures (BTree, List, Set, SortedSet).

### Key Features

- **Universal Support**: Watch a key once, and it applies to all data structure buckets with that name.
- **Graceful Shutdown**: The watch function guarantees that all pending messages in the pipeline are processed before returning, ensuring no data loss during shutdown.
- **High Performance**: Uses a buffered channel design to ensure the database performance is not impacted by slow watchers.

### Use Cases

1. **Cache Invalidation**: Automatically invalidate cache entries when data changes
2. **Real-Time Dashboards**: Update UI when underlying data is modified
3. **Audit Logging**: Track all changes to sensitive keys
4. **Event-Driven Workflows**: Trigger business logic based on data changes
5. **Data Synchronization**: Replicate changes to external systems
6. **Monitoring & Alerting**: Detect and alert on specific data patterns

### Configuration

To use the watch feature, you must enable it when opening the database:

```go
opts := nutsdb.DefaultOptions
opts.EnableWatch = true // Enable watch feature

db, err := nutsdb.Open(opts, nutsdb.WithDir("/tmp/nutsdb"))
```

### Usage

The `Watch` function blocks and listens for changes to a specific key in a bucket.

```go
func (db *DB) Watch(bucket string, key []byte, cb func(message *Message) error, opts ...WatchOptions) error
```

### Parameters
- **bucket**: The bucket name to watch.
- **key**: The key to watch.
- **cb**: A callback function that handles the incoming `*Message`. Return an error to stop watching.
- **opts**: Optional settings, such as `CallbackTimeout` (default 1s).

### Example

```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nutsdb/nutsdb"
)

func main() {
	// 1. Open DB with EnableWatch = true
	opt := nutsdb.DefaultOptions
	opt.EnableWatch = true
	db, _ := nutsdb.Open(opt, nutsdb.WithDir("/tmp/nutsdb"))
	defer db.Close()

	bucket := "myBucket"
	key := []byte("myKey")

	// 2. Start watching in a separate goroutine
	go func() {
		// Watch is a blocking call
		err := db.Watch(bucket, key, func(msg *nutsdb.Message) error {
			fmt.Printf("Received event: Key=%s, Value=%s, Flag=%d\n", msg.Key, msg.Value, msg.Flag)
			// Return nil to continue watching, or an error to stop.
			return nil
		})
		if err != nil {
			log.Println("Watch stopped:", err)
		}
	}()

	// 3. Perform operations
	db.Update(func(tx *nutsdb.Tx) error {
		tx.NewBucket(nutsdb.DataStructureBTree, bucket)
		return tx.Put(bucket, key, []byte("hello world"), 0)
	})

	time.Sleep(1 * time.Second)
}
```
### Watch Manager Architecture

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
