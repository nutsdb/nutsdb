<p align="center">
    <img src="https://user-images.githubusercontent.com/6065007/141310364-62d7eebb-2cbb-4949-80ed-5cd20f705405.png">
</p>

# NutsDB [![GoDoc](https://godoc.org/github.com/xujiajun/nutsdb?status.svg)](https://godoc.org/github.com/xujiajun/nutsdb)  [![Go Report Card](https://goreportcard.com/badge/github.com/xujiajun/nutsdb)](https://goreportcard.com/report/github.com/xujiajun/nutsdb) <a href="https://travis-ci.org/xujiajun/nutsdb"><img src="https://travis-ci.org/xujiajun/nutsdb.svg?branch=master" alt="Build Status"></a> [![Coverage Status](https://coveralls.io/repos/github/xujiajun/nutsdb/badge.svg?branch=master)](https://coveralls.io/github/xujiajun/nutsdb?branch=master) [![License](http://img.shields.io/badge/license-Apache_2-blue.svg?style=flat-square)](https://raw.githubusercontent.com/xujiajun/nutsdb/master/LICENSE) [![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go#database)  

English | [简体中文](https://github.com/xujiajun/nutsdb/blob/master/README-CN.md)

NutsDB is a simple, fast, embeddable and persistent key/value store written in pure Go. 

It supports fully serializable transactions and many data structures such as list、set、sorted set. All operations happen inside a Tx. Tx represents a transaction, which can be read-only or read-write. Read-only transactions can read values for a given bucket and a given key or iterate over a set of key-value pairs. Read-write transactions can read, update and delete keys from the DB.

## Announcement
* v0.11.0 release, see for details: https://github.com/nutsdb/nutsdb/issues/219
* v0.10.0 release, see for details: https://github.com/nutsdb/nutsdb/issues/193
* v0.9.0 release, see for details: https://github.com/nutsdb/nutsdb/issues/167

📢 Note: Starting from v0.9.0, **defaultSegmentSize** in **DefaultOptions** has been adjusted from **8MB** to **256MB**. The original value is the default value, which needs to be manually changed to 8MB, otherwise the original data will not be parsed. The reason for the size adjustment here is that there is a cache for file descriptors starting from v0.9.0 (detail see https://github.com/nutsdb/nutsdb/pull/164 ), so users need to look at the number of fds they use on the server, which can be set manually. If you have any questions, you can open an issue.

## Architecture
![image](https://user-images.githubusercontent.com/6065007/163713248-73a80478-8d6a-4c53-927c-71ba34569ae7.png)


 Welcome [contributions to NutsDB](https://github.com/xujiajun/nutsdb#contributing).

## Table of Contents

  - [Getting Started](#getting-started)
    - [Installing](#installing)
    - [Opening a database](#opening-a-database)
    - [Options](#options)
      - [Default Options](#default-options)
    - [Transactions](#transactions)
      - [Read-write transactions](#read-write-transactions)
      - [Read-only transactions](#read-only-transactions)
      - [Managing transactions manually](#managing-transactions-manually)
    - [Using buckets](#using-buckets)
      - [Iterate buckets](#iterate-buckets)
      - [Delete bucket](#delete-bucket)
    - [Using key/value pairs](#using-keyvalue-pairs)
    - [Using TTL(Time To Live)](#using-ttltime-to-live)
    - [Iterating over keys](#iterating-over-keys)
      - [Prefix scans](#prefix-scans)
      - [Prefix search scans](#prefix-search-scans)
      - [Range scans](#range-scans)
      - [Get all](#get-all)
      - [Iterator](#iterator)
    - [Merge Operation](#merge-operation)
    - [Database backup](#database-backup)
    - [Using in memory mode](#using-in-memory-mode)
    - [Using other data structures](#using-other-data-structures)
      - [List](#list)
        - [RPush](#rpush)
        - [LPush](#lpush)
        - [LPop](#lpop)
        - [LPeek](#lpeek)
        - [RPop](#rpop)
        - [RPeek](#rpeek)
        - [LRange](#lrange)
        - [LRem](#lrem)
        - [LRemByIndex](#lrembyindex)
        - [LSet](#lset)
        - [Ltrim](#ltrim)
        - [LSize](#lsize)
        - [LKeys](#lkeys)
      - [Set](#set)
        - [SAdd](#sadd)
        - [SAreMembers](#saremembers)
        - [SCard](#scard)
        - [SDiffByOneBucket](#sdiffbyonebucket)
        - [SDiffByTwoBuckets](#sdiffbytwobuckets)
        - [SHasKey](#shaskey)
        - [SIsMember](#sismember)
        - [SMembers](#smembers)
        - [SMoveByOneBucket](#smovebyonebucket)
        - [SMoveByTwoBuckets](#smovebytwobuckets)
        - [SPop](#spop)
        - [SRem](#srem)
        - [SUnionByOneBucket](#sunionbyonebucket)
        - [SUnionByTwoBuckets](#sunionbytwobuckets)
        - [SKeys](#skeys)
      - [Sorted Set](#sorted-set)
        - [ZAdd](#zadd)
        - [ZCard](#zcard)
        - [ZCount](#zcount)
        - [ZGetByKey](#zgetbykey)
        - [ZMembers](#zmembers)
        - [ZPeekMax](#zpeekmax)
        - [ZPeekMin](#zpeekmin)
        - [ZPopMax](#zpopmax)
        - [ZPopMin](#zpopmin)
        - [ZRangeByRank](#zrangebyrank)
        - [ZRangeByScore](#zrangebyscore)
        - [ZRank](#zrank)
        - [ZKeys](#zkeys)
      - [ZRevRank](#zrevrank)
        - [ZRem](#zrem)
        - [ZRemRangeByRank](#zremrangebyrank)
        - [ZScore](#zscore)
    - [Comparison with other databases](#comparison-with-other-databases)
      - [BoltDB](#boltdb)
      - [LevelDB, RocksDB](#leveldb-rocksdb)
      - [Badger](#badger)
    - [Benchmarks](#benchmarks)
    - [Caveats & Limitations](#caveats--limitations)
    - [Contact](#contact)
    - [Contributing](#contributing)
    - [Acknowledgements](#acknowledgements)
    - [License](#license)

## Getting Started

### Installing

To start using NutsDB, first needs [Go](https://golang.org/dl/) installed (version 1.11+ is required).  and run go get:

```
go get -u github.com/xujiajun/nutsdb
```

### Opening a database

To open your database, use the nutsdb.Open() function,with the appropriate options.The `Dir` , `EntryIdxMode`  and  `SegmentSize`  options are must be specified by the client. About options see [here](https://github.com/xujiajun/nutsdb#options) for detail.

```golang
package main

import (
    "log"

    "github.com/xujiajun/nutsdb"
)

func main() {
    // Open the database located in the /tmp/nutsdb directory.
    // It will be created if it doesn't exist.
    db, err := nutsdb.Open(
        nutsdb.DefaultOptions,
        nutsdb.WithDir("/tmp/nutsdb"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    ...
}
```

### Options

* Dir                  string  

`Dir` represents Open the database located in which dir.

* EntryIdxMode         EntryIdxMode 

`EntryIdxMode` represents using which mode to index the entries. `EntryIdxMode` includes three options: `HintKeyValAndRAMIdxMode`,`HintKeyAndRAMIdxMode` and `HintBPTSparseIdxMode`. `HintKeyValAndRAMIdxMode` represents ram index (key and value) mode, `HintKeyAndRAMIdxMode` represents ram index (only key) mode and `HintBPTSparseIdxMode` represents b+ tree sparse index mode.

* RWMode               RWMode  

`RWMode` represents the read and write mode. `RWMode` includes two options: `FileIO` and `MMap`.
FileIO represents the read and write mode using standard I/O. And MMap represents the read and write mode using mmap.

* SegmentSize          int64 

NutsDB will truncate data file if the active file is larger than `SegmentSize`.
Current version default `SegmentSize` is 8MB,but you can custom it.
**The defaultSegmentSize becomes 256MB when the version is greater than 0.8.0.**

Once set, it cannot be changed. see [caveats--limitations](https://github.com/xujiajun/nutsdb#caveats--limitations) for detail.

* NodeNum              int64

`NodeNum` represents the node number.Default NodeNum is 1. `NodeNum` range [1,1023] .

* SyncEnable           bool

`SyncEnable` represents if call Sync() function.
if `SyncEnable` is false, high write performance but potential data loss likely.
if `SyncEnable` is true, slower but persistent.

* StartFileLoadingMode RWMode

`StartFileLoadingMode` represents when open a database which RWMode to load files.
    
#### Default Options

Recommend to use the `DefaultOptions` . Unless you know what you're doing.

```
var DefaultOptions = Options{
    EntryIdxMode:         HintKeyValAndRAMIdxMode,
    SegmentSize:          defaultSegmentSize,
    NodeNum:              1,
    RWMode:               FileIO,
    SyncEnable:           true,
    StartFileLoadingMode: MMap,
}
```

### Transactions

NutsDB allows only one read-write transaction at a time but allows as many read-only transactions as you want at a time. Each transaction has a consistent view of the data as it existed when the transaction started.

When a transaction fails, it will roll back, and revert all changes that occurred to the database during that transaction.
If the option `SyncEnable` is set to true, when a read/write transaction succeeds, all changes are persisted to disk.

Creating transaction from the `DB` is thread safe.

#### Read-write transactions

```golang
err := db.Update(
    func(tx *nutsdb.Tx) error {
    ...
    return nil
})

```

#### Read-only transactions

```golang
err := db.View(
    func(tx *nutsdb.Tx) error {
    ...
    return nil
})

```

#### Managing transactions manually

The `DB.View()`  and  `DB.Update()`  functions are wrappers around the  `DB.Begin()`  function. These helper functions will start the transaction, execute a function, and then safely close your transaction if an error is returned. This is the recommended way to use NutsDB transactions.

However, sometimes you may want to manually start and end your transactions. You can use the DB.Begin() function directly but please be sure to close the transaction. 

```golang
 // Start a write transaction.
tx, err := db.Begin(true)
if err != nil {
    return err
}

bucket := "bucket1"
key := []byte("foo")
val := []byte("bar")

// Use the transaction.
if err = tx.Put(bucket, key, val, nutsdb.Persistent); err != nil {
    // Rollback the transaction.
    tx.Rollback()
} else {
    // Commit the transaction and check for error.
    if err = tx.Commit(); err != nil {
        tx.Rollback()
        return err
    }
}
```

### Using buckets

Buckets are collections of key/value pairs within the database. All keys in a bucket must be unique.
Bucket can be interpreted as a table or namespace. So you can store the same key in different bucket. 

```golang

key := []byte("key001")
val := []byte("val001")

bucket001 := "bucket001"
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        if err := tx.Put(bucket001, key, val, 0); err != nil {
            return err
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

bucket002 := "bucket002"
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        if err := tx.Put(bucket002, key, val, 0); err != nil {
            return err
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

```

Also, this bucket is related to the data structure you use. Different data index structures that use the same bucket are also different. For example, you define a bucket named `bucket_foo`, so you need to use the `list` data structure, use `tx.RPush` to add data, you must query or retrieve from this bucket_foo data structure, use `tx.RPop`, `tx.LRange`, etc. You cannot use `tx.Get` (same index type as `tx.GetAll`, `tx.Put`, `tx.Delete`, `tx.RangeScan`, etc.) to read the data in this `bucket_foo`, because the index structure is different. Other data structures such as `Set`, `Sorted Set` are the same.

#### Iterate buckets

IterateBuckets iterates over all the buckets that match the pattern. IterateBuckets function has three parameters: `ds`, `pattern` and function `f`.

The current version of the Iterate Buckets method supports the following EntryId Modes:

* `HintKeyValAndRAMIdxMode`：represents ram index (key and value) mode.
* `HintKeyAndRAMIdxMode`：represents ram index (only key) mode.

The `pattern` added in version `0.11.0` (represents the pattern to match):

* `pattern` syntax refer to: `filepath.Match`

The current version of `ds` (represents the data structure):

* DataStructureSet
* DataStructureSortedSet
* DataStructureBPTree
* DataStructureList

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        return tx.IterateBuckets(nutsdb.DataStructureBPTree, "*", func(bucket string) bool {
            fmt.Println("bucket: ", bucket)
            // true: continue, false: break
            return true
        })
    }); err != nil {
    log.Fatal(err)
}
    
```

#### Delete bucket

DeleteBucket represents delete bucket. DeleteBucket function has two parameters: `ds`(represents the data structure) and `bucket`.

The current version of the Iterate Buckets method supports the following EntryId Modes:

* `HintKeyValAndRAMIdxMode`：represents ram index (key and value) mode.
* `HintKeyAndRAMIdxMode`：represents ram index (only key) mode.

The current version of `ds` (represents the data structure)：

* DataStructureSet
* DataStructureSortedSet
* DataStructureBPTree
* DataStructureList

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.DeleteBucket(nutsdb.DataStructureBPTree, bucket)
    }); err != nil {
    log.Fatal(err)
}
    
```


### Using key/value pairs

To save a key/value pair to a bucket, use the `tx.Put` method:

```golang

if err := db.Update(
    func(tx *nutsdb.Tx) error {
    key := []byte("name1")
    val := []byte("val1")
    bucket := "bucket1"
    if err := tx.Put(bucket, key, val, 0); err != nil {
        return err
    }
    return nil
}); err != nil {
    log.Fatal(err)
}

```

This will set the value of the "name1" key to "val1" in the bucket1 bucket.

To update the the value of the "name1" key,we can still use the `tx.Put` function:

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
    key := []byte("name1")
    val := []byte("val1-modify") // Update the value
    bucket := "bucket1"
    if err := tx.Put(bucket, key, val, 0); err != nil {
        return err
    }
    return nil
}); err != nil {
    log.Fatal(err)
}

```

To retrieve this value, we can use the `tx.Get` function:

```golang
if err := db.View(
func(tx *nutsdb.Tx) error {
    key := []byte("name1")
    bucket := "bucket1"
    if e, err := tx.Get(bucket, key); err != nil {
        return err
    } else {
        fmt.Println(string(e.Value)) // "val1-modify"
    }
    return nil
}); err != nil {
    log.Println(err)
}
```

Use the `tx.Delete()` function to delete a key from the bucket.

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
    key := []byte("name1")
    bucket := "bucket1"
    if err := tx.Delete(bucket, key); err != nil {
        return err
    }
    return nil
}); err != nil {
    log.Fatal(err)
}
```

### Using TTL(Time To Live)

NusDB supports TTL(Time to Live) for keys, you can use `tx.Put` function with a `ttl` parameter.

```golang
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
### Iterating over keys

NutsDB stores its keys in byte-sorted order within a bucket. This makes sequential iteration over these keys extremely fast.

#### Prefix scans

To iterate over a key prefix, we can use `PrefixScan` function, and the parameters `offsetNum` and `limitNum` constrain the number of entries returned :

```golang

if err := db.View(
    func(tx *nutsdb.Tx) error {
        prefix := []byte("user_")
        bucket := "user_list"
        // Constrain 100 entries returned 
        if entries, _, err := tx.PrefixScan(bucket, prefix, 25, 100); err != nil {
            return err
        } else {
            for _, entry := range entries {
                fmt.Println(string(entry.Key), string(entry.Value))
            }
        }
        return nil
    }); err != nil {
        log.Fatal(err)
}

```

#### Prefix search scans

To iterate over a key prefix with search by regular expression on a second part of key without prefix, we can use `PrefixSearchScan` function, and the parameters `offsetNum`, `limitNum` constrain the number of entries returned :

```golang

if err := db.View(
    func(tx *nutsdb.Tx) error {
        prefix := []byte("user_")
        reg := "username"
        bucket := "user_list"
        // Constrain 100 entries returned 
        if entries, _, err := tx.PrefixSearchScan(bucket, prefix, reg, 25, 100); err != nil {
            return err
        } else {
            for _, entry := range entries {
                fmt.Println(string(entry.Key), string(entry.Value))
            }
        }
        return nil
    }); err != nil {
        log.Fatal(err)
}

```

#### Range scans

To scan over a range, we can use `RangeScan` function. For example：

```golang
if err := db.View(
    func(tx *nutsdb.Tx) error {
        // Assume key from user_0000000 to user_9999999.
        // Query a specific user key range like this.
        start := []byte("user_0010001")
        end := []byte("user_0010010")
        bucket := "user_list"
        if entries, err := tx.RangeScan(bucket, start, end); err != nil {
            return err
        } else {
            for _, entry := range entries {
                fmt.Println(string(entry.Key), string(entry.Value))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

#### Get all

To scan all keys and values of the bucket stored, we can use `GetAll` function. For example:

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "user_list"
        entries, err := tx.GetAll(bucket)
        if err != nil {
            return err
        }

        for _, entry := range entries {
            fmt.Println(string(entry.Key),string(entry.Value))
        }

        return nil
    }); err != nil {
    log.Println(err)
}
```

#### iterator

The option parameter 'Reverse' that determines whether the iterator is forward or Reverse. The current version does not support the iterator for HintBPTSparseIdxMode.

#### forward iterator
```go
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
```

#### reverse iterator


```go
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
    
  ```
### Merge Operation

In order to maintain high-performance writing, NutsDB will write multiple copies of the same key. If your service has multiple updates or deletions to the same key, and you want to merge the same key, you can use NutsDB to provide `db.Merge()`method. This method requires you to write a merge strategy according to the actual situation. Once executed, it will block normal write requests, so it is best to avoid peak periods, such as scheduled execution in the middle of the night.

Of course, if you don't have too many updates or deletes for the same key, it is recommended not to use the Merge() function.

```golang
err := db.Merge()
if err != nil {
    ...
}
```

Notice: the `HintBPTSparseIdxMode` mode does not support the merge operation of the current version.

### Database backup

NutsDB is easy to backup. You can use the `db.Backup()` function at given dir, call this function from a read-only transaction, and it will perform a hot backup and not block your other database reads and writes.

```golang
err = db.Backup(dir)
if err != nil {
   ...
}
```

NutsDB also provides gzip to compress backups. You can use the `db.BackupTarGZ()` function.

```golang
f, _ := os.Create(path)
defer f.Close()
err = db.BackupTarGZ(f)
if err != nil {
   ...
}
    
```

### Using in memory mode

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

### Using other data structures

The syntax here is modeled after [Redis commands](https://redis.io/commands)

#### List

##### RPush

Inserts the values at the tail of the list stored in the bucket at given bucket, key and values.

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "bucketForList"
        key := []byte("myList")
        val := []byte("val1")
        return tx.RPush(bucket, key, val)
    }); err != nil {
    log.Fatal(err)
}
```

##### LPush 

Inserts the values at the head of the list stored in the bucket at given bucket, key and values.

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        val := []byte("val2")
        return tx.LPush(bucket, key, val)
    }); err != nil {
    log.Fatal(err)
}
```

##### LPop 

Removes and returns the first element of the list stored in the bucket at given bucket and key.

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        if item, err := tx.LPop(bucket, key); err != nil {
            return err
        } else {
            fmt.Println("LPop item:", string(item))
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### LPeek

Returns the first element of the list stored in the bucket at given bucket and key.

```golang
if err := db.View(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        if item, err := tx.LPeek(bucket, key); err != nil {
            return err
        } else {
            fmt.Println("LPeek item:", string(item)) //val11
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### RPop 

Removes and returns the last element of the list stored in the bucket at given bucket and key.

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        if item, err := tx.RPop(bucket, key); err != nil {
            return err
        } else {
            fmt.Println("RPop item:", string(item))
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### RPeek

Returns the last element of the list stored in the bucket at given bucket and key.

```golang
if err := db.View(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        if item, err := tx.RPeek(bucket, key); err != nil {
            return err
        } else {
            fmt.Println("RPeek item:", string(item))
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### LRange 

Returns the specified elements of the list stored in the bucket at given bucket,key, start and end.
The offsets start and stop are zero-based indexes 0 being the first element of the list (the head of the list),
1 being the next element and so on. 
Start and end can also be negative numbers indicating offsets from the end of the list,
where -1 is the last element of the list, -2 the penultimate element and so on.

```golang
if err := db.View(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        if items, err := tx.LRange(bucket, key, 0, -1); err != nil {
            return err
        } else {
            //fmt.Println(items)
            for _, item := range items {
                fmt.Println(string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### LRem 

Note: This feature can be used starting from v0.6.0

Removes the first count occurrences of elements equal to value from the list stored in the bucket at given bucket,key,count.
The count argument influences the operation in the following ways:

* count > 0: Remove elements equal to value moving from head to tail.
* count < 0: Remove elements equal to value moving from tail to head.
* count = 0: Remove all elements equal to value.

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        return tx.LRem(bucket, key, 1, []byte("value11))
    }); err != nil {
    log.Fatal(err)
}
```

##### LRemByIndex

Note: This feature can be used starting from v0.10.0

Remove the element at a specified position (single or multiple) from the list


```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "bucketForList"
        key := []byte("myList")
        removedNum, err := tx.LRemByIndex(bucket, key, 0, 1)
        fmt.Printf("removed num %d\n", removedNum)
        return err
    }); err != nil {
    log.Fatal(err)
}
```

##### LSet 

Sets the list element at index to value.

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        if err := tx.LSet(bucket, key, 0, []byte("val11")); err != nil {
            return err
        } else {
            fmt.Println("LSet ok, index 0 item value => val11")
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### Ltrim 

Trims an existing list so that it will contain only the specified range of elements specified.
The offsets start and stop are zero-based indexes 0 being the first element of the list (the head of the list),
1 being the next element and so on.Start and end can also be negative numbers indicating offsets from the end of the list,
where -1 is the last element of the list, -2 the penultimate element and so on.

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        return tx.LTrim(bucket, key, 0, 1)
    }); err != nil {
    log.Fatal(err)
}
```

##### LSize 

Returns the size of key in the bucket in the bucket at given bucket and key.

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        if size,err := tx.LSize(bucket, key); err != nil {
            return err
        } else {
            fmt.Println("myList size is ",size)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### LKeys

find all `keys` of type `List` matching a given `pattern`, similar to Redis command: [KEYS](https://redis.io/commands/keys/)

Note: pattern matching use `filepath.Match`, It is different from redis' behavior in some details, such as `[`.

```golang
if err := db.View(
    func(tx *nutsdb.Tx) error {
        var keys []string
        err := tx.LKeys(bucket, "*", func(key string) bool {
            keys = append(keys, key)
            // true: continue, false: break
            return true
        })
        fmt.Printf("keys: %v\n", keys)
        return err
    }); err != nil {
    log.Fatal(err)
}
```

#### Set

##### SAdd

Adds the specified members to the set stored int the bucket at given bucket,key and items.

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForSet"
        key := []byte("mySet")
        return tx.SAdd(bucket, key, []byte("a"), []byte("b"), []byte("c"))
    }); err != nil {
    log.Fatal(err)
}
```

##### SAreMembers 

Returns if the specified members are the member of the set int the bucket at given bucket,key and items.

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "bucketForSet"
        key := []byte("mySet")
        if ok, err := tx.SAreMembers(bucket, key, []byte("a"), []byte("b"), []byte("c")); err != nil {
            return err
        } else {
            fmt.Println("SAreMembers:", ok)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### SCard 

Returns the set cardinality (number of elements) of the set stored in the bucket at given bucket and key.

```go

if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "bucketForSet"
        key := []byte("mySet")
        if num, err := tx.SCard(bucket, key); err != nil {
            return err
        } else {
            fmt.Println("SCard:", num)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

```
##### SDiffByOneBucket 

Returns the members of the set resulting from the difference between the first set and all the successive sets in one bucket.

```go

key1 := []byte("mySet1")
key2 := []byte("mySet2")
bucket := "bucketForSet"

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket, key1, []byte("a"), []byte("b"), []byte("c"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket, key2, []byte("c"), []byte("d"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SDiffByOneBucket(bucket, key1, key2); err != nil {
            return err
        } else {
            fmt.Println("SDiffByOneBucket:", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
            //item a
            //item b
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

```

##### SDiffByTwoBuckets 

Returns the members of the set resulting from the difference between the first set and all the successive sets in two buckets.

```go
bucket1 := "bucket1"
key1 := []byte("mySet1")

bucket2 := "bucket2"
key2 := []byte("mySet2")

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket1, key1, []byte("a"), []byte("b"), []byte("c"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket2, key2, []byte("c"), []byte("d"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SDiffByTwoBuckets(bucket1, key1, bucket2, key2); err != nil {
            return err
        } else {
            fmt.Println("SDiffByTwoBuckets:", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

```
##### SHasKey 

Returns if the set in the bucket at given bucket and key.

```go

bucket := "bucketForSet"

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if ok, err := tx.SHasKey(bucket, []byte("mySet")); err != nil {
            return err
        } else {
            fmt.Println("SHasKey", ok)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

```
##### SIsMember 

Returns if member is a member of the set stored in the bucket at given bucket, key and item.

```go
bucket := "bucketForSet"

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if ok, err := tx.SIsMember(bucket, []byte("mySet"), []byte("a")); err != nil {
            return err
        } else {
            fmt.Println("SIsMember", ok)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### SMembers 

Returns all the members of the set value stored in the bucket at given bucket and key.

```
bucket := "bucketForSet"

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SMembers(bucket, []byte("mySet")); err != nil {
            return err
        } else {
            fmt.Println("SMembers", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### SMoveByOneBucket 

Moves member from the set at source to the set at destination in one bucket.

```go
bucket3 := "bucket3"

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket3, []byte("mySet1"), []byte("a"), []byte("b"), []byte("c"))
    }); err != nil {
    log.Fatal(err)
}
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket3, []byte("mySet2"), []byte("c"), []byte("d"), []byte("e"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        if ok, err := tx.SMoveByOneBucket(bucket3, []byte("mySet1"), []byte("mySet2"), []byte("a")); err != nil {
            return err
        } else {
            fmt.Println("SMoveByOneBucket", ok)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SMembers(bucket3, []byte("mySet1")); err != nil {
            return err
        } else {
            fmt.Println("after SMoveByOneBucket bucket3 mySet1 SMembers", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SMembers(bucket3, []byte("mySet2")); err != nil {
            return err
        } else {
            fmt.Println("after SMoveByOneBucket bucket3 mySet2 SMembers", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### SMoveByTwoBuckets 

Moves member from the set at source to the set at destination in two buckets.

```go
bucket4 := "bucket4"
bucket5 := "bucket5"
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket4, []byte("mySet1"), []byte("a"), []byte("b"), []byte("c"))
    }); err != nil {
    log.Fatal(err)
}
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket5, []byte("mySet2"), []byte("c"), []byte("d"), []byte("e"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        if ok, err := tx.SMoveByTwoBuckets(bucket4, []byte("mySet1"), bucket5, []byte("mySet2"), []byte("a")); err != nil {
            return err
        } else {
            fmt.Println("SMoveByTwoBuckets", ok)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SMembers(bucket4, []byte("mySet1")); err != nil {
            return err
        } else {
            fmt.Println("after SMoveByTwoBuckets bucket4 mySet1 SMembers", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SMembers(bucket5, []byte("mySet2")); err != nil {
            return err
        } else {
            fmt.Println("after SMoveByTwoBuckets bucket5 mySet2 SMembers", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### SPop 

Removes and returns one or more random elements from the set value store in the bucket at given bucket and key.

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        key := []byte("mySet")
        if item, err := tx.SPop(bucket, key); err != nil {
            return err
        } else {
            fmt.Println("SPop item from mySet:", string(item))
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### SRem 

Removes the specified members from the set stored in the bucket at given bucket,key and items.

```go
bucket6:="bucket6"
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket6, []byte("mySet"), []byte("a"), []byte("b"), []byte("c"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        if err := tx.SRem(bucket6, []byte("mySet"), []byte("a")); err != nil {
            return err
        } else {
            fmt.Println("SRem ok")
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SMembers(bucket6, []byte("mySet")); err != nil {
            return err
        } else {
            fmt.Println("SMembers items:", items)
            for _, item := range items {
                fmt.Println("item:", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### SUnionByOneBucket 

The members of the set resulting from the union of all the given sets in one bucket.

```go
bucket7 := "bucket1"
key1 := []byte("mySet1")
key2 := []byte("mySet2")

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket7, key1, []byte("a"), []byte("b"), []byte("c"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket7, key2, []byte("c"), []byte("d"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SUnionByOneBucket(bucket7, key1, key2); err != nil {
            return err
        } else {
            fmt.Println("SUnionByOneBucket:", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### SUnionByTwoBuckets 

The members of the set resulting from the union of all the given sets in two buckets.

```go
bucket8 := "bucket1"
key1 := []byte("mySet1")

bucket9 := "bucket2"
key2 := []byte("mySet2")

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket8, key1, []byte("a"), []byte("b"), []byte("c"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket9, key2, []byte("c"), []byte("d"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SUnionByTwoBuckets(bucket8, key1, bucket9, key2); err != nil {
            return err
        } else {
            fmt.Println("SUnionByTwoBucket:", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### SKeys

find all `keys` of type `Set` matching a given `pattern`, similar to Redis command: [KEYS](https://redis.io/commands/keys/)

Note: pattern matching use `filepath.Match`, It is different from redis' behavior in some details, such as `[`.

```golang
if err := db.View(
    func(tx *nutsdb.Tx) error {
        var keys []string
        err := tx.SKeys(bucket, "*", func(key string) bool {
            keys = append(keys, key)
            // true: continue, false: break
            return true
        })
        fmt.Printf("keys: %v\n", keys)
        return err
    }); err != nil {
    log.Fatal(err)
}
```

#### Sorted Set

##### ZAdd

Adds the specified member with the specified score and the specified value to the sorted set stored at bucket.

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        return tx.ZAdd(bucket, key, 1, []byte("val1"))
    }); err != nil {
    log.Fatal(err)
}
```
##### ZCard 

Returns the sorted set cardinality (number of elements) of the sorted set stored at bucket.

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        if num, err := tx.ZCard(bucket); err != nil {
            return err
        } else {
            fmt.Println("ZCard num", num)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### ZCount 

Returns the number of elements in the sorted set at bucket with a score between min and max and opts.

Opts includes the following parameters:

* Limit        int  // limit the max nodes to return
* ExcludeStart bool // exclude start value, so it search in interval (start, end] or (start, end)
* ExcludeEnd   bool // exclude end value, so it search in interval [start, end) or (start, end)

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        if num, err := tx.ZCount(bucket, 0, 1, nil); err != nil {
            return err
        } else {
            fmt.Println("ZCount num", num)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### ZGetByKey 

Returns node in the bucket at given bucket and key.

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key2")
        if node, err := tx.ZGetByKey(bucket, key); err != nil {
            return err
        } else {
            fmt.Println("ZGetByKey key2 val:", string(node.Value))
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### ZMembers 

Returns all the members of the set value stored at bucket.

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        if nodes, err := tx.ZMembers(bucket); err != nil {
            return err
        } else {
            fmt.Println("ZMembers:", nodes)

            for _, node := range nodes {
                fmt.Println("member:", node.Key(), string(node.Value))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### ZPeekMax 

Returns the member with the highest score in the sorted set stored at bucket.

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        if node, err := tx.ZPeekMax(bucket); err != nil {
            return err
        } else {
            fmt.Println("ZPeekMax:", string(node.Value))
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### ZPeekMin 

Returns the member with lowest score in the sorted set stored at bucket.

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        if node, err := tx.ZPeekMin(bucket); err != nil {
            return err
        } else {
            fmt.Println("ZPeekMin:", string(node.Value))
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### ZPopMax 

Removes and returns the member with the highest score in the sorted set stored at bucket.

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        if node, err := tx.ZPopMax(bucket); err != nil {
            return err
        } else {
            fmt.Println("ZPopMax:", string(node.Value)) //val3
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### ZPopMin 

Removes and returns the member with the lowest score in the sorted set stored at bucket.

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        if node, err := tx.ZPopMin(bucket); err != nil {
            return err
        } else {
            fmt.Println("ZPopMin:", string(node.Value)) //val1
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### ZRangeByRank 

Returns all the elements in the sorted set in one bucket at bucket and key with a rank between start and end (including elements with rank equal to start or end).

```go
// ZAdd add items
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet2"
        key1 := []byte("key1")
        return tx.ZAdd(bucket, key1, 1, []byte("val1"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet2"
        key2 := []byte("key2")
        return tx.ZAdd(bucket, key2, 2, []byte("val2"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet2"
        key3 := []byte("key3")
        return tx.ZAdd(bucket, key3, 3, []byte("val3"))
    }); err != nil {
    log.Fatal(err)
}

// ZRangeByRank
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet2"
        if nodes, err := tx.ZRangeByRank(bucket, 1, 2); err != nil {
            return err
        } else {
            fmt.Println("ZRangeByRank nodes :", nodes)
            for _, node := range nodes {
                fmt.Println("item:", node.Key(), node.Score())
            }
            
            //item: key1 1
            //item: key2 2
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### ZRangeByScore 

Returns all the elements in the sorted set at key with a score between min and max.
And the parameter `Opts` is the same as ZCount's.

```go
// ZAdd
if err := db.Update(
        func(tx *nutsdb.Tx) error {
            bucket := "myZSet3"
            key1 := []byte("key1")
            return tx.ZAdd(bucket, key1, 70, []byte("val1"))
        }); err != nil {
        log.Fatal(err)
    }

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet3"
        key2 := []byte("key2")
        return tx.ZAdd(bucket, key2, 90, []byte("val2"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet3"
        key3 := []byte("key3")
        return tx.ZAdd(bucket, key3, 86, []byte("val3"))
    }); err != nil {
    log.Fatal(err)
}

// ZRangeByScore
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet3"
        if nodes, err := tx.ZRangeByScore(bucket, 80, 100,nil); err != nil {
            return err
        } else {
            fmt.Println("ZRangeByScore nodes :", nodes)
            for _, node := range nodes {
                fmt.Println("item:", node.Key(), node.Score())
            }
            //item: key3 86
            //item: key2 90
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}   
```
##### ZRank

Returns the rank of member in the sorted set stored in the bucket at given bucket and key, with the scores ordered from low to high.

```go

// ZAdd
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet4"
        key1 := []byte("key1")
        return tx.ZAdd(bucket, key1, 70, []byte("val1"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet4"
        key2 := []byte("key2")
        return tx.ZAdd(bucket, key2, 90, []byte("val2"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet4"
        key3 := []byte("key3")
        return tx.ZAdd(bucket, key3, 86, []byte("val3"))
    }); err != nil {
    log.Fatal(err)
}

// ZRank
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet4"
        key1 := []byte("key1")
        if rank, err := tx.ZRank(bucket, key1); err != nil {
            return err
        } else {
            fmt.Println("key1 ZRank :", rank) // key1 ZRank : 1
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

#### ZRevRank

Returns the rank of member in the sorted set stored in the bucket at given bucket and key,with the scores ordered from high to low.

```go
// ZAdd
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet8"
        key1 := []byte("key1")
        return tx.ZAdd(bucket, key1, 10, []byte("val1"))
    }); err != nil {
    log.Fatal(err)
}
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet8"
        key2 := []byte("key2")
        return tx.ZAdd(bucket, key2, 20, []byte("val2"))
    }); err != nil {
    log.Fatal(err)
}
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet8"
        key3 := []byte("key3")
        return tx.ZAdd(bucket, key3, 30, []byte("val3"))
    }); err != nil {
    log.Fatal(err)
}

// ZRevRank
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet8"
        if rank, err := tx.ZRevRank(bucket, []byte("key3")); err != nil {
            return err
        } else {
            fmt.Println("ZRevRank key1 rank:", rank) //ZRevRank key3 rank: 1
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### ZRem 

Removes the specified members from the sorted set stored in one bucket at given bucket and key.

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet5"
        key1 := []byte("key1")
        return tx.ZAdd(bucket, key1, 10, []byte("val1"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet5"
        key2 := []byte("key2")
        return tx.ZAdd(bucket, key2, 20, []byte("val2"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet5"
        if nodes,err := tx.ZMembers(bucket); err != nil {
            return err
        } else {
            fmt.Println("before ZRem key1, ZMembers nodes",nodes)
            for _,node:=range nodes {
                fmt.Println("item:",node.Key(),node.Score())
            }
        }
        // before ZRem key1, ZMembers nodes map[key1:0xc00008cfa0 key2:0xc00008d090]
        // item: key1 10
        // item: key2 20
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet5"
        if err := tx.ZRem(bucket, "key1"); err != nil {
            return err
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet5"
        if nodes,err := tx.ZMembers(bucket); err != nil {
            return err
        } else {
            fmt.Println("after ZRem key1, ZMembers nodes",nodes)
            for _,node:=range nodes {
                fmt.Println("item:",node.Key(),node.Score())
            }
            // after ZRem key1, ZMembers nodes map[key2:0xc00008d090]
            // item: key2 20
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

```

##### ZRemRangeByRank 

Removes all elements in the sorted set stored in one bucket at given bucket with rank between start and end.
The rank is 1-based integer. Rank 1 means the first node; Rank -1 means the last node.

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet6"
        key1 := []byte("key1")
        return tx.ZAdd(bucket, key1, 10, []byte("val1"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet6"
        key2 := []byte("key2")
        return tx.ZAdd(bucket, key2, 20, []byte("val2"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet6"
        key3 := []byte("key3")
        return tx.ZAdd(bucket, key3, 30, []byte("val2"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet6"
        if nodes,err := tx.ZMembers(bucket); err != nil {
            return err
        } else {
            fmt.Println("before ZRemRangeByRank, ZMembers nodes",nodes)
            for _,node:=range nodes {
                fmt.Println("item:",node.Key(),node.Score())
            }
            // before ZRemRangeByRank, ZMembers nodes map[key3:0xc00008d450 key1:0xc00008d270 key2:0xc00008d360]
            // item: key1 10
            // item: key2 20
            // item: key3 30
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet6"
        if err := tx.ZRemRangeByRank(bucket, 1,2); err != nil {
            return err
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet6"
        if nodes,err := tx.ZMembers(bucket); err != nil {
            return err
        } else {
            fmt.Println("after ZRemRangeByRank, ZMembers nodes",nodes)
            for _,node:=range nodes {
                fmt.Println("item:",node.Key(),node.Score())
            }
            // after ZRemRangeByRank, ZMembers nodes map[key3:0xc00008d450]
            // item: key3 30
            // key1 ZScore 10
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### ZScore

Returns the score of member in the sorted set in the bucket at given bucket and key.

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet7"
        if score,err := tx.ZScore(bucket, []byte("key1")); err != nil {
            return err
        } else {
            fmt.Println("ZScore key1 score:",score)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### ZKeys

find all `keys` of type `Sorted Set` matching a given `pattern`, similar to Redis command: [KEYS](https://redis.io/commands/keys/)

Note: pattern matching use `filepath.Match`, It is different from redis' behavior in some details, such as `[`.

```golang
if err := db.View(
    func(tx *nutsdb.Tx) error {
        var keys []string
        err := tx.ZKeys(bucket, "*", func(key string) bool {
            keys = append(keys, key)
            // true: continue, false: break
            return true
        })
        fmt.Printf("keys: %v\n", keys)
        return err
    }); err != nil {
    log.Fatal(err)
}
```

### Comparison with other databases

#### BoltDB

BoltDB is similar to NutsDB, both use B+tree and support transaction. However, Bolt uses a B+tree internally and only a single file, and NutsDB is based on bitcask model with multiple log files. NutsDB supports TTL and many data structures, but BoltDB does not support them .

#### LevelDB, RocksDB

LevelDB and RocksDB are based on a log-structured merge-tree (LSM tree).An LSM tree optimizes random writes by using a write ahead log and multi-tiered, sorted files called SSTables. LevelDB does not have transactions. It supports batch writing of key/value pairs and it supports read snapshots but it will not give you the ability to do a compare-and-swap operation safely. NutsDB supports many data structures, but RocksDB does not support them.

#### Badger

Badger is based in LSM tree with value log. It designed for SSDs. It also supports transaction and TTL. But in my benchmark its write performance is not as good as i thought. In addition, NutsDB supports data structures such as list、set、sorted set, but Badger does not support them.

### Benchmarks

## Tested kvstore 

Selected kvstore which is embedded, persistence and support transactions.

* [BadgerDB](https://github.com/dgraph-io/badger) (master branch with default options)
* [BoltDB](https://github.com/boltdb/bolt) (master branch  with default options)
* [NutsDB](https://github.com/xujiajun/nutsdb) (master branch with default options or custom options)

## Benchmark System:

* Go Version : go1.11.4 darwin/amd64
* OS: Mac OS X 10.13.6
* Architecture: x86_64
* 16 GB 2133 MHz LPDDR3
* CPU: 3.1 GHz Intel Core i7

##  Benchmark results:

```
badger 2019/03/11 18:06:05 INFO: All 0 tables opened in 0s
goos: darwin
goarch: amd64
pkg: github.com/xujiajun/kvstore-bench
BenchmarkBadgerDBPutValue64B-8         10000        112382 ns/op        2374 B/op         74 allocs/op
BenchmarkBadgerDBPutValue128B-8        20000         94110 ns/op        2503 B/op         74 allocs/op
BenchmarkBadgerDBPutValue256B-8        20000         93480 ns/op        2759 B/op         74 allocs/op
BenchmarkBadgerDBPutValue512B-8        10000        101407 ns/op        3271 B/op         74 allocs/op
BenchmarkBadgerDBGet-8               1000000          1552 ns/op         416 B/op          9 allocs/op
BenchmarkBoltDBPutValue64B-8           10000        203128 ns/op       21231 B/op         62 allocs/op
BenchmarkBoltDBPutValue128B-8           5000        229568 ns/op       13716 B/op         64 allocs/op
BenchmarkBoltDBPutValue256B-8          10000        196513 ns/op       17974 B/op         64 allocs/op
BenchmarkBoltDBPutValue512B-8          10000        199805 ns/op       17064 B/op         64 allocs/op
BenchmarkBoltDBGet-8                 1000000          1122 ns/op         592 B/op         10 allocs/op
BenchmarkNutsDBPutValue64B-8           30000         53614 ns/op         626 B/op         14 allocs/op
BenchmarkNutsDBPutValue128B-8          30000         51998 ns/op         664 B/op         13 allocs/op
BenchmarkNutsDBPutValue256B-8          30000         53958 ns/op         920 B/op         13 allocs/op
BenchmarkNutsDBPutValue512B-8          30000         55787 ns/op        1432 B/op         13 allocs/op
BenchmarkNutsDBGet-8                 2000000           661 ns/op          88 B/op          3 allocs/op
BenchmarkNutsDBGetByHintKey-8          50000         27255 ns/op         840 B/op         16 allocs/op
PASS
ok      github.com/xujiajun/kvstore-bench   83.856s
```

## Conclusions:

### Put(write) Performance: 

NutsDB is fastest. NutsDB is 2-5x faster than BoltDB, 0.5-2x faster than BadgerDB.
And BadgerDB is 1-3x faster than BoltDB.

### Get(read) Performance: 

All are fast. And NutsDB is 1x faster than others.
And NutsDB reads with HintKey option is much slower than its default option way. 


the benchmark code can be found in the [gokvstore-bench](https://github.com/xujiajun/gokvstore-bench) repo.

### Caveats & Limitations

#### Index mode

From the version v0.3.0, NutsDB supports two modes about entry index: `HintKeyValAndRAMIdxMode`  and  `HintKeyAndRAMIdxMode`. From the version v0.5.0, NutsDB supports `HintBPTSparseIdxMode` mode.

The default mode use `HintKeyValAndRAMIdxMode`, entries are indexed base on RAM, so its read/write performance is fast. but can’t handle databases much larger than the available physical RAM. If you set the `HintKeyAndRAMIdxMode` mode, HintIndex will not cache the value of the entry. Its write performance is also fast. To retrieve a key by seeking to offset relative to the start of the data file, so its read performance more slowly that RAM way, but it can save memory. The mode `HintBPTSparseIdxMode` is based b+ tree sparse index, this mode saves memory very much (1 billion data only uses about 80MB of memory). And other data structures such as ***list, set, sorted set only supported with mode HintKeyValAndRAMIdxMode***.
***It cannot switch back and forth between modes because the index structure is different***.

NutsDB will truncate data file if the active file is larger than  `SegmentSize`, so the size of an entry can not be set larger than `SegmentSize` , default `SegmentSize` is 8MB, you can set it(opt.SegmentSize) as option before DB opening. ***Once set, it cannot be changed***.

#### Support OS

NutsDB currently works on Mac OS, Linux and Windows.  

#### About merge operation

The HintBPTSparseIdxMode mode does not support the merge operation of the current version.

#### About transactions

Recommend use the latest version.

### Contact

* [xujiajun](https://github.com/xujiajun)

### Contributing

See [CONTRIBUTING](https://github.com/xujiajun/nutsdb/blob/master/CONTRIBUTING.md) for details on submitting patches and the contribution workflow.

### Acknowledgements

This package is inspired by the following:

* [Bitcask-intro](https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf)
* [BoltDB](https://github.com/boltdb)
* [BuntDB](https://github.com/tidwall/buntdb)
* [Redis](https://redis.io)
* [Sorted Set](https://github.com/wangjia184/sortedset)

### License

The NutsDB is open-sourced software licensed under the [Apache 2.0 license](https://github.com/xujiajun/nutsdb/blob/master/LICENSE).
