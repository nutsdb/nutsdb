# NutsDB
NutsDB is a simple, fast, embeddable and persistent key/value store
written in pure Go. It supports fully serializable transactions. All operations happen inside a Tx. Tx represents a transaction, which can be read-only or read-write. Read-only transactions can read values for a
given bucket and given key , or iterate over keys. Read-write transactions can update and delete keys from the DB.


## Table of Contents

- [Getting Started](#getting-started)
  - [Installing](#installing)
  - [Opening a database](#opening-a-database)
  - [Transactions](#transactions)
    - [Read-write transactions](#read-write-transactions)
    - [Read-only transactions](#read-only-transactions)
    - [Managing transactions manually](#managing-transactions-manually)
  - [Using buckets](#using-buckets)
  - [Using key/value pairs](#using-keyvalue-pairs)
  - [Iterating over keys](#iterating-over-keys)
    - [Prefix scans](#prefix-scans)
    - [Range scans](#range-scans)
  - [Database backups](#database-backups)
- [Comparison with other databases](#comparison-with-other-databases)

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

To open your database, use the nutsdb.Open() function,with the appropriate options.The `Dir` , `EntryIdxMode`  and  `SegmentSize`  options are must be specified by the client.
```
package main

import (
	"log"

	"github.com/xujiajun/nutsdb"
)

func main() {
	// Open the database located in the /tmp/nutsdb directory.
	// It will be created if it doesn't exist.
	opt := nutsdb.DefaultOptions
	opt.Dir = "/tmp/nutsdb"
	db, err := nutsdb.Open(opt)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	...
}
```

### Transactions

NutsDB allows only one read-write transaction at a time but allows as many read-only transactions as you want at a time. Each transaction has a consistent view of the data as it existed when the transaction started.

When a transaction fails, it will roll back, and revert all changes that occurred to the database during that transaction.
When a read/write transaction succeeds all changes are persisted to disk.

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
if err = tx.Put(bucket, key, val, Persistent); err != nil {
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

### Using key/value pairs

To save a key/value pair to a bucket, use the `tx.Put` method:

```golang

if err := db.Update(
	func(tx *nutsdb.Tx) error {
	key := []byte("name1")
	val := []byte("val1")
	bucket: = "bucket1"
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

```
if err := db.Update(
	func(tx *nutsdb.Tx) error {
	key := []byte("name1")
	val := []byte("val1-modify") // Update the value
	bucket: = "bucket1"
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
	bucket: = "bucket1"
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
	bucket: = "bucket1"
	if err := tx.Delete(bucket, key); err != nil {
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

To iterate over a key prefix, we can use `PrefixScan` function, and the paramter `limitNum` constrain the number of entries returned :

```golang

if err := db.View(
	func(tx *nutsdb.Tx) error {
		prefix := []byte("user_")
		// Constrain 100 entries returned 
		if entries, err := tx.PrefixScan(bucket, prefix, 100); err != nil {
			return err
		} else {
			keys, es := nutsdb.SortedEntryKeys(entries)
			for _, key := range keys {
				fmt.Println(key, string(es[key].Value))
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
		bucket：= []byte("user_list)
		if entries, err := tx.RangeScan(bucket, start, end); err != nil {
			return err
		} else {
			keys, es := nutsdb.SortedEntryKeys(entries)
			for _, key := range keys {
				fmt.Println(key, string(es[key].Value))
			}
		}
		return nil
	}); err != nil {
	log.Fatal(err)
}
```

### Database backups
TODO


### License

The NutsDB is open-sourced software licensed under the [Apache 2.0 license](https://github.com/xujiajun/nutsdb/blob/master/LICENSE).
