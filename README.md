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
    - [Batch read-write transactions](#batch-read-write-transactions)
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
