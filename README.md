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
