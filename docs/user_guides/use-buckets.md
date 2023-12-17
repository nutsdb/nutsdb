# How to use buckets

Buckets are collections of key/value pairs within the database. All keys in a bucket must be unique. Bucket can be interpreted as a table or namespace. So you can store the same key in different bucket.

```go
key := []byte("key001")
val := []byte("val001")

bucket001 := "bucket001"

if err := db.Update(func(tx *nutsdb.Tx) error {
    // you should call Bucket with data structure and the name of bucket first
    return tx.NewBucket(nutsdb.DataStructureBTree, bucket001)
}); err != nil {
    log.Fatal(err)
}

if err := db.Update(func(tx *nutsdb.Tx) error {
    return tx.Put(bucket001, key, val, 0)
}); err != nil {
    log.Fatal(err)
}
```

Also, this bucket is related to the data structure you use. Different data index structures that use the same bucket are also different. For example, you define a bucket named bucket_foo, so you need to use the list data structure, use tx.RPush to add data, you must query or retrieve from this bucket_foo data structure, use tx.RPop, tx.LRange, etc. You cannot use tx.Get (same index type as tx.GetAll, tx.Put, tx.Delete, tx.RangeScan, etc.) to read the data in this bucket_foo, because the index structure is different. Other data structures such as Set, Sorted Set are the same.

### Iterate

IterateBuckets iterates over all the buckets that match the pattern. IterateBuckets function has three parameters: `ds`, `pattern` and function `f`.

The current version of the Iterate Buckets method supports the following EntryId Modes:

- `HintKeyValAndRAMIdxMode`：represents ram index (key and value) mode.
- `HintKeyAndRAMIdxMode`：represents ram index (only key) mode.

The `pattern` added in version `0.11.0` (represents the pattern to match):

- `pattern` syntax refer to: `filepath.Match`

The current version of `ds` (represents the data structure):

- DataStructureSet
- DataStructureSortedSet
- DataStructureBTree
- DataStructureList

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        return tx.IterateBuckets(nutsdb.DataStructureBTree, "*", func(bucket string) bool {
            fmt.Println("bucket: ", bucket)
            // true: continue, false: break
            return true
        })
    }); err != nil {
    log.Fatal(err)
}
```

### Delete

DeleteBucket represents delete bucket. DeleteBucket function has two parameters: `ds`(represents the data structure) and `bucket`.

The current version of the Iterate Buckets method supports the following EntryId Modes:

- `HintKeyValAndRAMIdxMode`：represents ram index (key and value) mode.
- `HintKeyAndRAMIdxMode`：represents ram index (only key) mode.

The current version of `ds` (represents the data structure)：

- DataStructureSet
- DataStructureSortedSet
- DataStructureBTree
- DataStructureList

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.DeleteBucket(nutsdb.DataStructureBTree, bucket)
    }); err != nil {
    log.Fatal(err)
}
```