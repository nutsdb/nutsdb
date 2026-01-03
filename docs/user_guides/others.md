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