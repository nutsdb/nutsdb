# Iterator

NutsDB stores its keys in byte-sorted order within a bucket. This makes sequential iteration over these keys extremely fast.

### Prefix scans

To iterate over a key prefix, we can use `PrefixScan` function, and the parameters `offsetNum` and `limitNum` constrain the number of entries returned :

```go
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

### Prefix search scans

To iterate over a key prefix with search by regular expression on a second part of key without prefix, we can use `PrefixSearchScan` function, and the parameters `offsetNum`, `limitNum` constrain the number of entries returned :

```go
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

### Range scans

To scan over a range, we can use `RangeScan` function. For example：

```go
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

### Get all

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

### iterator

The option parameter `Reverse` that determines whether the iterator is forward or Reverse. The current version does not support the iterator for HintBPTSparseIdxMode.

<table>
<thead><tr><th>Forward</th><th>Reverse</th></tr></thead>
<tbody>
<tr><td>

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

</td><td>

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