# How to use key/value pairs

To save a key/value pair to a bucket, use the `tx.Put`:

```go
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

To update the the value of the "name1" key,we can still use the `tx.Put`:

```go
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

To retrieve this value, we can use the `tx.Get`:

```go
if err := db.View(
func(tx *nutsdb.Tx) error {
    key := []byte("name1")
    bucket := "bucket1"
    if value, err := tx.Get(bucket, key); err != nil {
        return err
    } else {
        fmt.Println(string(value)) // "val1-modify"
    }
    return nil
}); err != nil {
    log.Println(err)
}
```

Use the `tx.Delete()` to delete a key from the bucket:

```go
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

