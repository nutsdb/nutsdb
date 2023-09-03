# Transactions

NutsDB allows only one read-write transaction at a time but allows as many read-only transactions as you want at a time. Each transaction has a consistent view of the data as it existed when the transaction started.

When a transaction fails, it will roll back, and revert all changes that occurred to the database during that transaction. If the option SyncEnable is set to true, when a read/write transaction succeeds, all changes are persisted to disk.

Creating transaction from the DB is thread safe.

<table>
<thead><tr><th>Read-write</th><th>Read-only</th></tr></thead>
<tbody>
<tr><td>

```go
err := db.Update(
func(tx *nutsdb.Tx) error {
...
return nil
})
```

</td><td>

```go
err := db.View(
func(tx *nutsdb.Tx) error {
...
return nil
})
```

</td></tr>
</tbody></table>

### Managing manually

The DB.View() and DB.Update() functions are wrappers around the DB.Begin() function. These helper functions will start the transaction, execute a function, and then safely close your transaction if an error is returned. This is the recommended way to use NutsDB transactions.

However, sometimes you may want to manually start and end your transactions. You can use the DB.Begin() function directly but please be sure to close the transaction.

```go
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