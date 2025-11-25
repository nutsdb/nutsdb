# Options

About options see here for detail.

| Arguments            | Comment                                                      | Type              |
| -------------------- | ------------------------------------------------------------ | ----------------- |
| `Dir`                | `Dir` represents Open the database located in which dir.     | `string`          |
| EntryIdxMode         | `EntryIdxMode` represents using which mode to index the entries. `EntryIdxMode` includes three options: `HintKeyValAndRAMIdxMode`,`HintKeyAndRAMIdxMode` and `HintBPTSparseIdxMode`. `HintKeyValAndRAMIdxMode` represents ram index (key and value) mode, `HintKeyAndRAMIdxMode` represents ram index (only key) mode and `HintBPTSparseIdxMode` represents b+ tree sparse index mode. | EntryIdxMode      |
| RWMode               | `RWMode` represents the read and write mode. `RWMode` includes two options: `FileIO` and `MMap`. FileIO represents the read and write mode using standard I/O. And MMap represents the read and write mode using mmap. | RWMode            |
| SegmentSize          | NutsDB will truncate data file if the active file is larger than `SegmentSize`. Current version default `SegmentSize` is 8MB,but you can custom it. **The defaultSegmentSize becomes 256MB when the version is greater than 0.8.0.**<br /><br />Once set, it cannot be changed. see [caveats--limitations](https://github.com/nutsdb/nutsdb#caveats--limitations) for detail. | int64             |
| NodeNum              | `NodeNum` represents the node number.Default NodeNum is 1. `NodeNum` range [1,1023] . | int64             |
| SyncEnable           | `SyncEnable` represents if call Sync() function. if `SyncEnable` is false, high write performance but potential data loss likely. if `SyncEnable` is true, slower but persistent. | bool              |
| StartFileLoadingMode | `StartFileLoadingMode` represents when open a database which RWMode to load files. | RWMode            |
| GCWhenClose          | `GCWhenClose` represents initiative GC when calling `db.Close()`. Nutsdb doesn't<br/>immediately trigger GC on `db.Close()` by default. | bool              |
| CommitBufferSize     | `CommitBufferSize` represent the size of memory preallocated for transaction. Nutsdb will preallocate memory and reducing the number of memory allocations. | int64             |
| ErrorHandler         | `ErrorHandler` handles an error that occur during transaction. | ErrorHandler      |
| LessFunc             | `LessFunc` represents func to sort keys. Nutsdb sorts keys in lexicographical order by default. | LessFunc          |
| MergeInterval        | `MergeInterval` represent the interval for automatic merges, with 0 meaning automatic merging is disabled. Default interval is 2 hours. | time.Duration     |
| MaxBatchCount        | `MaxBatchCount` represents max entries in batch.             | int64             |
| MaxBatchSize         | `MaxBatchSize` represents max batch size in bytes.           | int64             |
| ExpiredDeleteType    | `ExpiredDeleteType ` represents the data structure used for expired deletion. TimeWheel means use the time wheel, You can use it when you need high performance or low memory usage. TimeHeap means use the time heap, You can use it when you need to delete precisely or memory usage will be high. | ExpiredDeleteType |
| EnableWatch          | `EnableWatch` toggles the watch feature. If `EnableWatch` is true, the watch feature will be enabled. The watch feature will be disabled by default. | bool              |

### Default Options

Recommend to use the `DefaultOptions`. Unless you know what you're doing.

```go
var DefaultOptions = func() Options {
    return Options{
        EntryIdxMode:      HintKeyValAndRAMIdxMode,
        SegmentSize:       defaultSegmentSize,
        NodeNum:           1,
        RWMode:            FileIO,
        SyncEnable:        true,
        CommitBufferSize:  4 * MB,
        MergeInterval:     2 * time.Hour,
        MaxBatchSize:      (15 * defaultSegmentSize / 4) / 100,
        MaxBatchCount:     (15 * defaultSegmentSize / 4) / 100 / 100,
        ExpiredDeleteType: TimeWheel,
        EnableWatch:       false,
    }
}()
```
