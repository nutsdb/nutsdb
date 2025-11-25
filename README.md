<p align="center">
    <img src="https://user-images.githubusercontent.com/6065007/141310364-62d7eebb-2cbb-4949-80ed-5cd20f705405.png">
</p>

<div class="column" align="middle">
  <a href="https://godoc.org/github.com/nutsdb/nutsdb"><img src="https://godoc.org/github.com/nutsdb/nutsdb?status.svg" /></a>
  <a href="https://goreportcard.com/report/github.com/nutsdb/nutsdb"><img src="https://goreportcard.com/badge/github.com/nutsdb/nutsdb" /></a>
  <a href="https://github.com/nutsdb/nutsdb/actions/workflows/go.yml"><img src="https://github.com/nutsdb/nutsdb/actions/workflows/go.yml/badge.svg"/></a>
  <a href="https://codecov.io/gh/nutsdb/nutsdb"><img src="https://codecov.io/gh/nutsdb/nutsdb/branch/master/graph/badge.svg?token=CupujOXpbe"/></a>
  <a href="https://raw.githubusercontent.com/nutsdb/nutsdb/master/LICENSE"><img src="http://img.shields.io/badge/license-Apache_2-blue.svg?style=flat-square"/></a>
  <a href="https://github.com/avelino/awesome-go#database"><img src="https://awesome.re/mentioned-badge.svg"/></a>
</div>

## What is NutsDB?

English | [简体中文](https://github.com/nutsdb/nutsdb/blob/master/README-CN.md)

NutsDB is a simple, fast, embeddable and persistent key/value store written in pure Go.

It supports fully serializable transactions and many data structures such as list、set、sorted set. All operations happen inside a Tx. Tx represents a transaction, which can be read-only or read-write. Read-only transactions can read values for a given bucket and a given key or iterate over a set of key-value pairs. Read-write transactions can read, update and delete keys from the DB.

We can learn more about NutsDB in details on the documents site of NutsDB: [NutsDB Documents](https://nutsdb.github.io/nutsdb-docs/)

## Announcement

* v1.0.0 release, see for details: [https://github.com/nutsdb/nutsdb/releases/tag/v1.0.0](https://github.com/nutsdb/nutsdb/releases/tag/v1.0.0)
* v0.14.3 release, see for details: [https://github.com/nutsdb/nutsdb/releases/tag/v0.14.3](https://github.com/nutsdb/nutsdb/releases/tag/v0.14.3)
* v0.14.2 release, see for details: [https://github.com/nutsdb/nutsdb/releases/tag/v0.14.2](https://github.com/nutsdb/nutsdb/releases/tag/v0.14.2)
* v0.14.1 release, see for details: [https://github.com/nutsdb/nutsdb/releases/tag/v0.14.1](https://github.com/nutsdb/nutsdb/releases/tag/v0.14.1)

📢 Note: Starting from v0.9.0, **defaultSegmentSize** in **DefaultOptions** has been adjusted from **8MB** to **256MB**. The original value is the default value, which needs to be manually changed to 8MB, otherwise the original data will not be parsed. The reason for the size adjustment here is that there is a cache for file descriptors starting from v0.9.0 (detail see https://github.com/nutsdb/nutsdb/pull/164 ), so users need to look at the number of fds they use on the server, which can be set manually. If you have any questions, you can open an issue.

After **nutsdb v1.0.0**, due to changes in the underlying data storage protocol, **the data of the old version is not compatible**. Please rewrite it before using the new version. And the current Bucket needs to be created manually. Please see the Bucket usage [documentation](./docs/user_guides/use-buckets.md) for details.

## Architecture
![nutsdb-架构图](./docs/img/nutsdb-架构图.png)


 Welcome [contributions to NutsDB](https://github.com/nutsdb/nutsdb#contributing).

## Quick start

### Install NutsDB

To start using NutsDB, first needs [Go](https://golang.org/dl/) installed (version 1.18+ is required).  and run go get:

```
go get -u github.com/nutsdb/nutsdb
```

### Opening a database

To open your database, use the nutsdb.Open() function,with the appropriate options.The `Dir` , `EntryIdxMode`  and  `SegmentSize`  options are must be specified by the client. About options see [here](https://github.com/nutsdb/nutsdb#options) for detail.

```go
package main

import (
    "log"

    "github.com/nutsdb/nutsdb"
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

## Merge V2 and HintFile

### Overview

NutsDB introduces **Merge V2** and **HintFile** features to significantly improve database performance, especially for startup time and memory efficiency during large-scale compaction operations.

### Merge V2

**Merge V2** is a high-performance compaction algorithm that optimizes memory usage while maintaining data consistency. It's designed to handle large-scale data compaction efficiently.

#### Key Features

- **Memory Efficiency**: Reduces memory usage by ~65% during merge operations (from ~145 bytes/entry to ~50 bytes/entry)
- **Concurrent Safety**: Allows concurrent writes during merge operations
- **Atomic Operations**: Ensures data consistency through atomic commit and rollback
- **Large-Scale Support**: Optimized for handling 10GB+ datasets efficiently

#### How It Works

1. **Preparation Phase**: Enumerates files and validates merge state
2. **Rewrite Phase**: Processes data files and rewrites valid entries to new merge files
3. **Commit Phase**: Updates in-memory indexes and writes hint files
4. **Finalization**: Ensures all data is persisted and cleans up old files

#### File ID Strategy

Merge V2 uses negative FileIDs for merge files to ensure correct processing order:
- Merge files: Negative IDs (starting from `math.MinInt64`)
- Normal data files: Positive IDs (starting from 0)
- Processing order: Merge files are always processed before normal files during index rebuild

#### Configuration

```go
// Merge V2 is enabled by default and works automatically
// You can configure merge behavior through options

options := nutsdb.DefaultOptions
options.SegmentSize = 256 * 1024 * 1024 // 256MB segments

db, err := nutsdb.Open(options, nutsdb.WithDir("/tmp/nutsdb"))
```

### HintFile

**HintFile** is an index persistence feature that dramatically reduces database startup time by maintaining persistent indexes of key metadata.

#### Key Features

- **Fast Startup**: Eliminates full data file scans during database recovery
- **Automatic Fallback**: Gracefully falls back to traditional scanning if hint files are missing or corrupted
- **Transparent Operation**: Works seamlessly with existing data structures
- **Configurable**: Can be enabled/disabled via configuration

#### How It Works

1. **During Merge**: Hint files are automatically created alongside merge data files
2. **During Startup**: Database loads hint files to reconstruct in-memory indexes instantly
3. **Fallback Mechanism**: If hint files are unavailable, the system falls back to scanning data files

#### Hint File Structure

Each hint entry contains:
- Bucket ID, Key metadata, Value size
- Timestamp, TTL, and operation flags
- File ID and data position for direct access
- Key data for lookup operations

#### Configuration

```go
// Basic configuration with both features enabled
options := nutsdb.DefaultOptions
options.EnableHintFile = true
options.EnableMergeV2 = true
options.SegmentSize = 256 * 1024 * 1024 // 256MB segments

// Or use option functions
db, err := nutsdb.Open(
    nutsdb.DefaultOptions,
    nutsdb.WithDir("/tmp/nutsdb"),
    nutsdb.WithEnableHintFile(true),
    nutsdb.WithEnableMergeV2(true),
)
```

#### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `EnableHintFile` | bool | `false` | Enable/disable hint file feature |
| `EnableMergeV2` | bool | `false` | Enable/disable Merge V2 algorithm |
| `SegmentSize` | int64 | `256MB` | Size of data files before rotation |

### Performance Benefits

#### Memory Efficiency
- **Traditional Merge**: ~145 bytes per entry
- **Merge V2**: ~50 bytes per entry
- **Savings**: ~65% memory reduction for large datasets

#### Startup Time
- **With HintFile**: Index reconstruction in milliseconds
- **Without HintFile**: Full data file scanning (seconds to minutes)
- **Fallback**: Automatic degradation if hint files are unavailable

#### Benchmark Results

Performance testing with Intel Core i5-14600KF on large datasets:

| Configuration | Total Time | Memory Usage | Allocations | Avg Restart | Merge Time |
|---------------|------------|--------------|-------------|-------------|------------|
| Merge V1 + No HintFile | 4.29s | 2.32GB | 40.4M | 4.27s | 12.86s |
| Merge V1 + HintFile | 1.69s | 1.56GB | 25.4M | 1.64s | 16.37s |
| **Merge V2 + HintFile** | **1.87s** | **1.56GB** | **25.4M** | **1.83s** | **8.63s** |

##### Key Performance Insights

- **HintFile Impact**: 61.6% faster startup time (4.27s → 1.64s)
- **Memory Optimization**: 32.9% reduction in memory allocation with HintFile
- **Merge V2 Efficiency**: 47.3% faster merge operations (16.37s → 8.63s)
- **Overall Performance**: Best balance with Merge V2 + HintFile combination

#### Use Cases
- **Large Datasets**: Essential for databases with millions of entries
- **Frequent Restarts**: Critical for applications requiring fast restarts
- **Memory-Constrained Environments**: Reduces peak memory usage during maintenance
- **High Availability**: Enables quick database recovery after failures

### Best Practices

1. **Enable HintFile** for production workloads with large datasets
2. **Monitor Disk Space** - Hint files add additional storage overhead
3. **Regular Merges** - Schedule merges during low-traffic periods
4. **Backup Strategy** - Include hint files in your backup process

### Migration Guide

#### Enabling HintFile in Existing Database

```go
// Step 1: Backup your database
// Step 2: Enable HintFile and perform a merge to create hint files
options := nutsdb.DefaultOptions
options.EnableHintFile = true
options.EnableMergeV2 = true

db, err := nutsdb.Open(options)
if err != nil {
    log.Fatal(err)
}

// Step 3: Perform a merge to generate hint files
if err := db.Merge(); err != nil {
    log.Fatal(err)
}

// Step 4: Restart database - hint files will be used for fast startup
db.Close()
db, err = nutsdb.Open(options)
```

#### Disabling HintFile

```go
// Simply set EnableHintFile to false
options := nutsdb.DefaultOptions
options.EnableHintFile = false
options.EnableMergeV2 = true // Keep Merge V2 if desired

// Hint files will be ignored but not automatically deleted
// You can manually delete .hint files if you want to reclaim space
```

### Troubleshooting

#### Common Issues

1. **HintFile Corruption**
   ```
   Symptom: Database startup fails or takes long time
   Solution: NutsDB automatically falls back to scanning data files
   ```

2. **Merge V2 Memory Usage**
   ```
   Symptom: High memory usage during merge operations
   Solution: Reduce SegmentSize or disable HintFile temporarily
   ```

3. **Slow Startup with Large Dataset**
   ```
   Symptom: Database takes minutes to start
   Solution: Enable HintFile and perform a manual merge
   ```

#### Recovery Procedures

```go
// Check database health
db, err := nutsdb.Open(options)
if err != nil {
    // Try disabling HintFile if corruption suspected
    options.EnableHintFile = false
    db, err = nutsdb.Open(options)
}

// Force rebuild hint files
if err := db.Merge(); err != nil {
    log.Printf("Merge failed: %v", err)
}
```

### Compatibility

- **Backward Compatible**: Works with existing NutsDB databases
- **Configurable**: HintFile can be disabled if not needed
- **Data Structures**: Supports all NutsDB data structures (BTree, Set, List, SortedSet)
- **Graceful Degradation**: Automatic fallback when hint files are unavailable

## Documentation

<details>
  <summary><b>Buckets</b></summary>

- [Using buckets](./docs/user_guides/use-buckets.md)
</details>

<details>
  <summary><b>Pairs</b></summary>

- [Using key/value pairs](./docs/user_guides/use-kv-pair.md)
</details>

<details>
  <summary><b>Iterator</b></summary>

- [Iterating over keys](./docs/user_guides/iterator.md)
</details>

<details>
  <summary><b>Data Structures</b></summary>

- [List](./docs/user_guides/data-structure.md#list)
- [Set](./docs/user_guides/data-structure.md#set)
- [Sorted Set](./docs/user_guides/data-structure.md#sorted-set)
</details>

<details>
  <summary><b>Database Options</b></summary>

- [Options](./docs/user_guides/options.md)
</details>

<details>
  <summary><b>Advanced Features</b></summary>

- [Merge V2 and HintFile](#merge-v2-and-hintfile)
- [Watch Key Changes](./docs/user_guides/others.md#watch-key-changes)
- [More Operation](./docs/user_guides/others.md)
</details>

<details>
  <summary><b>Comparison</b></summary>

- [Comparison](./docs/user_guides/comparison.md)
</details>

<details>
  <summary><b>Benchmark</b></summary>

- [Benchmark](./docs/user_guides/benchmarks.md)
</details>

## Contributors

Thank you for considering contributing to NutsDB! The contribution guide can be found in the [CONTRIBUTING](https://github.com/nutsdb/nutsdb/blob/master/CONTRIBUTING.md) for details on submitting patches and the contribution workflow.

<a href="https://github.com/nutsdb/nutsdb/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=nutsdb/nutsdb" />
</a>

## Acknowledgements

This package is inspired by the following:

- [Bitcask-intro](https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf)
- [BoltDB](https://github.com/boltdb)
- [BuntDB](https://github.com/tidwall/buntdb)
- [Redis](https://redis.io/)
- [Sorted Set](https://github.com/wangjia184/sortedset)

## License

The NutsDB is open-sourced software licensed under the [Apache 2.0 license](https://github.com/nutsdb/nutsdb/blob/master/LICENSE).
