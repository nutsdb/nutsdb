# Benchmarks

## Environment

Selected kvstore which is embedded, persistence and support transactions.

- [BadgerDB](https://github.com/dgraph-io/badger) (master branch with default options)
- [BoltDB](https://github.com/boltdb/bolt) (master branch with default options)
- [NutsDB](https://github.com/nutsdb/nutsdb) (master branch with default options or custom options)

Benchmark System:

- Go Version : go1.11.4 darwin/amd64
- OS: Mac OS X 10.13.6
- Architecture: x86_64
- 16 GB 2133 MHz LPDDR3
- CPU: 3.1 GHz Intel Core i7

## Results

``` go
badger 2019/03/11 18:06:05 INFO: All 0 tables opened in 0s
goos: darwin
goarch: amd64
pkg: github.com/nutsdb/kvstore-bench
BenchmarkBadgerDBPutValue64B-8         10000        112382 ns/op        2374 B/op         74 allocs/op
BenchmarkBadgerDBPutValue128B-8        20000         94110 ns/op        2503 B/op         74 allocs/op
BenchmarkBadgerDBPutValue256B-8        20000         93480 ns/op        2759 B/op         74 allocs/op
BenchmarkBadgerDBPutValue512B-8        10000        101407 ns/op        3271 B/op         74 allocs/op
BenchmarkBadgerDBGet-8               1000000          1552 ns/op         416 B/op          9 allocs/op
BenchmarkBoltDBPutValue64B-8           10000        203128 ns/op       21231 B/op         62 allocs/op
BenchmarkBoltDBPutValue128B-8           5000        229568 ns/op       13716 B/op         64 allocs/op
BenchmarkBoltDBPutValue256B-8          10000        196513 ns/op       17974 B/op         64 allocs/op
BenchmarkBoltDBPutValue512B-8          10000        199805 ns/op       17064 B/op         64 allocs/op
BenchmarkBoltDBGet-8                 1000000          1122 ns/op         592 B/op         10 allocs/op
BenchmarkNutsDBPutValue64B-8           30000         53614 ns/op         626 B/op         14 allocs/op
BenchmarkNutsDBPutValue128B-8          30000         51998 ns/op         664 B/op         13 allocs/op
BenchmarkNutsDBPutValue256B-8          30000         53958 ns/op         920 B/op         13 allocs/op
BenchmarkNutsDBPutValue512B-8          30000         55787 ns/op        1432 B/op         13 allocs/op
BenchmarkNutsDBGet-8                 2000000           661 ns/op          88 B/op          3 allocs/op
BenchmarkNutsDBGetByHintKey-8          50000         27255 ns/op         840 B/op         16 allocs/op
PASS
ok      github.com/nutsdb/kvstore-bench   83.856s
```

## Conclusions

### Index mode

From the version v0.3.0, NutsDB supports two modes about entry index: `HintKeyValAndRAMIdxMode` and `HintKeyAndRAMIdxMode`. From the version v0.5.0, NutsDB supports `HintBPTSparseIdxMode` mode.

The default mode use `HintKeyValAndRAMIdxMode`, entries are indexed base on RAM, so its read/write performance is fast. but can’t handle databases much larger than the available physical RAM. If you set the `HintKeyAndRAMIdxMode` mode, HintIndex will not cache the value of the entry. Its write performance is also fast. To retrieve a key by seeking to offset relative to the start of the data file, so its read performance more slowly that RAM way, but it can save memory. The mode `HintBPTSparseIdxMode` is based b+ tree sparse index, this mode saves memory very much (1 billion data only uses about 80MB of memory). And other data structures such as ***list, set, sorted set only supported with mode HintKeyValAndRAMIdxMode***. ***It cannot switch back and forth between modes because the index structure is different***.

NutsDB will truncate data file if the active file is larger than `SegmentSize`, so the size of an entry can not be set larger than `SegmentSize` , default `SegmentSize` is 8MB, you can set it(opt.SegmentSize) as option before DB opening. ***Once set, it cannot be changed***.

### Support OS

NutsDB currently works on Mac OS, Linux and Windows.

### About merge operation

The HintBPTSparseIdxMode mode does not support the merge operation of the current version.

### About transactions

Recommend use the latest version.
