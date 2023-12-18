# 性能测试

为了保证尽可能公平，找了2款关注度很高的内嵌型的kvstore来做对比，他们都支持事务、支持持久化。

* [BadgerDB](https://github.com/dgraph-io/badger) (master分支和默认配置)
* [BoltDB](https://github.com/boltdb/bolt) (master分支和默认配置)
* [NutsDB](https://github.com/nutsdb/nutsdb) (master分支和默认配置+自定义配置)

## 测试的环境:

* Go Version : go1.11.4 darwin/amd64
* OS: Mac OS X 10.13.6
* Architecture: x86_64
* 16 GB 2133 MHz LPDDR3
* CPU: 3.1 GHz Intel Core i7


##  Benchmark的结果:

```
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

## 结论:

### 写性能:

NutsDB最快。 NutsDB比BoltDB快2-5倍 , 比BadgerDB快0.5-2倍。
BadgerDB次之，他比BoltDB快1-3倍。
BoltDB最慢。

### 读性能:

默认模式下，读都很快。其中NutsDB在默认配置下比其他数据库快一倍。但是如果使用`HintKeyAndRAMIdxMode`的选项，读速度比默认配置低很多。道理很简单，默认配置是全内存索引，但是`HintKeyAndRAMIdxMode`的模式，是内存索引+磁盘混合的方式，但是这个选项模式可以保存远大于内存的数据。特别是value远大于key的场景效果更明显。
