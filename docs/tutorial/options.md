# NutsDB 配置

## NutsDB 配置选项

* Dir                  string

`Dir` 代表数据库存放数据的目录

* EntryIdxMode         EntryIdxMode

`EntryIdxMode` 代表索引entry的模式.
`EntryIdxMode` 包括选项: `HintKeyValAndRAMIdxMode` 、 `HintKeyAndRAMIdxMode`和 `HintBPTSparseIdxMode`。

其中`HintKeyValAndRAMIdxMode` 代表纯内存索引模式（key和value都会被cache）。
`HintKeyAndRAMIdxMode` 代表内存+磁盘的索引模式（只有key被cache）。
`HintBPTSparseIdxMode`（v0.4.0之后的版本支持） 是专门节约内存的设计方案，单机10亿条数据，只要80几M内存。但是读性能不高，需要自己加缓存来加速。

* RWMode               RWMode

`RWMode` 代表读写模式. `RWMode` 包括两种选项: `FileIO` and `MMap`.
`FileIO` 用标准的 I/O读写。 `MMap` 代表使用mmap进行读写。

* SegmentSize          int64

`SegmentSize` 代表数据库的数据单元，每个数据单元（文件）为`SegmentSize`，现在默认是8。**注意：从大于0.8.0版本开始，默认SegmentSize变成256MB**
MB，这个可以自己配置。但是一旦被设置，下次启动数据库也要用这个配置，不然会报错。详情见 [限制和警告](https://github.com/nutsdb/nutsdb/blob/master/README-CN.md#%E8%AD%A6%E5%91%8A%E5%92%8C%E9%99%90%E5%88%B6)。

* NodeNum              int64

`NodeNum` 代表节点的号码.默认 NodeNum是 1. `NodeNum` 取值范围 [1,1023] 。

* SyncEnable           bool

`SyncEnable` 代表调用了 Sync() 方法.
如果 `SyncEnable` 为 false， 写性能会很高，但是如果遇到断电或者系统奔溃，会有数据丢失的风险。
如果  `SyncEnable` 为 true，写性能会相比false的情况慢很多，但是数据更有保障，每次事务提交成功都会落盘。

* StartFileLoadingMode RWMode

`StartFileLoadingMode` 代表启动数据库的载入文件的方式。参数选项同`RWMode`。

* GCWhenClose bool

`GCWhenClose` 表示调用 ```db.Close()``` 时主动 GC。Nutsdb 预设不会立即在 ```db.Close()``` 时触发 GC.

* CommitBufferSize int64

`CommitBufferSize` 表示为事务预分配的内存大小。Nutsdb 将预分配内存以减少内存分配的次数。

* ErrorHandler ErrorHandler

`ErrorHandler` 处理事务执行期间发生的错误。

* LessFunc LessFunc

`LessFunc` 表示对 key 进行排序的函数。Nutsdb 默认按字典序对 key 进行排序。

* MergeInterval time.Duration

`MergeInterval` 表示自动化 Merge 的间隔，0 表示不触发自动化 Merge，默认间隔为 2 小时。

- MaxBatchCount int64

`MaxBatchCount` 表示批量写入的最大条数。

- MaxBatchSize int64

`MaxBatchSize` 表示批量写入的最大字节数。

- ExpiredDeleteType ExpiredDeleteType

`ExpiredDeleteType ` 表示用于自动过期删除的数据结构。TimeWheel 意味着使用时间轮，你可以在需要高性能或者内存会充足的时候使用。TimeHeap 意味着使用时间轮，你可以在需要高精度删除或者内存将吃紧的时候使用。



## 默认选项

推荐使用默认选项的方式。兼顾了持久化+快速的启动数据库。当然具体还要看你场景的要求。

> 以下配置是比较保守的方式。
> 如果你对写性能要求比较高，可以设置SyncEnable等于false，RWMode改成MMap，写性能会得到极大提升，缺点是可能会丢数据（例如遇到断电或者系统奔溃）

```
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
	}
}()
```