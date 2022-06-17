<p align="center">
    <img src="https://user-images.githubusercontent.com/6065007/141310364-62d7eebb-2cbb-4949-80ed-5cd20f705405.png">
</p>

# NutsDB [![GoDoc](https://godoc.org/github.com/xujiajun/nutsdb?status.svg)](https://godoc.org/github.com/xujiajun/nutsdb)  [![Go Report Card](https://goreportcard.com/badge/github.com/xujiajun/nutsdb)](https://goreportcard.com/report/github.com/xujiajun/nutsdb) <a href="https://travis-ci.org/xujiajun/nutsdb"><img src="https://travis-ci.org/xujiajun/nutsdb.svg?branch=master" alt="Build Status"></a> [![Coverage Status](https://coveralls.io/repos/github/xujiajun/nutsdb/badge.svg?branch=master)](https://coveralls.io/github/xujiajun/nutsdb?branch=master) [![License](http://img.shields.io/badge/license-Apache_2-blue.svg?style=flat-square)](https://raw.githubusercontent.com/xujiajun/nutsdb/master/LICENSE) [![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go#database)  

[English](https://github.com/xujiajun/nutsdb/blob/master/README.md) | 简体中文

NutsDB是纯Go语言编写一个简单、高性能、内嵌型、持久化的key-value数据库。

NutsDB支持事务，从v0.2.0之后的版本开始支持ACID的特性，建议使用最新的release版本。v0.2.0之前的版本，保持高性能，没有作sync，但是具备高性能的写（本地测试，百万数据写入达40~50W+/s）。所有的操作都在事务中执行。NutsDB从v0.2.0版本开始支持多种数据结构，如列表(list)、集合(set)、有序集合(sorted set)。从0.4.0版本开始增加自定义配置读写方式、启动时候的文件载入方式、sync是否开启等，详情见[选项配置](https://github.com/xujiajun/nutsdb/blob/master/README-CN.md#%E9%80%89%E9%A1%B9%E9%85%8D%E7%BD%AE)


### 加群
欢迎对NutsDB感兴趣的加群、一起开发，具体看这个issue：https://github.com/nutsdb/nutsdb/issues/116

### 公告
v0.9.0 release，详情见： https://github.com/nutsdb/nutsdb/issues/167

📢 注意：从v0.9.0开始，**DefaultOptions**里面的**defaultSegmentSize**做了调整从原来的**8MB**变成了**256MB**，如果你原来设置256MB不用改，如果原来使用的是默认值的，需要手动改成8MB，不然原来的数据不会解析。这边的大小调整原因是从v0.9.0开始有对文件描述符的缓存（详解见PR https://github.com/nutsdb/nutsdb/issues/164 ），所以需要用户看下自己的fd数量，有不清楚可以提issue或者群里问。

### 学习资料

https://www.bilibili.com/video/BV1T34y1x7AS/

## 架构设计
![image](https://user-images.githubusercontent.com/6065007/163700148-3f176acf-c9c5-4248-a999-684acb92849a.png)




## 目录

- [入门指南](#入门指南)
  - [安装](#安装)
  - [开启数据库](#开启数据库)
  - [选项配置](#选项配置)
    - [默认选项](#默认选项)
  - [使用事务](#使用事务)
    - [读写事务](#读写事务)
    - [只读事务](#只读事务)
    - [手动管理事务](#手动管理事务)
  - [使用buckets](#使用buckets)
    - [迭代buckets](#迭代buckets)
    - [删除bucket](#删除bucket)
  - [使用键值对](#使用键值对)
  - [使用TTL](#使用ttl)
  - [对keys的扫描操作](#对keys的扫描操作)
    - [前缀扫描](#前缀扫描)
    - [前缀后的正则搜索扫描](#前缀后的正则扫描)    
    - [范围扫描](#范围扫描)
    - [获取全部的key和value](#获取全部的key和value)
  - [合并操作](#合并操作)
  - [数据库备份](#数据库备份)
- [使用内存模式](#使用内存模式)
- [使用其他数据结构](#使用其他数据结构)
   - [List](#list)
     - [RPush](#rpush)
     - [LPush](#lpush)
     - [LPop](#lpop)
     - [LPeek](#lpeek)
     - [RPop](#rpop)
     - [RPeek](#rpeek)
     - [LRange](#lrange)
     - [LRem](#lrem)
     - [LSet](#lset)    
     - [Ltrim](#ltrim)
     - [LSize](#lsize)      
   - [Set](#set)
     - [SAdd](#sadd)
     - [SAreMembers](#saremembers)
     - [SCard](#scard)
     - [SDiffByOneBucket](#sdiffbyonebucket)
     - [SDiffByTwoBuckets](#sdiffbytwobuckets)
     - [SHasKey](#shaskey)
     - [SIsMember](#sismember)
     - [SMembers](#smembers)
     - [SMoveByOneBucket](#smovebyonebucket)
     - [SMoveByTwoBuckets](#smovebytwobuckets)
     - [SPop](#spop)
     - [SRem](#srem)
     - [SUnionByOneBucket](#sunionbyonebucket)
     - [SUnionByTwoBucket](#sunionbytwobuckets)
   - [Sorted Set](#sorted-set)
     - [ZAdd](#zadd)
     - [ZCard](#zcard)
     - [ZCount](#zcount)
     - [ZGetByKey](#zgetbykey)
     - [ZMembers](#zmembers)
     - [ZPeekMax](#zpeekmax)
     - [ZPeekMin](#zpeekmin)
     - [ZPopMax](#zpopmax)
     - [ZPopMin](#zpopmin)
     - [ZRangeByRank](#zrangebyrank)
     - [ZRangeByScore](#zrangebyscore)
     - [ZRank](#zrank)
     - [ZRevRank](#zrevrank)
     - [ZRem](#zrem)
     - [ZRemRangeByRank](#zremrangebyrank)
     - [ZScore](#zscore)
- [与其他数据库的比较](#与其他数据库的比较)
   - [BoltDB](#boltdb)
   - [LevelDB, RocksDB](#leveldb-rocksdb)
   - [Badger](#badger)
- [Benchmarks](#benchmarks)
- [警告和限制](#警告和限制)
- [联系作者](#联系作者)
- [参与贡献](#参与贡献)
- [致谢](#致谢)
- [License](#license)

## 入门指南

### 安装

NutsDB的安装很简单，首先保证 [Golang](https://golang.org/dl/) 已经安装好 (版本要求1.11以上). 然后在终端执行命令:

```
go get -u github.com/xujiajun/nutsdb
```

### 开启数据库

要打开数据库需要使用` nutsdb.Open()`这个方法。其中用到的选项(options)包括 `Dir` , `EntryIdxMode`和 `SegmentSize`，在调用的时候这些参数必须设置。官方提供了`DefaultOptions`的选项，直接使用`nutsdb.DefaultOptions`即可。当然你也可以根据需要自己定义。

例子： 

```golang
package main

import (
    "log"

    "github.com/xujiajun/nutsdb"
)

func main() {
    opt := nutsdb.DefaultOptions
    opt.Dir = "/tmp/nutsdb" //这边数据库会自动创建这个目录文件
    db, err := nutsdb.Open(opt)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    ...
}
```

### 选项配置

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
MB，这个可以自己配置。但是一旦被设置，下次启动数据库也要用这个配置，不然会报错。详情见 [限制和警告](https://github.com/xujiajun/nutsdb/blob/master/README-CN.md#%E8%AD%A6%E5%91%8A%E5%92%8C%E9%99%90%E5%88%B6)。

* NodeNum              int64

`NodeNum` 代表节点的号码.默认 NodeNum是 1. `NodeNum` 取值范围 [1,1023] 。

* SyncEnable           bool

`SyncEnable` 代表调用了 Sync() 方法.
如果 `SyncEnable` 为 false， 写性能会很高，但是如果遇到断电或者系统奔溃，会有数据丢失的风险。
如果  `SyncEnable` 为 true，写性能会相比false的情况慢很多，但是数据更有保障，每次事务提交成功都会落盘。

* StartFileLoadingMode RWMode

`StartFileLoadingMode` 代表启动数据库的载入文件的方式。参数选项同`RWMode`。
    
    
#### 默认选项

推荐使用默认选项的方式。兼顾了持久化+快速的启动数据库。当然具体还要看你场景的要求。

> 以下配置是比较保守的方式。
> 如果你对写性能要求比较高，可以设置SyncEnable等于false，RWMode改成MMap，写性能会得到极大提升，缺点是可能会丢数据（例如遇到断电或者系统奔溃）

```
var DefaultOptions = Options{
    EntryIdxMode:         HintKeyValAndRAMIdxMode,
    SegmentSize:          defaultSegmentSize,
    NodeNum:              1,
    RWMode:               FileIO,
    SyncEnable:           true,
    StartFileLoadingMode: MMap,
}
```

### 使用事务

NutsDB为了保证隔离性，防止并发读写事务时候数据的不一致性，同一时间只能执行一个读写事务，但是允许同一时间执行多个只读事务。
从v0.3.0版本开始，NutsDB遵循标准的ACID原则。（参见[限制和警告](https://github.com/xujiajun/nutsdb/blob/master/README-CN.md#%E8%AD%A6%E5%91%8A%E5%92%8C%E9%99%90%E5%88%B6)）


#### 读写事务

```golang
err := db.Update(
    func(tx *nutsdb.Tx) error {
    ...
    return nil
})

```

#### 只读事务

```golang
err := db.View(
    func(tx *nutsdb.Tx) error {
    ...
    return nil
})

```

#### 手动管理事务

从上面的例子看到 `DB.View()` 和`DB.Update()` 这两个是数据库调用事务的主要方法。他们本质上是基于 `DB.Begin()`方法进行的包装。他们可以帮你自动管理事务的生命周期，从事务的开始、事务的执行、事务提交或者回滚一直到事务的安全的关闭为止，如果中间有错误会返回。所以**一般情况下推荐用这种方式去调用事务**。

这好比开车有手动挡和自动挡一样， `DB.View()` 和`DB.Update()`等于提供了自动档的效果。

如果你需要手动去开启、执行、关闭事务，你会用到`DB.Begin()`方法开启一个事务，`tx.Commit()` 方法用来提交事务、`tx.Rollback()`方法用来回滚事务

例子：

```golang
//开始事务
tx, err := db.Begin(true)
if err != nil {
    return err
}

bucket := "bucket1"
key := []byte("foo")
val := []byte("bar")

// 使用事务
if err = tx.Put(bucket, key, val, nutsdb.Persistent); err != nil {
    // 回滚事务
    tx.Rollback()
} else {
    // 提交事务
    if err = tx.Commit(); err != nil {
        tx.Rollback()
        return err
    }
}
```

### 使用buckets

buckets中文翻译过来是桶的意思，你可以理解成类似mysql的table表的概念，也可以理解成命名空间，或者多租户的概念。
所以你可以用他存不同的key的键值对，也可以存相同的key的键值对。所有的key在一个bucket里面不能重复。

例子：

```golang

key := []byte("key001")
val := []byte("val001")

bucket001 := "bucket001"
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        if err := tx.Put(bucket001, key, val, 0); err != nil {
            return err
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

bucket002 := "bucket002"
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        if err := tx.Put(bucket002, key, val, 0); err != nil {
            return err
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

```
这边注意下，这个bucket和你使用数据结构有关，不同数据索引结构，用同一个bucket，也是不同的。比如你定义了一个bucket，命名为`bucket_foo`，比如你要用`list`这个数据结构，使用 `tx.RPush`加数据，必须对应他的数据结构去从这个`bucket_foo`查询或者取出，比如用 `tx.RPop`，`tx.LRange` 等，不能用`tx.Get`（和GetAll、Put、Delete、RangeScan等同一索引类型）去读取这个`bucket_foo`里面的数据，因为索引结构不同。其他数据结构如`Set`、`Sorted Set`同理。

下面说明下迭代buckets 和 删除bucket。它们都用到了`ds`。

ds表示数据结构，支持如下：
* DataStructureSet
* DataStructureSortedSet
* DataStructureBPTree
* DataStructureList

目前支持的`EntryIdxMode`如下：

* HintKeyValAndRAMIdxMode 
* HintKeyAndRAMIdxMode 

#### 迭代buckets

IterateBuckets支持迭代指定ds的迭代。

```go

if err := db.View(
    func(tx *nutsdb.Tx) error {
        return tx.IterateBuckets(nutsdb.DataStructureBPTree, func(bucket string) {
            fmt.Println("bucket: ", bucket)
        })
    }); err != nil {
    log.Fatal(err)
}
```

#### 删除bucket

DeleteBucket支持删除指定的bucket，需要两个参数`ds`和`bucket`。

```go

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.DeleteBucket(nutsdb.DataStructureBPTree, bucket)
    }); err != nil {
    log.Fatal(err)
}
```


### 使用键值对

将key-value键值对保存在一个bucket, 你可以使用 `tx.Put` 这个方法:

* 添加数据

```golang

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

* 更新数据

上面的代码执行之后key为"name1"和value值"val1"被保存在命名为bucket1的bucket里面。
 
如果你要做更新操作，你可以仍然用`tx.Put`方法去执行，比如下面的例子把value的值改成"val1-modify"：

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
    key := []byte("name1")
    val := []byte("val1-modify") // 更新值
    bucket := "bucket1"
    if err := tx.Put(bucket, key, val, 0); err != nil {
        return err
    }
    return nil
}); err != nil {
    log.Fatal(err)
}

```

* 获取数据

获取值可以用`tx.Get` 这个方法:

```golang
if err := db.View(
func(tx *nutsdb.Tx) error {
    key := []byte("name1")
    bucket := "bucket1"
    if e, err := tx.Get(bucket, key); err != nil {
        return err
    } else {
        fmt.Println(string(e.Value)) // "val1-modify"
    }
    return nil
}); err != nil {
    log.Println(err)
}
```

* 删除数据

删除使用`tx.Delete()` 方法：

```golang
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

### 使用TTL

NusDB支持TTL(存活时间)的功能，可以对指定的bucket里的key过期时间的设置。使用`tx.Put`这个方法的使用`ttl`参数就可以了。
如果设置 ttl = 0 或者 Persistent, 这个key就会永久存在。下面例子中ttl设置成 60 , 60s之后key就会过期，在查询的时候将不会被搜到。

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
    key := []byte("name1")
    val := []byte("val1")
    bucket := "bucket1"
    
    // 如果设置 ttl = 0 or Persistent, 这个key就会永久不删除
    // 这边 ttl = 60 , 60s之后就会过期。
    if err := tx.Put(bucket, key, val, 60); err != nil {
        return err
    }
    return nil
}); err != nil {
    log.Fatal(err)
}
```
### 对keys的扫描操作

key在一个bucket里面按照byte-sorted有序排序的，所以对于keys的扫描操作，在NutsDB里是很高效的。
 

#### 前缀扫描

对于前缀的扫描，我们可以用`PrefixScan` 方法, 使用参数 `offSet`和`limitNum` 来限制返回的结果的数量，比方下面例子限制100个entries:

```golang

if err := db.View(
    func(tx *nutsdb.Tx) error {
        prefix := []byte("user_")
        bucket := "user_list"
        // 从offset=0开始 ，限制 100 entries 返回 
        if entries, err := tx.PrefixScan(bucket, prefix, 0, 100); err != nil {
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

#### 前缀后的正则扫描

对于前缀后的扫描，可以通过正则表达式对键的第二部分进行搜索来遍历一个键前缀，我们可以使用`PrefixSearchScan`方法，用参数`reg`来编写正则表达式，使用参数`offsetNum`、`limitNum` 来约束返回的条目的数量:

```go

if err := db.View(
    func(tx *nutsdb.Tx) error {
        prefix := []byte("user_") // 定义前缀
        reg := "99"  // 定义正则表达式
        bucket := "user_list"
        // 从offset=25开始，限制 100 entries 返回 
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

#### 范围扫描

对于范围的扫描，我们可以用 `RangeScan` 方法。

例子：

```golang
if err := db.View(
    func(tx *nutsdb.Tx) error {
        // 假设用户key从 user_0000000 to user_9999999.
        // 执行区间扫描类似这样一个start和end作为主要参数.
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
### 获取全部的key和value

对于获取一个bucket的所有key和value，可以使用`GetAll`方法。

例子：

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
### 合并操作

nutsdb为了保持高性能写，同一个key会写多份，如果你的服务，有对同一个key多次的更新或者删除，你希望对同一个key做合并，可以使用NutsDB提供了`db.Merge()`方法。
这个方法需要自己根据实际情况编写合并策略。一旦执行会影响到正常的写请求，所以最好避开高峰期，比如半夜定时执行等。

当然，如果你没有对同一个key有太多的更新或者删除，可以不用Merge()函数。

```golang
err := db.Merge()
if err != nil {
    ...
}
```

注意：当前版本不支持`HintBPTSparseIdxMode`模式的合并操作

### 数据库备份

对于数据库的备份，你可以调用 `db.Backup()`方法，只要提供一个备份的文件目录地址即可。这个方法执行的是一个热备份，不会阻塞到数据库其他的只读事务操作，对写（读写）事务会有影响。

```golang
err = db.Backup(dir)
if err != nil {
   ...
}
```

NutsDB还提供gzip的压缩备份：

```golang
f, _ := os.Create(path)
defer f.Close()
err = db.BackupTarGZ(f)
if err != nil {
   ...
}

```

好了，入门指南已经完结。 散花~，到目前为止都是String类型的数据的crud操作，下面将学习其他更多的数据结构的操作。

### 使用内存模式

NutsDB从0.7.0版本开始支持内存模式，这个模式下，重启数据库，数据会丢失的。

例子：

```go

    opts := inmemory.DefaultOptions
    db, err := inmemory.Open(opts)
    if err != nil {
        panic(err)
    }
    bucket := "bucket1"
    key := []byte("key1")
    val := []byte("val1")
    err = db.Put(bucket, key, val, 0)
    if err != nil {
        fmt.Println("err", err)
    }

    entry, err := db.Get(bucket, key)
    if err != nil {
        fmt.Println("err", err)
    }

    fmt.Println("entry.Key", string(entry.Key))     // entry.Key key1
    fmt.Println("entry.Value", string(entry.Value)) // entry.Value val1
    
```

### 使用其他数据结构

看到这边我们将学习其他数据结构，Api命名风格模仿 [Redis 命令](https://redis.io/commands)。所以如果你熟悉Redis，将会很快掌握使用。
其他方面继承了上面的bucket/key/value模型，所以你会看到和Redis的Api使用上稍微有些不同，会多一个bucket。

#### List

##### RPush

从指定bucket里面的指定队列key的右边入队一个或者多个元素val。

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "bucketForList"
        key := []byte("myList")
        val := []byte("val1")
        return tx.RPush(bucket, key, val)
    }); err != nil {
    log.Fatal(err)
}
```

##### LPush 

从指定bucket里面的指定队列key的左边入队一个或者多个元素val。

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        val := []byte("val2")
        return tx.LPush(bucket, key, val)
    }); err != nil {
    log.Fatal(err)
}
```

##### LPop 

从指定bucket里面的指定队列key的左边出队一个元素，删除并返回。

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        if item, err := tx.LPop(bucket, key); err != nil {
            return err
        } else {
            fmt.Println("LPop item:", string(item))
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### LPeek

从指定bucket里面的指定队列key的左边出队一个元素返回不删除。

```golang
if err := db.View(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        if item, err := tx.LPeek(bucket, key); err != nil {
            return err
        } else {
            fmt.Println("LPeek item:", string(item)) //val11
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### RPop 

从指定bucket里面的指定队列key的右边出队一个元素，删除并返回。

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        if item, err := tx.RPop(bucket, key); err != nil {
            return err
        } else {
            fmt.Println("RPop item:", string(item))
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### RPeek

从指定bucket里面的指定队列key的右边出队一个元素返回不删除。

```golang
if err := db.View(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        if item, err := tx.RPeek(bucket, key); err != nil {
            return err
        } else {
            fmt.Println("RPeek item:", string(item))
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### LRange 

返回指定bucket里面的指定队列key列表里指定范围内的元素。 start 和 end 偏移量都是基于0的下标，即list的第一个元素下标是0（list的表头），第二个元素下标是1，以此类推。
偏移量也可以是负数，表示偏移量是从list尾部开始计数。 例如：-1 表示列表的最后一个元素，-2 是倒数第二个，以此类推。

```golang
if err := db.View(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        if items, err := tx.LRange(bucket, key, 0, -1); err != nil {
            return err
        } else {
            //fmt.Println(items)
            for _, item := range items {
                fmt.Println(string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### LRem 

注意: 这个方法在 v0.6.0版本开始支持，之前的版本实现和描述有问题。

从指定bucket里面的指定的key的列表里移除前 count 次出现的值为 value 的元素。 这个 count 参数通过下面几种方式影响这个操作：

count > 0: 从头往尾移除值为 value 的元素。
count < 0: 从尾往头移除值为 value 的元素。
count = 0: 移除所有值为 value 的元素。

下面的例子count=1：

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        return tx.LRem(bucket, key, 1, []byte("val11"))
    }); err != nil {
    log.Fatal(err)
}
```

##### LSet 

设置指定bucket的指定list的index位置的的值为value。

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        if err := tx.LSet(bucket, key, 0, []byte("val11")); err != nil {
            return err
        } else {
            fmt.Println("LSet ok, index 0 item value => val11")
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### Ltrim 

修剪一个已存在的 list，这样 list 就会只包含指定范围的指定元素。start 和 stop 都是由0开始计数的， 这里的 0 是列表里的第一个元素（表头），1 是第二个元素，以此类推。

例如： LTRIM foobar 0 2 将会对存储在 foobar 的列表进行修剪，只保留列表里的前3个元素。

start 和 end 也可以用负数来表示与表尾的偏移量，比如 -1 表示列表里的最后一个元素， -2 表示倒数第二个，等等。

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        return tx.LTrim(bucket, key, 0, 1)
    }); err != nil {
    log.Fatal(err)
}
```

##### LSize 

返回指定bucket下指定key列表的size大小

```golang
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        if size,err := tx.LSize(bucket, key); err != nil {
            return err
        } else {
            fmt.Println("myList size is ",size)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

#### Set

##### SAdd

添加一个指定的member元素到指定bucket的里的指定集合key中。

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForSet"
        key := []byte("mySet")
        return tx.SAdd(bucket, key, []byte("a"), []byte("b"), []byte("c"))
    }); err != nil {
    log.Fatal(err)
}
```

##### SAreMembers 

返回多个成员member是否是指定bucket的里的指定集合key的成员。

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "bucketForSet"
        key := []byte("mySet")
        if ok, err := tx.SAreMembers(bucket, key, []byte("a"), []byte("b"), []byte("c")); err != nil {
            return err
        } else {
            fmt.Println("SAreMembers:", ok)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### SCard 

返回指定bucket的指定的集合key的基数 (集合元素的数量)。

```go

if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "bucketForSet"
        key := []byte("mySet")
        if num, err := tx.SCard(bucket, key); err != nil {
            return err
        } else {
            fmt.Println("SCard:", num)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

```
##### SDiffByOneBucket 

返回一个集合与给定集合的差集的元素。这两个集合都在一个bucket中。

```go

key1 := []byte("mySet1") // 集合1
key2 := []byte("mySet2") // 集合2
bucket := "bucketForSet"

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket, key1, []byte("a"), []byte("b"), []byte("c"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket, key2, []byte("c"), []byte("d"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SDiffByOneBucket(bucket, key1, key2); err != nil {
            return err
        } else {
            fmt.Println("SDiffByOneBucket:", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
            //item a
            //item b
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

```

##### SDiffByTwoBuckets 

返回一个集合与给定集合的差集的元素。这两个集合分别在不同bucket中。

```go
bucket1 := "bucket1"
key1 := []byte("mySet1")

bucket2 := "bucket2"
key2 := []byte("mySet2")

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket1, key1, []byte("a"), []byte("b"), []byte("c"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket2, key2, []byte("c"), []byte("d"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SDiffByTwoBuckets(bucket1, key1, bucket2, key2); err != nil {
            return err
        } else {
            fmt.Println("SDiffByTwoBuckets:", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

```
##### SHasKey 

判断是否指定的集合在指定的bucket中。

```go

bucket := "bucketForSet"

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if ok, err := tx.SHasKey(bucket, []byte("mySet")); err != nil {
            return err
        } else {
            fmt.Println("SHasKey", ok)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

```
##### SIsMember 

返回成员member是否是指定bucket的存指定key集合的成员。

```go
bucket := "bucketForSet"

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if ok, err := tx.SIsMember(bucket, []byte("mySet"), []byte("a")); err != nil {
            return err
        } else {
            fmt.Println("SIsMember", ok)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### SMembers 

返回指定bucket的指定key集合所有的元素。

```go
bucket := "bucketForSet"

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SMembers(bucket, []byte("mySet")); err != nil {
            return err
        } else {
            fmt.Println("SMembers", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### SMoveByOneBucket 

将member从source集合移动到destination集合中，其中source集合和destination集合均在一个bucket中。

```go
bucket3 := "bucket3"

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket3, []byte("mySet1"), []byte("a"), []byte("b"), []byte("c"))
    }); err != nil {
    log.Fatal(err)
}
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket3, []byte("mySet2"), []byte("c"), []byte("d"), []byte("e"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        if ok, err := tx.SMoveByOneBucket(bucket3, []byte("mySet1"), []byte("mySet2"), []byte("a")); err != nil {
            return err
        } else {
            fmt.Println("SMoveByOneBucket", ok)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SMembers(bucket3, []byte("mySet1")); err != nil {
            return err
        } else {
            fmt.Println("after SMoveByOneBucket bucket3 mySet1 SMembers", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SMembers(bucket3, []byte("mySet2")); err != nil {
            return err
        } else {
            fmt.Println("after SMoveByOneBucket bucket3 mySet2 SMembers", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### SMoveByTwoBuckets 

将member从source集合移动到destination集合中。其中source集合和destination集合在两个不同的bucket中。

```go
bucket4 := "bucket4"
bucket5 := "bucket5"
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket4, []byte("mySet1"), []byte("a"), []byte("b"), []byte("c"))
    }); err != nil {
    log.Fatal(err)
}
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket5, []byte("mySet2"), []byte("c"), []byte("d"), []byte("e"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        if ok, err := tx.SMoveByTwoBuckets(bucket4, []byte("mySet1"), bucket5, []byte("mySet2"), []byte("a")); err != nil {
            return err
        } else {
            fmt.Println("SMoveByTwoBuckets", ok)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SMembers(bucket4, []byte("mySet1")); err != nil {
            return err
        } else {
            fmt.Println("after SMoveByTwoBuckets bucket4 mySet1 SMembers", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SMembers(bucket5, []byte("mySet2")); err != nil {
            return err
        } else {
            fmt.Println("after SMoveByTwoBuckets bucket5 mySet2 SMembers", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### SPop 

从指定bucket里的指定key的集合中移除并返回一个或多个随机元素。

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        key := []byte("mySet")
        if item, err := tx.SPop(bucket, key); err != nil {
            return err
        } else {
            fmt.Println("SPop item from mySet:", string(item))
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### SRem 

在指定bucket里面移除指定的key集合中移除指定的一个或者多个元素。

```go
bucket6:="bucket6"
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket6, []byte("mySet"), []byte("a"), []byte("b"), []byte("c"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        if err := tx.SRem(bucket6, []byte("mySet"), []byte("a")); err != nil {
            return err
        } else {
            fmt.Println("SRem ok")
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SMembers(bucket6, []byte("mySet")); err != nil {
            return err
        } else {
            fmt.Println("SMembers items:", items)
            for _, item := range items {
                fmt.Println("item:", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### SUnionByOneBucket 

返回指定一个bucket里面的给定的两个集合的并集中的所有成员。

```go
bucket7 := "bucket1"
key1 := []byte("mySet1")
key2 := []byte("mySet2")

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket7, key1, []byte("a"), []byte("b"), []byte("c"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket7, key2, []byte("c"), []byte("d"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SUnionByOneBucket(bucket7, key1, key2); err != nil {
            return err
        } else {
            fmt.Println("SUnionByOneBucket:", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### SUnionByTwoBuckets 

返回指定两个bucket里面的给定的两个集合的并集中的所有成员。

```go
bucket8 := "bucket1"
key1 := []byte("mySet1")

bucket9 := "bucket2"
key2 := []byte("mySet2")

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket8, key1, []byte("a"), []byte("b"), []byte("c"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        return tx.SAdd(bucket9, key2, []byte("c"), []byte("d"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        if items, err := tx.SUnionByTwoBuckets(bucket8, key1, bucket9, key2); err != nil {
            return err
        } else {
            fmt.Println("SUnionByTwoBucket:", items)
            for _, item := range items {
                fmt.Println("item", string(item))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

#### Sorted Set

> 注意：这边的bucket是有序集合名。

##### ZAdd

将指定成员（包括key、score、value）添加到指定bucket的有序集合（sorted set）里面。


```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1" // 注意：这边的bucket是有序集合名
        key := []byte("key1")
        return tx.ZAdd(bucket, key, 1, []byte("val1"))
    }); err != nil {
    log.Fatal(err)
}
```
##### ZCard 

返回指定bucket的的有序集元素个数。

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        if num, err := tx.ZCard(bucket); err != nil {
            return err
        } else {
            fmt.Println("ZCard num", num)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### ZCount 

返回指定bucket的有序集，score值在min和max之间(默认包括score值等于start或end)的成员。

Opts包含的参数：

* Limit        int  // 限制返回的node数目
* ExcludeStart bool // 排除start
* ExcludeEnd   bool // 排除end

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        if num, err := tx.ZCount(bucket, 0, 1, nil); err != nil {
            return err
        } else {
            fmt.Println("ZCount num", num)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### ZGetByKey 

返回一个节点通过指定的bucket有序集合和指定的key来获取。

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key2")
        if node, err := tx.ZGetByKey(bucket, key); err != nil {
            return err
        } else {
            fmt.Println("ZGetByKey key2 val:", string(node.Value))
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### ZMembers 

返回所有成员通过在指定的bucket。

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        if nodes, err := tx.ZMembers(bucket); err != nil {
            return err
        } else {
            fmt.Println("ZMembers:", nodes)

            for _, node := range nodes {
                fmt.Println("member:", node.Key(), string(node.Value))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### ZPeekMax 

返回指定bucket有序集合中的具有最高得分的成员。

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        if node, err := tx.ZPeekMax(bucket); err != nil {
            return err
        } else {
            fmt.Println("ZPeekMax:", string(node.Value))
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### ZPeekMin 

返回指定bucket有序集合中的具有最低得分的成员。

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        if node, err := tx.ZPeekMin(bucket); err != nil {
            return err
        } else {
            fmt.Println("ZPeekMin:", string(node.Value))
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### ZPopMax 

删除并返回指定bucket有序集合中的具有最高得分的成员。

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        if node, err := tx.ZPopMax(bucket); err != nil {
            return err
        } else {
            fmt.Println("ZPopMax:", string(node.Value)) //val3
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### ZPopMin 

删除并返回指定bucket有序集合中的具有最低得分的成员。

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        if node, err := tx.ZPopMin(bucket); err != nil {
            return err
        } else {
            fmt.Println("ZPopMin:", string(node.Value)) //val1
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### ZRangeByRank 

返回指定bucket有序集合的排名start到end的范围（包括start和end）的所有元素。

```go
// ZAdd add items
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet2"
        key1 := []byte("key1")
        return tx.ZAdd(bucket, key1, 1, []byte("val1"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet2"
        key2 := []byte("key2")
        return tx.ZAdd(bucket, key2, 2, []byte("val2"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet2"
        key3 := []byte("key3")
        return tx.ZAdd(bucket, key3, 3, []byte("val3"))
    }); err != nil {
    log.Fatal(err)
}

// ZRangeByRank
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet2"
        if nodes, err := tx.ZRangeByRank(bucket, 1, 2); err != nil {
            return err
        } else {
            fmt.Println("ZRangeByRank nodes :", nodes)
            for _, node := range nodes {
                fmt.Println("item:", node.Key(), node.Score())
            }
            
            //item: key1 1
            //item: key2 2
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### ZRangeByScore 

返回指定bucket有序集合的分数start到end的范围（包括start和end）的所有元素。其中有个`Opts`参数用法参考`ZCount`。

```go
// ZAdd
if err := db.Update(
        func(tx *nutsdb.Tx) error {
            bucket := "myZSet3"
            key1 := []byte("key1")
            return tx.ZAdd(bucket, key1, 70, []byte("val1"))
        }); err != nil {
        log.Fatal(err)
    }

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet3"
        key2 := []byte("key2")
        return tx.ZAdd(bucket, key2, 90, []byte("val2"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet3"
        key3 := []byte("key3")
        return tx.ZAdd(bucket, key3, 86, []byte("val3"))
    }); err != nil {
    log.Fatal(err)
}

// ZRangeByScore
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet3"
        if nodes, err := tx.ZRangeByScore(bucket, 80, 100,nil); err != nil {
            return err
        } else {
            fmt.Println("ZRangeByScore nodes :", nodes)
            for _, node := range nodes {
                fmt.Println("item:", node.Key(), node.Score())
            }
            //item: key3 86
            //item: key2 90
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}   
```
##### ZRank

返回有序集bucket中成员指定成员key的排名。其中有序集成员按score值递增(从小到大)顺序排列。注意排名以1为底，也就是说，score值最小的成员排名为1。
这点和Redis不同，Redis是从0开始的。

```go

// ZAdd
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet4"
        key1 := []byte("key1")
        return tx.ZAdd(bucket, key1, 70, []byte("val1"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet4"
        key2 := []byte("key2")
        return tx.ZAdd(bucket, key2, 90, []byte("val2"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet4"
        key3 := []byte("key3")
        return tx.ZAdd(bucket, key3, 86, []byte("val3"))
    }); err != nil {
    log.Fatal(err)
}

// ZRank
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet4"
        key1 := []byte("key1")
        if rank, err := tx.ZRank(bucket, key1); err != nil {
            return err
        } else {
            fmt.Println("key1 ZRank :", rank) // key1 ZRank : 1
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

#### ZRevRank

返回有序集bucket中成员指定成员key的反向排名。其中有序集成员还是按score值递增(从小到大)顺序排列。但是获取反向排名，注意排名还是以1为开始，也就是说，但是这个时候score值最大的成员排名为1。

```go
// ZAdd
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet8"
        key1 := []byte("key1")
        return tx.ZAdd(bucket, key1, 10, []byte("val1"))
    }); err != nil {
    log.Fatal(err)
}
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet8"
        key2 := []byte("key2")
        return tx.ZAdd(bucket, key2, 20, []byte("val2"))
    }); err != nil {
    log.Fatal(err)
}
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet8"
        key3 := []byte("key3")
        return tx.ZAdd(bucket, key3, 30, []byte("val3"))
    }); err != nil {
    log.Fatal(err)
}

// ZRevRank
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet8"
        if rank, err := tx.ZRevRank(bucket, []byte("key3")); err != nil {
            return err
        } else {
            fmt.Println("ZRevRank key1 rank:", rank) //ZRevRank key3 rank: 1
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

##### ZRem 

删除指定成员key在一个指定的有序集合bucket中。

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet5"
        key1 := []byte("key1")
        return tx.ZAdd(bucket, key1, 10, []byte("val1"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet5"
        key2 := []byte("key2")
        return tx.ZAdd(bucket, key2, 20, []byte("val2"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet5"
        if nodes,err := tx.ZMembers(bucket); err != nil {
            return err
        } else {
            fmt.Println("before ZRem key1, ZMembers nodes",nodes)
            for _,node:=range nodes {
                fmt.Println("item:",node.Key(),node.Score())
            }
        }
        // before ZRem key1, ZMembers nodes map[key1:0xc00008cfa0 key2:0xc00008d090]
        // item: key1 10
        // item: key2 20
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet5"
        if err := tx.ZRem(bucket, "key1"); err != nil {
            return err
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet5"
        if nodes,err := tx.ZMembers(bucket); err != nil {
            return err
        } else {
            fmt.Println("after ZRem key1, ZMembers nodes",nodes)
            for _,node:=range nodes {
                fmt.Println("item:",node.Key(),node.Score())
            }
            // after ZRem key1, ZMembers nodes map[key2:0xc00008d090]
            // item: key2 20
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

```

##### ZRemRangeByRank 

删除所有成员满足排名start到end（包括start和end）在一个指定的有序集合bucket中。其中排名以1开始，排名1表示第一个节点元素，排名-1表示最后的节点元素。

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet6"
        key1 := []byte("key1")
        return tx.ZAdd(bucket, key1, 10, []byte("val1"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet6"
        key2 := []byte("key2")
        return tx.ZAdd(bucket, key2, 20, []byte("val2"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet6"
        key3 := []byte("key3")
        return tx.ZAdd(bucket, key3, 30, []byte("val2"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet6"
        if nodes,err := tx.ZMembers(bucket); err != nil {
            return err
        } else {
            fmt.Println("before ZRemRangeByRank, ZMembers nodes",nodes)
            for _,node:=range nodes {
                fmt.Println("item:",node.Key(),node.Score())
            }
            // before ZRemRangeByRank, ZMembers nodes map[key3:0xc00008d450 key1:0xc00008d270 key2:0xc00008d360]
            // item: key1 10
            // item: key2 20
            // item: key3 30
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet6"
        if err := tx.ZRemRangeByRank(bucket, 1,2); err != nil {
            return err
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet6"
        if nodes,err := tx.ZMembers(bucket); err != nil {
            return err
        } else {
            fmt.Println("after ZRemRangeByRank, ZMembers nodes",nodes)
            for _,node:=range nodes {
                fmt.Println("item:",node.Key(),node.Score())
            }
            // after ZRemRangeByRank, ZMembers nodes map[key3:0xc00008d450]
            // item: key3 30
            // key1 ZScore 10
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
##### ZScore

返回指定有序集bucket中，成员key的score值。

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet7"
        if score,err := tx.ZScore(bucket, []byte("key1")); err != nil {
            return err
        } else {
            fmt.Println("ZScore key1 score:",score)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
### 与其他数据库的比较

#### BoltDB

BoltDB和NutsDB很相似都是内嵌型的key-value数据库，同时支持事务。Bolt基于B+tree引擎模型，只有一个文件，NutsDB基于bitcask引擎模型，会生成多个文件。当然他们都支持范围扫描和前缀扫描这两个实用的特性。

#### LevelDB, RocksDB

LevelDB 和 RocksDB 都是基于LSM tree模型。不支持bucket。 其中RocksDB目前还没看到golang实现的版本。

#### Badger

Badger也是基于LSM tree模型。但是写性能没有我想象中高。不支持bucket。

另外，以上数据库均不支持多种数据结构如list、set、sorted set，而NutsDB从0.2.0版本开始支持这些数据结构。

### Benchmarks

为了保证尽可能公平，找了2款关注度很高的内嵌型的kvstore来做对比，他们都支持事务、支持持久化。

* [BadgerDB](https://github.com/dgraph-io/badger) (master分支和默认配置)
* [BoltDB](https://github.com/boltdb/bolt) (master分支和默认配置)
* [NutsDB](https://github.com/xujiajun/nutsdb) (master分支和默认配置+自定义配置)

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
pkg: github.com/xujiajun/kvstore-bench
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
ok      github.com/xujiajun/kvstore-bench   83.856s
```

## 结论:

### 写性能: 

NutsDB最快。 NutsDB比BoltDB快2-5倍 , 比BadgerDB快0.5-2倍。
BadgerDB次之，他比BoltDB快1-3倍。
BoltDB最慢。

### 读性能: 

默认模式下，读都很快。其中NutsDB在默认配置下比其他数据库快一倍。但是如果使用`HintKeyAndRAMIdxMode`的选项，读速度比默认配置低很多。道理很简单，默认配置是全内存索引，但是`HintKeyAndRAMIdxMode`的模式，是内存索引+磁盘混合的方式，但是这个选项模式可以保存远大于内存的数据。特别是value远大于key的场景效果更明显。
 

### 警告和限制

* 启动索引模式

当前版本使用`HintKeyValAndRAMIdxMode`、 `HintKeyAndRAMIdxMode`和`HintBPTSparseIdxMode` 这三种作为db启动的时候索引模式。
默认使用`HintKeyValAndRAMIdxMode`。在基本的功能的string数据类型（put、get、delete、rangeScan、PrefixScan）这三种模式都支持。`HintKeyValAndRAMIdxMode`，作为数据库默认选项，他是全内存索引，读写性能都很高。他的瓶颈在于内存。如果你内存够的话，这种默认是适合的。另一种模式`HintKeyAndRAMIdxMode`，他会把value存磁盘，通过索引去找offset，这种模式特别适合value远大于key的场景，他的读性能要比起默认模式要降低不少。`HintBPTSparseIdxMode`这个模式（v0.4.0之后支持）这个模式非常省内存，使用多级索引，测试10亿数据，只占用80几MB的内存，但是读性能比较差，需要自己加缓存加速。具体看自己的要求选择模式。

关于**其他的数据结构（list\set\sorted set）只支持默认的HintKeyValAndRAMIdxMode。请根据需要选模式**。**还有一个启动索引模式一旦开启不要来回切换到其他模式，因为索引结构不一样，可能导致数据读不出来**。 

* Segment配置问题

NutsDB会自动切割分成一个个块（Segment），默认`SegmentSize`是8MB，这个参数可以自己需要配置（比如16MB、32MB、64MB、128MB、512MB等），但是**一旦配置不能修改**。

* key和value的大小限制问题

关于key和value的大小受到SegmentSize的大小的影响，比如SegmentSize为8M，key和value的大小肯定是小于8M的，不然会返回错误。
在NutsDB里面entry是最小单位，只要保证entry不大于`SegmentSize`就可以了。

* entry的大小问题

entry的的大小=EntryHeader的大小+key的大小+value的大小+bucket的大小

* 关于支持的操作系统

支持 Mac OS 、Linux 、Windows 三大平台。

* 关于合并操作

`HintBPTSparseIdxMode` 这个模式在当前版本还没有支持。

* 关于事务说明

在传统的关系式数据库中，常常用 ACID 性质来检验事务功能的安全性，~~NutsDB目前的版本并没有完全支持ACID。~~ NutsDB从v0.2.0之后的版本开始完全支持ACID。

这这特别感谢 @damnever 给我提的[issue](https://github.com/xujiajun/nutsdb/issues/10)给我指出，特别在这说明下，免得误导大家。

从v0.3.0版本起，NutsDB支持（A）原子性、C（一致性）、I（隔离性），并保证（D）持久化。以下参考[wiki百科](https://zh.wikipedia.org/wiki/ACID)的对ACID定义分别讲一下。如讲的有误，欢迎帮我指正。

1、（A）原子性

所谓原子性，一个事务（transaction）中的所有操作，或者全部完成，或者全部不完成，不会结束在中间某个环节。实现事务的原子性，要支持回滚操作，在某个操作失败后，回滚到事务执行之前的状态。一般的做法是类似数据快照的方案。关于这一点，NutsDB支持回滚操作。NutsDB的作法是先实际预演一边所有要执行的操作，这个时候数据其实还是uncommitted状态，一直到所有环节都没有问题，才会作commit操作，如果中间任何环节一旦发生错误，直接作rollback回滚操作，保证原子性。 就算发生错误的时候已经有数据进磁盘，下次启动也不会被索引到这些数据。

2、（C）一致性

在事务开始之前和事务结束以后，数据库的完整性没有被破坏。这表示写入的数据必须完全符合预期的。NutsDB基于读写锁实现锁机制，在高并发场景下，一个读写事务具有排他性的，比如一个goroutine需要执行一个读写事务，其他不管想要读写的事务或者只读的只能等待，直到这个锁释放为止。保证了数据的一致性。所以这一点NutsDB满足一致性。

3、（I）隔离性

数据库允许多个并发事务同时对其数据进行读写和修改的能力，隔离性可以防止多个事务并发执行时由于交叉执行而导致数据的不一致。如上面的一致性所说，NutsDB基于读写锁实现锁机制。不会出现数据串的情况。所以也是满足隔离性的。

关于事务的隔离级别，我们也来对照[wiki百科](https://zh.wikipedia.org/wiki/%E4%BA%8B%E5%8B%99%E9%9A%94%E9%9B%A2)，来看下NutsDB属于哪一个级别：

#### 隔离级别低到高：

##### 1）未提交读（READ UNCOMMITTED）

这个是最低的隔离级别。允许“脏读”（dirty reads），事务可以看到其他事务“尚未提交”的修改。很明显nutsDB是避免脏读的。

##### 2）在提交读（READ COMMITTED）

定义：这个隔离级别中，基于锁机制并发控制的DBMS需要对选定对象的写锁一直保持到事务结束，但是读锁在SELECT操作完成后马上释放（因此“不可重复读”现象可能会发生）。
看下“不可重复读”的定义：在一次事务中，当一行数据获取两遍得到不同的结果表示发生了“不可重复读”。

nutsDB不会出现“不可重复读”这种情况，当高并发的时候，正在进行读写操作，一个goroutine刚好先拿到只读锁，这个时候要完成一个读写事务操作的那个goroutine要阻塞等到只读锁释放为止。也就避免上面的问题。

##### 3）在可重复读（REPEATABLE READS）

定义：这个隔离级别中，基于锁机制并发控制的DBMS需要对选定对象的读锁（read locks）和写锁（write locks）一直保持到事务结束，但不要求“范围锁”，因此可能会发生“幻影读”。

关于幻影读定义，指在事务执行过程中，当两个完全相同的查询语句执行得到不同的结果集。这种现象称为“幻影读（phantom read）”，有些人也叫他幻读，正如上面所说，在nutsDB中，当进行只读操作的时候，同一时间只能并发只读操作，其他有关“写”的事务是被阻塞的，直到这些只读锁释放为止，因此不会出现“幻影读”的情况。
 
##### 4）可串行化 （Serializable）

定义：这个隔离级别是最高的。避免了所有上面的“脏读”、不可重复读”、“幻影读”现象。

在nutsDB中，一个只读事务和一个写（读写）事务，是互斥的，需要串行执行，不会出现并发执行。nutsDB属于这个可串行化级别。
这个级别的隔离一般来说在高并发场景下性能会受到影响。但是如果锁本身性能还可以，也不失为一个简单有效的方法。当前版本nutsDB基于读写锁，在并发读多写少的场景下，性能会好一点。
 

4、（D）持久化

事务处理结束后，对数据的修改就是永久的，即便系统故障也不会丢失。v0.3.0之前版本的nutsdb为了提供高性能的写入，并没有实时的做sync操作。从v0.3.0开始使用sync操作作强制同步，开始支持持久化，建议使用最新版本。


关与其他信息待补充。有错误请帮忙指出，提给我issue，谢谢。

### 联系作者

* [xujiajun](https://github.com/xujiajun)

### 参与贡献

:+1::tada: 首先感谢你能看到这里，参与贡献 :tada::+1:

参与贡献方式不限于：

* 提各种issues（包括询问问题、提功能建议、性能建议等）
* 提交bug
* 提pull requests
* 优化修改README文档

#### 代码风格指南参考

https://github.com/golang/go/wiki/CodeReviewComments

#### git commit 规范参考
 
 commit message格式
 
 ```
 <type>(<scope>): <subject>
 ```

 
####  type的参考

![image](https://user-images.githubusercontent.com/6065007/162549766-58f164df-3794-4a5a-ab25-dd47962de74e.png)
 
 

详情参考英文版的 [CONTRIBUTING](https://github.com/xujiajun/nutsdb/blob/master/CONTRIBUTING.md) 。

### 致谢

这个项目受到以下项目或多或少的灵感和帮助：

* [Bitcask-intro](https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf)
* [BoltDB](https://github.com/boltdb)
* [BuntDB](https://github.com/tidwall/buntdb)
* [Redis](https://redis.io)
* [Sorted Set](https://github.com/wangjia184/sortedset)

### License

The NutsDB is open-sourced software licensed under the [Apache 2.0 license](https://github.com/xujiajun/nutsdb/blob/master/LICENSE).
