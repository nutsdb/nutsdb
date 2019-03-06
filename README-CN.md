# NutsDB [![GoDoc](https://godoc.org/github.com/xujiajun/nutsdb?status.svg)](https://godoc.org/github.com/xujiajun/nutsdb)  [![Go Report Card](https://goreportcard.com/badge/github.com/xujiajun/nutsdb)](https://goreportcard.com/report/github.com/xujiajun/nutsdb) <a href="https://travis-ci.org/xujiajun/nutsdb"><img src="https://travis-ci.org/xujiajun/nutsdb.svg?branch=master" alt="Build Status"></a> [![Coverage Status](https://coveralls.io/repos/github/xujiajun/nutsdb/badge.svg?branch=develop)](https://coveralls.io/github/xujiajun/nutsdb?branch=develop) [![License](http://img.shields.io/badge/license-Apache_2-blue.svg?style=flat-square)](https://raw.githubusercontent.com/xujiajun/nutsdb/master/LICENSE)

[English](https://github.com/xujiajun/nutsdb/blob/master/README.md) | 简体中文

NutsDB是纯Go语言编写一个简单、高性能、内嵌型、持久化的key-value数据库。

NutsDB支持ACID事务，所有的操作都在事务中执行，保证了数据的完整性。NutsDB从v0.2.0版本开始支持多种数据结构，如列表(list)、集合(set)、排序集(sorted set)。

## 为什么有NutsDB

### 对于现状或多或少的不满

我想找一个用纯go编写，尽量简单（方便二次开发、研究）、高性能（读写都能快一点）、内嵌型的（减少网络开销）数据库，最好支持事务。因为我觉得对于数据库而言，数据完整性很重要。如果能像Redis一样支持多种数据结构就更好了。
而像Redis一般用作缓存，对于事务支持也很弱。

找到几个备选项：

* BoltDB

BoltDB是一个基于B+ tree，有着非常好的读性能，还支持很实用的特性：范围扫描和按照前缀进行扫描。有很多项目采用了他。虽然现在官方不维护，由etcd团队在维护
他也支持ACID事务，但是他的写性能不是很好。如果对写性能要求不高也值得尝试。

* GoLevelDB

GoLevelDB是google开源的[leveldb](https://github.com/google/leveldb)的go语言版本的实现。他的性能很高，特别是写性能，据官方c++版本说可以到40w+次写/秒，他基于LSM tree实现。他不支持事务。

* Badger

Badger同样是基于LSM tree，不同的是他把key/value分离。据他官网描述是基于为SSD优化。同是他也支持事务。但是我简单写了[benchmark](https://github.com/xujiajun/nutsdb#benchmarks)发现他的写性能没我想象中高。

### 好奇心的驱使

对于如何实现kv数据库的好奇心吧。数据库可以说是系统的核心，了解数据库的内核或者自己有实现，对更好的用轮子或者下次根据业务定制轮子都很有帮助。

基于以上两点，我决定尝试开发一个简单的kv数据库，性能要好，功能也要强大（至少他们好的功能特性都要继承）。

如上面的选项，我发现大致基于存储引擎的模型分：B+ tree和LSM tree。基于B+ tree的模型相对后者成熟。一般使用覆盖页的方式和WAL（预写日志）来作崩溃恢复。而LSM tree的模型他是先写log文件，然后在写入MemTable内存中，当一定的时候写回SSTable，文件会越来越多，于是他一般作法是在后台进行合并和压缩操作。
一般来说，基于B+ tree的模型写性能不如LSM tree的模型。而在读性能上比LSM tree的模型要来得好。当然LSM tree的模型也可以优化，比如引入BloomFilter。
但是这些模型还是太复杂了。我喜欢简单，简单意味着好实现，好维护，相对不容易出错。

直到我找到bitcask这种模型，他其实本质上也算LSM tree的范畴吧。
他模型非常简单很好理解和实现，很快我就实现了一个版本。但是他的缺点是不支持范围扫描。我尝试去优化他，又开发一个版本，基于B+ tree作为索引，满足了范围扫描的问题
，读性能是够了，写性能很一般，又用mmap和对原模型作了精简，这样又实现了一版。写性能又提高了几十倍。现在这个版本基本上都实现上面提到的数据库的一些有用的特性，包括支持范围扫描和前缀扫描、包括支持bucket、事务等，还支持了更多的数据结构（list、set、sorted set）。从[benchmark](https://github.com/xujiajun/nutsdb#benchmarks)来看，NutsDB性能只高不低，100w条数据，我本机基本上2s跑完。写性能可达到40~50W+/秒。
 
天下没有银弹，NutsDB也有他的局限，比如随着数据量的增大，索引变大，启动会慢。只想说NutsDB还有很多优化和提高的空间，由于本人精力以及能力有限。所以把这个项目开源出来。更重要的是我认为一个项目需要有人去使用，有人提意见才会成长。

> 希望看到这个文档的童鞋有兴趣的，一起来参与贡献，欢迎Star、提issues、提交PR ！ [contributions to NutsDB](https://github.com/xujiajun/nutsdb#contributing).

## 目录

- [入门指南](#入门指南)
  - [安装](#安装)
  - [开启数据库](#开启数据库)
  - [使用事务](#使用事务)
    - [读写事务](#读写事务)
    - [只读事务](#只读事务)
    - [手动管理事务](#手动管理事务)
  - [Using buckets](#using-buckets)
  - [Using key/value pairs](#using-keyvalue-pairs)
  - [Using TTL(Time To Live)](#using-ttltime-to-live)
  - [Iterating over keys](#iterating-over-keys)
    - [Prefix scans](#prefix-scans)
    - [Range scans](#range-scans)
  - [Merge Operation](#merge-operation)
  - [Database backup](#database-backup)
- [Using Other data structures](#using-other-data-structures)
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
- [Comparison with other databases](#comparison-with-other-databases)
   - [BoltDB](#boltdb)
   - [LevelDB, RocksDB](#leveldb-rocksdb)
   - [Badger](#badger)
- [Benchmarks](#benchmarks)
- [Caveats & Limitations](#caveats--limitations)
- [Contact](#contact)
- [Contributing](#contributing)
- [Acknowledgements](#acknowledgements)
- [License](#license)

## 入门指南

### 安装

NutsDB的安装很简单，首先保证 [Golang](https://golang.org/dl/) 已经安装好 (版本要求1.11以上). 然后在终端执行命令:

```
go get -u github.com/xujiajun/nutsdb
```

### 开启数据库

要打开数据库需要使用` nutsdb.Open()`这个函数。其中用到的选项(options)包括 `Dir` , `EntryIdxMode`和 `SegmentSize`，在调用的时候这些参数必须设置。官方提供了`DefaultOptions`的选项，直接使用`nutsdb.DefaultOptions`即可。当然你也可以根据需要自己定义。

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

### 使用事务

NutsDB为了保证隔离性，防止并发读写事务时候数据的不一致性，同一时间只能执行一个读写事务，但是允许同一时间执行多个只读事务。
NutsDB遵循ACID原则。


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

从上面的例子看到 `DB.View()` 和`DB.Update()` 这两个是数据库调用事务的主要方法。他们本质上是基于 `DB.Begin()`方法进行的包装。他们可以帮你自动管理事务的生命周期，从事务的开始、事务的执行、事务提交或者回滚一直到事务的安全的关闭为止，如果中间有错误会返回。所以一般情况下推荐用这种方式去调用事务。

这好比开车有手动挡和自动挡一样， `DB.View()` 和`DB.Update()`等于提供了自动档的效果。

如果你需要手动去开启、执行、关闭事务，你会用到`DB.Begin()`开启一个事务，`tx.Commit()` 用来提交事务、`tx.Rollback()`用来回滚事务

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
if err = tx.Put(bucket, key, val, Persistent); err != nil {
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

### Using buckets

Buckets are collections of key/value pairs within the database. All keys in a bucket must be unique.
Bucket can be interpreted as a table or namespace. So you can store the same key in different bucket. 

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

### Using key/value pairs

To save a key/value pair to a bucket, use the `tx.Put` method:

```golang

if err := db.Update(
	func(tx *nutsdb.Tx) error {
	key := []byte("name1")
	val := []byte("val1")
	bucket: = "bucket1"
	if err := tx.Put(bucket, key, val, 0); err != nil {
		return err
	}
	return nil
}); err != nil {
	log.Fatal(err)
}

```

This will set the value of the "name1" key to "val1" in the bucket1 bucket.

To update the the value of the "name1" key,we can still use the `tx.Put` function:

```golang
if err := db.Update(
	func(tx *nutsdb.Tx) error {
	key := []byte("name1")
	val := []byte("val1-modify") // Update the value
	bucket: = "bucket1"
	if err := tx.Put(bucket, key, val, 0); err != nil {
		return err
	}
	return nil
}); err != nil {
	log.Fatal(err)
}

```

To retrieve this value, we can use the `tx.Get` function:

```golang
if err := db.View(
func(tx *nutsdb.Tx) error {
	key := []byte("name1")
	bucket: = "bucket1"
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

Use the `tx.Delete()` function to delete a key from the bucket.

```golang
if err := db.Update(
	func(tx *nutsdb.Tx) error {
	key := []byte("name1")
	bucket: = "bucket1"
	if err := tx.Delete(bucket, key); err != nil {
		return err
	}
	return nil
}); err != nil {
	log.Fatal(err)
}
```

### Using TTL(Time To Live)

NusDB supports TTL(Time to Live) for keys, you can use `tx.Put` function with a `ttl` parameter.

```golang
if err := db.Update(
	func(tx *nutsdb.Tx) error {
	key := []byte("name1")
	val := []byte("val1")
	bucket: = "bucket1"
	
	// If set ttl = 0 or Persistent, this key will nerver expired.
	// Set ttl = 60 , after 60 seconds, this key will expired.
	if err := tx.Put(bucket, key, val, 60); err != nil {
		return err
	}
	return nil
}); err != nil {
	log.Fatal(err)
}
```
### Iterating over keys

NutsDB stores its keys in byte-sorted order within a bucket. This makes sequential iteration over these keys extremely fast.

#### Prefix scans

To iterate over a key prefix, we can use `PrefixScan` function, and the paramter `limitNum` constrain the number of entries returned :

```golang

if err := db.View(
	func(tx *nutsdb.Tx) error {
		prefix := []byte("user_")
		// Constrain 100 entries returned 
		if entries, err := tx.PrefixScan(bucket, prefix, 100); err != nil {
			return err
		} else {
			keys, es := nutsdb.SortedEntryKeys(entries)
			for _, key := range keys {
				fmt.Println(key, string(es[key].Value))
			}
		}
		return nil
	}); err != nil {
		log.Fatal(err)
}

```

#### Range scans

To scan over a range, we can use `RangeScan` function. For example：

```golang
if err := db.View(
	func(tx *nutsdb.Tx) error {
		// Assume key from user_0000000 to user_9999999.
		// Query a specific user key range like this.
		start := []byte("user_0010001")
		end := []byte("user_0010010")
		bucket：= []byte("user_list)
		if entries, err := tx.RangeScan(bucket, start, end); err != nil {
			return err
		} else {
			keys, es := nutsdb.SortedEntryKeys(entries)
			for _, key := range keys {
				fmt.Println(key, string(es[key].Value))
			}
		}
		return nil
	}); err != nil {
	log.Fatal(err)
}
```

### Merge Operation

NutsDB supports merge operation. you can use `db.Merge()` function removes dirty data and reduce data redundancy. Call this function from a read-write transaction. It will effect other write request. So you can execute it at the appropriate time.

```golang
err := db.Merge()
if err != nil {
    ...
}
```

### Database backup

NutsDB is easy to backup. You can use the `db.Backup()` function at given dir, call this function from a read-only transaction, it will perform a hot backup and not block your other database reads and writes.

```golang
err = db.Backup(dir)
if err != nil {
   ...
}
```

### Using other data structures

The syntax here is modeled after [Redis commands](https://redis.io/commands)

#### List

##### RPush

Inserts the values at the tail of the list stored in the bucket at given bucket,key and values.

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

Inserts the values at the head of the list stored in the bucket at given bucket,key and values.

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

Removes and returns the first element of the list stored in the bucket at given bucket and key.

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

Returns the first element of the list stored in the bucket at given bucket and key.

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

Removes and returns the last element of the list stored in the bucket at given bucket and key.

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

Returns the last element of the list stored in the bucket at given bucket and key.

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

Returns the specified elements of the list stored in the bucket at given bucket,key, start and end.
The offsets start and stop are zero-based indexes 0 being the first element of the list (the head of the list),
1 being the next element and so on. 
Start and end can also be negative numbers indicating offsets from the end of the list,
where -1 is the last element of the list, -2 the penultimate element and so on.

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

Removes the first count occurrences of elements equal to value from the list stored in the bucket at given bucket,key,count.
The count argument influences the operation in the following ways:

* count > 0: Remove elements equal to value moving from head to tail.
* count < 0: Remove elements equal to value moving from tail to head.
* count = 0: Remove all elements equal to value.

```golang
if err := db.Update(
	func(tx *nutsdb.Tx) error {
	        bucket := "bucketForList"
		key := []byte("myList")
		return tx.LRem(bucket, key, 1)
	}); err != nil {
	log.Fatal(err)
}
```

##### LSet 

Sets the list element at index to value.

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

Trims an existing list so that it will contain only the specified range of elements specified.
the offsets start and stop are zero-based indexes 0 being the first element of the list (the head of the list),
1 being the next element and so on.Start and end can also be negative numbers indicating offsets from the end of the list,
where -1 is the last element of the list, -2 the penultimate element and so on.

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

Returns the size of key in the bucket in the bucket at given bucket and key.

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

Adds the specified members to the set stored int the bucket at given bucket,key and items.

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

Returns if the specified members are the member of the set int the bucket at given bucket,key and items.

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

Returns the set cardinality (number of elements) of the set stored in the bucket at given bucket and key.

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

Returns the members of the set resulting from the difference between the first set and all the successive sets in one bucket.

```go

key1 := []byte("mySet1")
key2 := []byte("mySet2")
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

Returns the members of the set resulting from the difference between the first set and all the successive sets in two buckets.

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

Returns if the set in the bucket at given bucket and key.

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

Returns if member is a member of the set stored int the bucket at given bucket,key and item.

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

Returns all the members of the set value stored int the bucket at given bucket and key.

```
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

Moves member from the set at source to the set at destination in one bucket.

```go
bucket3 := "bucket3"

if err := db.Update(
	func(tx *nutsdb.Tx) error {
		return SAdd(bucket3, []byte("mySet1"), []byte("a"), []byte("b"), []byte("c"))
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

Moves member from the set at source to the set at destination in two buckets.

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

Removes and returns one or more random elements from the set value store in the bucket at given bucket and key.

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

Removes the specified members from the set stored int the bucket at given bucket,key and items.

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

The members of the set resulting from the union of all the given sets in one bucket.

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

The members of the set resulting from the union of all the given sets in two buckets.

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

##### ZAdd

Adds the specified member with the specified score and the specified value to the sorted set stored at key.

```go
if err := db.Update(
	func(tx *nutsdb.Tx) error {
		bucket := "myZSet1"
		key := []byte("key1")
		return tx.ZAdd(bucket, key, 1, []byte("val1"))
	}); err != nil {
	log.Fatal(err)
}
```
##### ZCard 

Returns the sorted set cardinality (number of elements) of the sorted set stored at key.

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

Returns the number of elements in the sorted set at key with a score between min and max and opts.

Opts includes the following parameters:

* Limit        int  // limit the max nodes to return
* ExcludeStart bool // exclude start value, so it search in interval (start, end] or (start, end)
* ExcludeEnd   bool // exclude end value, so it search in interval [start, end) or (start, end)

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

Returns node in the bucket at given bucket and key.

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

Returns all the members of the set value stored at key.

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

Returns up to count members with the highest scores in the sorted set stored at key.

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

Returns up to count members with the lowest scores in the sorted set stored at key.

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

Removes and returns up to count members with the highest scores in the sorted set stored at key.

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

Removes and returns up to count members with the lowest scores in the sorted set stored at key.

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

returns all the elements in the sorted set in one bucket at bucket and key with a rank between start and end (including elements with rank equal to start or end).

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

Returns all the elements in the sorted set at key with a score between min and max.
And the parameter `Opts` is the same as ZCount's.

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

Returns the rank of member in the sorted set stored in the bucket at given bucket and key, with the scores ordered from low to high.

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

Returns the rank of member in the sorted set stored in the bucket at given bucket and key,with the scores ordered from high to low.

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

Removes the specified members from the sorted set stored in one bucket at given bucket and key.

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

Removes all elements in the sorted set stored in one bucket at given bucket with rank between start and end.
The rank is 1-based integer. Rank 1 means the first node; Rank -1 means the last node.

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

Returns the score of member in the sorted set in the bucket at given bucket and key.

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
### Comparison with other databases

#### BoltDB

BoltDB is similar to NutsDB, both use B+tree and support transaction. However, Bolt uses a B+tree internally and only a single file, and NutsDB is based on bitcask model with  multiple log files. NutsDB supports TTL and many data structures, but BoltDB not supports them . NutsDB offers high-performance reads and writes, but BoltDb writes performance not so good.

#### LevelDB, RocksDB

LevelDB and RocksDB are based on a log-structured merge-tree (LSM tree).An LSM tree optimizes random writes by using a write ahead log and multi-tiered, sorted files called SSTables. LevelDB does not have transactions. It supports batch writing of key/values pairs and it supports read snapshots but it will not give you the ability to do a compare-and-swap operation safely. NutsDB supports fully serializable ACID transactions.

#### Badger

Badger is based in LSM tree with value log. It designed for SSDs. It also supports transaction and TTL. But in my benchmark its write performance is not as good as i thought. In addition, NutsDB supports data structures such as list、set、sorted set, but Badger not supports them.

### Benchmarks

#### Tested kvstore

* [BadgerDB](https://github.com/dgraph-io/badger) (with default options)
* [BoltDB](https://github.com/boltdb/bolt) (with default options)
* [BuntDB](https://github.com/tidwall/buntdb) (with default options)
* [LevelDB](https://github.com/syndtr/goleveldb) (with default options)
* [NutsDB](https://github.com/xujiajun/nutsdb) (with default options or custom options)

#### Benchmark System:

* Go Version : go1.11.4 darwin/amd64
* OS: Mac OS X 10.13.6
* Architecture: x86_64
* 16 GB 2133 MHz LPDDR3
* CPU: 3.1 GHz Intel Core i7

#### Benchmark results:

```
BenchmarkBadgerDBPutValue64B-8    	   10000	    135431 ns/op	    2375 B/op	      74 allocs/op
BenchmarkBadgerDBPutValue128B-8   	   10000	    119450 ns/op	    2503 B/op	      74 allocs/op
BenchmarkBadgerDBPutValue256B-8   	   10000	    142451 ns/op	    2759 B/op	      74 allocs/op
BenchmarkBadgerDBPutValue512B-8   	   10000	    109066 ns/op	    3270 B/op	      74 allocs/op
BenchmarkBadgerDBGet-8            	 1000000	      1679 ns/op	     416 B/op	       9 allocs/op
BenchmarkBoltDBPutValue64B-8      	    5000	    200487 ns/op	   20005 B/op	      59 allocs/op
BenchmarkBoltDBPutValue128B-8     	    5000	    230297 ns/op	   13703 B/op	      64 allocs/op
BenchmarkBoltDBPutValue256B-8     	    5000	    207220 ns/op	   16708 B/op	      64 allocs/op
BenchmarkBoltDBPutValue512B-8     	    5000	    262358 ns/op	   17768 B/op	      64 allocs/op
BenchmarkBoltDBGet-8              	 1000000	      1163 ns/op	     592 B/op	      10 allocs/op
BenchmarkBoltDBRangeScans-8       	 1000000	      1226 ns/op	     584 B/op	       9 allocs/op
BenchmarkBoltDBPrefixScans-8      	 1000000	      1275 ns/op	     584 B/op	       9 allocs/op
BenchmarkBuntDBPutValue64B-8      	  200000	      8930 ns/op	     927 B/op	      14 allocs/op
BenchmarkBuntDBPutValue128B-8     	  200000	      8892 ns/op	    1015 B/op	      15 allocs/op
BenchmarkBuntDBPutValue256B-8     	  200000	     11282 ns/op	    1274 B/op	      16 allocs/op
BenchmarkBuntDBPutValue512B-8     	  200000	     12323 ns/op	    1794 B/op	      16 allocs/op
BenchmarkBuntDBGet-8              	 2000000	       675 ns/op	     104 B/op	       4 allocs/op
BenchmarkLevelDBPutValue64B-8     	  100000	     11909 ns/op	     476 B/op	       7 allocs/op
BenchmarkLevelDBPutValue128B-8    	  200000	     10838 ns/op	     254 B/op	       7 allocs/op
BenchmarkLevelDBPutValue256B-8    	  100000	     11510 ns/op	     445 B/op	       7 allocs/op
BenchmarkLevelDBPutValue512B-8    	  100000	     12661 ns/op	     799 B/op	       8 allocs/op
BenchmarkLevelDBGet-8             	 1000000	      1371 ns/op	     184 B/op	       5 allocs/op
BenchmarkNutsDBPutValue64B-8      	 1000000	      2472 ns/op	     670 B/op	      14 allocs/op
BenchmarkNutsDBPutValue128B-8     	 1000000	      2182 ns/op	     664 B/op	      13 allocs/op
BenchmarkNutsDBPutValue256B-8     	 1000000	      2579 ns/op	     920 B/op	      13 allocs/op
BenchmarkNutsDBPutValue512B-8     	 1000000	      3640 ns/op	    1432 B/op	      13 allocs/op
BenchmarkNutsDBGet-8              	 2000000	       781 ns/op	      88 B/op	       3 allocs/op
BenchmarkNutsDBGetByMemoryMap-8   	   50000	     40734 ns/op	     888 B/op	      17 allocs/op
BenchmarkNutsDBPrefixScan-8       	 1000000	      1293 ns/op	     656 B/op	       9 allocs/op
BenchmarkNutsDBRangeScan-8        	 1000000	      2250 ns/op	     752 B/op	      12 allocs/op
```

#### Conclusions:

* Put(write) Performance:  NutsDB、BuntDB、LevelDB are fast. And NutsDB is fastest. It is 5-10x faster than LevelDB, 5x faster than BuntDB.

*  Get(read) Performance: All are fast. And NutsDB and BuntDB with default options are 2x faster than others.
And NutsDB reads with MemoryMap option is slower 40x than its default option way. 


The benchmarking code can be found in the [gokvstore-bench](https://github.com/xujiajun/gokvstore-bench) repo.

### Caveats & Limitations

NutsDB supports two modes about entry index: `HintAndRAMIdxMode`  and  `HintAndMemoryMapIdxMode`. The default mode use `HintAndRAMIdxMode`, entries are indexed base on RAM, so its read/write performance is fast. but can’t handle databases much larger than the available physical RAM. If you set the `HintAndMemoryMapIdxMode` mode, HintIndex will not cache the value of the entry. Its write performance is also fast. To retrieve a key by seeking to offset relative to the start of the data file, so its read performance more slowly that RAM way, but it can handle databases much larger than the available physical RAM. And other data structures such as ***list, set, sorted set only supported with mode HintAndRAMIdxMode***.

NutsDB will truncate data file if the active file is larger than  `SegmentSize`, so the size of an entry can not be set larger than `SegmentSize` , defalut `SegmentSize` is 8MB, you can set it(opt.SegmentSize) as option before DB opening. ***Once set, it cannot be changed***.

### Contact

* [xujiajun](https://github.com/xujiajun)

### Contributing

See [CONTRIBUTING](https://github.com/xujiajun/nutsdb/blob/master/CONTRIBUTING.md) for details on submitting patches and the contribution workflow.

### Acknowledgements

This package is inspired by the following:

* [Bitcask-intro](https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf)
* [BoltDB](https://github.com/boltdb)
* [BuntDB](https://github.com/tidwall/buntdb)
* [Redis](https://redis.io)
* [Sorted Set](https://github.com/wangjia184/sortedset)

### License

The NutsDB is open-sourced software licensed under the [Apache 2.0 license](https://github.com/xujiajun/nutsdb/blob/master/LICENSE).
