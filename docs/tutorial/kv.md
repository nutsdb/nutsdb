# 使用键值对

## 基本操作

将key-value键值对保存在一个bucket, 你可以使用 `tx.Put` 这个方法:

### 添加数据

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

### 更新数据

上面的代码执行之后key为"name1"和value值"val1"被保存在命名为bucket1的bucket里面。

如果你要做更新操作，你可以仍然用`tx.Put`方法去执行，比如下面的例子把value的值改成"val1-modify"：

```go
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

### 获取数据

获取值可以用`tx.Get` 这个方法:

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

### 删除数据

删除使用`tx.Delete()` 方法：

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

### 对值的位操作

#### 使用`tx.GetBit()`方法获取某一键所对应的值在某一偏移量上的值。当对应的键存在时，返回参数中偏移量所对应位置的上的值，当偏移量超出原有的数据范围时，将返回0且不报错；当对应的键不存在时，将报错提示键不存在。

```go
if err := db.View(func(tx *nutsdb.Tx) error {
	bucket := "bucket"
	key := []byte("key")
	offset := 2
    bit, err := tx.GetBit(bucket, key, offset)
    if err != nil {
        return err
    }
    log.Println("get bit:", bit)
    return nil
}); err != nil {
    log.Println(err)
}
```

#### 使用`tx.SetBit()`方法添加某一键所对应的值在某一偏移量上的值。当对应的键存在时，将会修改偏移量所对应的位上的值；当对应的键不存在或者偏移量超出原有的数据范围时，将会对原有值进行扩容直到能够在偏移量对应位置上修改。除偏移量对应位置之外，自动扩容产生的位的值均为0。

```go
if err := db.Update(func(tx *nutsdb.Tx) error {
	bucket := "bucket"
	key := []byte("key")
	offset := 2
	bit := 1
	return tx.SetBit(bucket, key, offset, bit)
}); err != nil {
    log.Println(err)
}
```

### 对值的自增和自减操作

在对值进行自增和自减操作时需要键存在，否则将报错提示键不存在。当值的自增和自减结果将超出`int64`的范围时，将使用基于字符串的大数计算，所以不必担心值的范围过大。

* 使用`tx.Incr()`方法让某一键所对应的值自增1

```go
if err := db.Update(func(tx *nutsdb.Tx) error {
	bucket := "bucket"
	key := []byte("key")
    return tx.Incr(bucket, key)
}); err != nil {
    log.Println(err)
}
```

* 使用`tx.IncrBy()`方法让某一键所对应的值自增指定的值

```go
if err := db.Update(func(tx *nutsdb.Tx) error {
    bucket := "bucket"
    key := []byte("key")
    return tx.IncrBy(bucket, key, 10)
}); err != nil {
    log.Println(err)
}
```

* 使用`tx.Decr()`方法让某一键所对应的值自减1

```go
if err := db.Update(func(tx *nutsdb.Tx) error {
	bucket := "bucket"
	key := []byte("key")
    return tx.Decr(bucket, key)
}); err != nil {
    log.Println(err)
}
```

* 使用`tx.DecrBy()`方法让某一键所对应的值自减指定的值

```go
if err := db.Update(func(tx *nutsdb.Tx) error {
    bucket := "bucket"
    key := []byte("key")
    return tx.DecrBy(bucket, key, 10)
}); err != nil {
    log.Println(err)
}
```

### 对值的连续多次Set和Get

* 使用`tx.MSet()`方法连续多次设置键值对。当使用`tx.MSet()`需要以`...[]byte`类型传入若干个键值对。此处要求参数的总数为偶数个，设i为从0开始的偶数，则第i个参数和第i+1个参数将分别成为一个键值对的键和值。

```go
if err := db.Update(func(tx *nutsdb.Tx) error {
	bucekt := "bucket"
	args := [][]byte{
        []byte("1"), []byte("2"), []byte("3"), []byte("4"),
    }
    return tx.MSet(bucket, nutsdb.Persistent, args...)
}); err != nil {
    log.Println(err)
}
```

* 使用`tx.MGet()`方法连续多次取值。当使用`tx.MGet()`需要以`...[]byte`类型传入若干个键，若其中任何一个键不存在都会返回`key not found`错误。返回值是一个切片，长度与传入的参数相同，并且根据切片索引一一对应。

```go
if err := db.View(func(tx *nutsdb.Tx) error {
	bucket := "bucket"
	key := [][]byte{
		[]byte("1"), []byte("2"), []byte("3"), []byte("4"),
    }
    values, err := tx.MGet(bucket, key...)
    if err != nil {
        return err
    }
    for i, value := range values {
        log.Printf("get value by MGet, the %d value is '%s'", i, string(value))
    }
    return nil
}); err != nil {
    log.Println(err)
}
```

### 对值的增补操作

* 使用`tx.Append()`方法对值进行增补。

```go
if err := db.Update(func(tx *nutsdb.Tx) error {
	bucket := "bucket"
	key := "key"
	appendage := "appendage"
    return tx.Append(bucket, []byte(key), []byte(appendage))
}); err != nil {
    log.Println(err)
}
```

### 获取值的一部分

* 使用`tx.GetRange()`方法可以根据给定的索引获取值的一部分。通过两个`int`类型的参数确定一个闭区间，返回闭区间所对应部分的值。

```go
if err := db.View(func(tx *nutsdb.Tx) error {
	bucket := "bucket"
	key := "key"
	start := 0
	end := 2
    value, err := tx.GetRange(bucket, []byte(key), start, end)
    if err != nil {
        return err
    }
    log.Printf("got value: '%s'", string(value))
    return nil
}); err != nil {
    log.Println(err)
}
```

### 使用TTL

NusDB支持TTL(存活时间)的功能，可以对指定的bucket里的key过期时间的设置。使用`tx.Put`这个方法的使用`ttl`参数就可以了。
如果设置 ttl = 0 或者 Persistent, 这个key就会永久存在。下面例子中ttl设置成 60 , 60s之后key就会过期，在查询的时候将不会被搜到。

```go
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

```go

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

```go
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

### 迭代器

主要是迭代器的选项参数`Reverse`的值来决定正向还是反向迭代器, 当前版本还不支持HintBPTSparseIdxMode的迭代器


#### 正向的迭代器

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

#### 反向的迭代器

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
