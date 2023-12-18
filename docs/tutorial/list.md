# 使用列表

## RPush

从指定bucket里面的指定队列key的右边入队一个或者多个元素val。

```go
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

## LPush

从指定bucket里面的指定队列key的左边入队一个或者多个元素val。

```go
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

## LPop

从指定bucket里面的指定队列key的左边出队一个元素，删除并返回。

```go
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

## LPeek

从指定bucket里面的指定队列key的左边出队一个元素返回不删除。

```go
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

## RPop

从指定bucket里面的指定队列key的右边出队一个元素，删除并返回。

```go
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

## RPeek

从指定bucket里面的指定队列key的右边出队一个元素返回不删除。

```go
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

## LRange

返回指定bucket里面的指定队列key列表里指定范围内的元素。 start 和 end 偏移量都是基于0的下标，即list的第一个元素下标是0（list的表头），第二个元素下标是1，以此类推。
偏移量也可以是负数，表示偏移量是从list尾部开始计数。 例如：-1 表示列表的最后一个元素，-2 是倒数第二个，以此类推。

```go
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
## LRem

注意: 这个方法在 v0.6.0版本开始支持，之前的版本实现和描述有问题。

从指定bucket里面的指定的key的列表里移除前 count 次出现的值为 value 的元素。 这个 count 参数通过下面几种方式影响这个操作：

count > 0: 从头往尾移除值为 value 的元素。
count < 0: 从尾往头移除值为 value 的元素。
count = 0: 移除所有值为 value 的元素。

下面的例子count=1：

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        return tx.LRem(bucket, key, 1, []byte("val11"))
    }); err != nil {
    log.Fatal(err)
}
```

## LRemByIndex

注意: 这个方法在 v0.10.0版本开始支持

移除列表中指定位置（单个或多个位置）的元素

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "bucketForList"
        key := []byte("myList")
        removedNum, err := tx.LRemByIndex(bucket, key, 0, 1)
        fmt.Printf("removed num %d\n", removedNum)
        return err
    }); err != nil {
    log.Fatal(err)
}
```

## LTrim

修剪一个已存在的 list，这样 list 就会只包含指定范围的指定元素。start 和 stop 都是由0开始计数的， 这里的 0 是列表里的第一个元素（表头），1 是第二个元素，以此类推。

例如： LTRIM foobar 0 2 将会对存储在 foobar 的列表进行修剪，只保留列表里的前3个元素。

start 和 end 也可以用负数来表示与表尾的偏移量，比如 -1 表示列表里的最后一个元素， -2 表示倒数第二个，等等。

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
            bucket := "bucketForList"
        key := []byte("myList")
        return tx.LTrim(bucket, key, 0, 1)
    }); err != nil {
    log.Fatal(err)
}
```

## LSize

返回指定bucket下指定key列表的size大小

```go
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

## LKeys

查找`List`类型的所有匹配指定模式`pattern`的`key`，类似于Redis命令: [KEYS](https://redis.io/commands/keys/)

注意：模式匹配使用 Go 标准库的`filepath.Match`，部分细节上和redis的行为有区别，比如对于 `[` 的处理。

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        var keys []string
        err := tx.LKeys(bucket, "*", func(key string) bool {
            keys = append(keys, key)
            // true: continue, false: break
            return true
        })
        fmt.Printf("keys: %v\n", keys)
        return err
    }); err != nil {
    log.Fatal(err)
}
```
