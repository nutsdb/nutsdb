# 使用有序集合

> 注意：这边的bucket是有序集合名。

## ZAdd

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
## ZCard

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

## ZCount

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
## ZGetByKey

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
## ZMembers

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
## ZPeekMax

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

## ZPeekMin

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

## ZPopMax

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
## ZPopMin

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

## ZRangeByRank

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

## ZRangeByScore

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
## ZRank

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

## ZRem

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

## ZRemRangeByRank

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
## ZScore

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

## ZKeys

查找`Sorted Set`类型的所有匹配指定模式`pattern`的`key`，类似于Redis命令: [KEYS](https://redis.io/commands/keys/)

注意：模式匹配使用 Go 标准库的`filepath.Match`，部分细节上和redis的行为有区别，比如对于 `[` 的处理。

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        var keys []string
        err := tx.ZKeys(bucket, "*", func(key string) bool {
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
