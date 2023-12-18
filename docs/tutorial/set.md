# 使用集合

## SAdd

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

## SAreMembers

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

## SCard

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
## SDiffByOneBucket

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

## SDiffByTwoBuckets

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
## SHasKey

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
## SIsMember

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
## SMembers

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
## SMoveByOneBucket

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
## SMoveByTwoBuckets

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
## SPop

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
## SRem

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
## SUnionByOneBucket

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

## SUnionByTwoBuckets

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

## SKeys

查找`Set`类型的所有匹配指定模式`pattern`的`key`，类似于Redis命令: [KEYS](https://redis.io/commands/keys/)

注意：模式匹配使用 Go 标准库的`filepath.Match`，部分细节上和redis的行为有区别，比如对于 `[` 的处理。

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        var keys []string
        err := tx.SKeys(bucket, "*", func(key string) bool {
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
