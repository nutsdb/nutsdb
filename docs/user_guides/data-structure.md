#        Data Structure

The syntax here is modeled after [Redis commands](https://redis.io/commands)

## List

<details>
  <summary><b>RPush</b></summary>
	Inserts the values at the tail of the list stored in the bucket at given bucket, key and values.

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
</details>

<details>
  <summary><b>LPush</b></summary>
    Inserts the values at the head of the list stored in the bucket at given bucket, key and values.

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
</details>

<details>
  <summary><b>LPop</b></summary>
    Removes and returns the first element of the list stored in the bucket at given bucket and key.

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
</details>

<details>
  <summary><b>LPeek</b></summary>
Returns the first element of the list stored in the bucket at given bucket and key.

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
  </details>

<details>
  <summary><b>RPop</b></summary>
Removes and returns the last element of the list stored in the bucket at given bucket and key.

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
  </details>

<details>
  <summary><b>RPeek</b></summary>
Returns the last element of the list stored in the bucket at given bucket and key.

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
  </details>

<details>
  <summary><b>LRange</b></summary>
Returns the specified elements of the list stored in the bucket at given bucket,key, start and end. The offsets start and stop are zero-based indexes 0 being the first element of the list (the head of the list), 1 being the next element and so on. Start and end can also be negative numbers indicating offsets from the end of the list, where -1 is the last element of the list, -2 the penultimate element and so on.

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
</details>

<details>
  <summary><b>LRem</b></summary>
Note: This feature can be used starting from v0.6.0

Removes the first count occurrences of elements equal to value from the list stored in the bucket at given bucket,key,count. The count argument influences the operation in the following ways:

- count > 0: Remove elements equal to value moving from head to tail.
- count < 0: Remove elements equal to value moving from tail to head.
- count = 0: Remove all elements equal to value.

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "bucketForList"
        key := []byte("myList")
        return tx.LRem(bucket, key, 1, []byte("value11))
    }); err != nil {
    log.Fatal(err)
}
```
  </details>

<details>
  <summary><b>LRemByIndex</b></summary>
Note: This feature can be used starting from v0.10.0

Remove the element at a specified position (single or multiple) from the list

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
</details>

<details>
  <summary><b>LTrim</b></summary>
Trims an existing list so that it will contain only the specified range of elements specified. The offsets start and stop are zero-based indexes 0 being the first element of the list (the head of the list), 1 being the next element and so on.Start and end can also be negative numbers indicating offsets from the end of the list, where -1 is the last element of the list, -2 the penultimate element and so on.

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
</details>

<details>
  <summary><b>LSize</b></summary>
Returns the size of key in the bucket in the bucket at given bucket and key.

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
  </details>

<details>
  <summary><b>LKeys</b></summary>
find all keys of type List matching a given pattern, similar to Redis command: KEYS

Note: pattern matching use filepath.Match, It is different from redis' behavior in some details, such as [.

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
</details>


## Set

<details>
  <summary><b>SAdd</b></summary>
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
</details>

<details>
  <summary><b>SAreMembers</b></summary>
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
</details>

<details>
  <summary><b>SCard</b></summary>
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
</details>

<details>
  <summary><b>SDiffByOneBucket</b></summary>
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
</details>

<details>
  <summary><b>SDiffByTwoBuckets</b></summary>
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
</details>

<details>
  <summary><b>SHasKey</b></summary>
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
</details>

<details>
  <summary><b>SIsMember</b></summary>
Returns if member is a member of the set stored in the bucket at given bucket, key and item.

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
</details>

<details>
  <summary><b>SMembers</b></summary>
Returns all the members of the set value stored in the bucket at given bucket and key.

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
</details>

<details>
  <summary><b>SMoveByOneBucket</b></summary>
Moves member from the set at source to the set at destination in one bucket.

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
</details>

<details>
  <summary><b>SMoveByTwoBuckets</b></summary>
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
</details>

<details>
  <summary><b>SPop</b></summary>
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
</details>

<details>
  <summary><b>SRem</b></summary>
Removes the specified members from the set stored in the bucket at given bucket,key and items.

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
</details>

<details>
  <summary><b>SUnionByOneBucket</b></summary>
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
</details>

<details>
  <summary><b>SUnionByTwoBuckets</b></summary>
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
</details>

<details>
  <summary><b>SKeys</b></summary>
find all keys of type Set matching a given pattern, similar to Redis command: KEYS

Note: pattern matching use filepath.Match, It is different from redis' behavior in some details, such as [.

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
</details>

## Sorted Set

<details>
  <summary><b>ZAdd</b></summary>
Adds the specified member with the specified score into the sorted set specified by key in a bucket.

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
</details>

<details>
  <summary><b>ZCard</b></summary>
Returns the sorted set cardinality (number of elements) of the sorted set specified by key in a bucket.

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        num, err := tx.ZCard(bucket, string(key))
        if err != nil {
            return err
        }
        fmt.Println("ZCard num", num)
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
</details>

<details>
  <summary><b>ZCount</b></summary>
Returns the number of elements in the sorted set specified by key in a bucket with a score between min and max and opts.

Opts includes the following parameters:

- Limit int // limit the max nodes to return
- ExcludeStart bool // exclude start value, so it search in interval (start, end] or (start, end)
- ExcludeEnd bool // exclude end value, so it search in interval [start, end) or (start, end)

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        num, err := tx.ZCount(bucket, string(key), 0, 1, nil)
        if err != nil {
            return err
        }
        fmt.Println("ZCount num", num)
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
</details>

<details>
  <summary><b>ZScore</b></summary>
Returns the score of members in a sorted set specified by key in a bucket.

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        score, err := tx.ZScore(bucket, string(key), []byte("val1"))
        if err != nil {
            return err
        }
        fmt.Println("val1 score: ", score)
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
</details>

<details>
  <summary><b>ZMembers</b></summary>
Returns all the members and scores of members of the set specified by key in a bucket.

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        nodes, err := tx.ZMembers(bucket, string(key))
        if err != nil {
            return err
        }
        for node := range nodes {
            fmt.Println("member:", node.Score, string(node.Value))
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
</details>

<details>
  <summary><b>ZPeekMax</b></summary>
Returns the member with the highest score in the sorted set specified by key in a bucket.

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        node, err := tx.ZPeekMax(bucket, string(key))
        if err != nil {
            return err
        }
        fmt.Println("ZPeekMax:", node.Score)
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
</details>

<details>
  <summary><b>ZPeekMin</b></summary>
Returns the member with the lowest score in the sorted set specified by key in a bucket.

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        node, err := tx.ZPeekMin(bucket, string(key))
        if err != nil {
            return err
        }
        fmt.Println("ZPeekMin:", node.Score)
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
</details>

<details>
  <summary><b>ZPopMax</b></summary>
Removes and returns the member with the highest score in the sorted set specified by key in a bucket.

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        node, err := tx.ZPopMax(bucket, string(key))
        if err != nil {
            return err
        }
        fmt.Println("ZPopMax:", node.Score)
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
</details>

<details>
  <summary><b>ZPopMin</b></summary>
Removes and returns the member with the lowest score in the sorted set specified by key in a bucket.

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        node, err := tx.ZPopMin(bucket, string(key))
        if err != nil {
            return err
        }
        fmt.Println("ZPopMin:", node.Score)
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
</details>

<details>
  <summary><b>ZRangeByRank</b></summary>
Returns all the elements in the sorted set specified by key in a bucket with a rank between start and end (including elements with rank equal to start or end).

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        return tx.ZAdd(bucket, key, 1, []byte("val1"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        return tx.ZAdd(bucket, key, 2, []byte("val2"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        return tx.ZAdd(bucket, key, 3, []byte("val3"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        nodes, err := tx.ZRangeByRank(bucket, string(key), 1, 3)
        if err != nil {
            return err
        }
        for _, node := range nodes {
            fmt.Println("item:", string(node.Value), node.Score)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
</details>

<details>
  <summary><b>ZRangeByScore</b></summary>
Returns all the elements in the sorted set specified by key in a bucket with a score between min and max. And the parameter Opts is the same as ZCount's.

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        return tx.ZAdd(bucket, key, 70, []byte("val1"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        return tx.ZAdd(bucket, key, 90, []byte("val2"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        return tx.ZAdd(bucket, key, 86, []byte("val3"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        nodes, err := tx.ZRangeByScore(bucket, string(key), 80, 100, nil)
        if err != nil {
            return err
        }
        for _, node := range nodes {
            fmt.Println("item:", node.Value, node.Score)
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
</details>

<details>
  <summary><b>ZRank</b></summary>
Returns the rank of member in the sorted set specified by key in a bucket, with the scores ordered from low to high.


```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        rank, err := tx.ZRank(bucket, string(key), []byte("val1"))
        if err != nil {
            return err
        }
        fmt.Println("val1 ZRank :", rank)
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        rank, err := tx.ZRank(bucket, string(key), []byte("val2"))
        if err != nil {
            return err
        }
        fmt.Println("val2 ZRank :", rank)
        return nil
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        rank, err := tx.ZRank(bucket, string(key), []byte("val3"))
        if err != nil {
            return err
        }
        fmt.Println("val3 ZRank :", rank)
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
</details>

<details>
  <summary><b>ZRevRank</b></summary>
Returns the rank of member in the sorted set specified by key in a bucket, with the scores ordered from high to low.


```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        rank, err := tx.ZRank(bucket, string(key), []byte("val1"))
        if err != nil {
            return err
        }
        fmt.Println("ZRevRank val1 rank:", rank) // ZRevRank key1 rank: 3
        return nil
    }); err != nil {
    log.Fatal(err)
}
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        rank, err := tx.ZRank(bucket, string(key), []byte("val2"))
        if err != nil {
            return err
        }
        fmt.Println("ZRevRank val2 rank:", rank) // ZRevRank key2 rank: 2
        return nil
    }); err != nil {
    log.Fatal(err)
}
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        rank, err := tx.ZRank(bucket, string(key), []byte("val3"))
        if err != nil {
            return err
        }
        fmt.Println("ZRevRank val3 rank:", rank) // ZRevRank key3 rank: 1
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
</details>

<details>
  <summary><b>ZRem</b></summary>
Returns the member with the lowest score in the sorted set specified by key in a bucket.

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        return tx.ZRem(bucket, string(key), []byte("val3"))
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        nodes, err := tx.ZMembers(bucket, string(key))
        if err != nil {
            return err
        }
        fmt.Println("after ZRem key1, ZMembers nodes")
        for node := range nodes {
            fmt.Println("item:", node.Score, string(node.Value))
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
</details>

<details>
  <summary><b>ZRemRangeByRank</b></summary>
Removes all elements in the sorted set stored specified by key in a bucket with rank between start and end. The rank is 1-based integer. Rank 1 means the first node; Rank -1 means the last node.

```go
if err := db.Update(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        return tx.ZRemRangeByRank(bucket, string(key), 1, 2)
    }); err != nil {
    log.Fatal(err)
}

if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "myZSet1"
        key := []byte("key1")
        nodes, err := tx.ZMembers(bucket, string(key))
        if err != nil {
            return err
        }
        fmt.Println("after ZRemRangeByRank, ZMembers nodes is 0")
        for node := range nodes {
            fmt.Println("item:", node.Score, string(node.Value))
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```
</details>
