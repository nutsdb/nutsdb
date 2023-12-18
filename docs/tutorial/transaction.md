# 使用事务

NutsDB为了保证隔离性，防止并发读写事务时候数据的不一致性，同一时间只能执行一个读写事务，但是允许同一时间执行多个只读事务。
从v0.3.0版本开始，NutsDB遵循标准的ACID原则。（参见[限制和警告](https://github.com/nutsdb/nutsdb/blob/master/README-CN.md#%E8%AD%A6%E5%91%8A%E5%92%8C%E9%99%90%E5%88%B6)）


## 读写事务

```go
err := db.Update(
    func(tx *nutsdb.Tx) error {
    ...
    return nil
})

```

## 只读事务

```go
err := db.View(
    func(tx *nutsdb.Tx) error {
    ...
    return nil
})

```

## 手动管理事务

从上面的例子看到 `DB.View()` 和`DB.Update()` 这两个是数据库调用事务的主要方法。他们本质上是基于 `DB.Begin()`方法进行的包装。他们可以帮你自动管理事务的生命周期，从事务的开始、事务的执行、事务提交或者回滚一直到事务的安全的关闭为止，如果中间有错误会返回。所以**一般情况下推荐用这种方式去调用事务**。

这好比开车有手动挡和自动挡一样， `DB.View()` 和`DB.Update()`等于提供了自动档的效果。

如果你需要手动去开启、执行、关闭事务，你会用到`DB.Begin()`方法开启一个事务，`tx.Commit()` 方法用来提交事务、`tx.Rollback()`方法用来回滚事务

例子：

```go
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
