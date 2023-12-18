# 开启NutsDB数据库

要打开数据库需要使用` nutsdb.Open()`这个方法。其中用到的选项(options)包括 `Dir` , `EntryIdxMode`和 `SegmentSize`，在调用的时候这些参数必须设置。官方提供了`DefaultOptions`的选项，直接使用`nutsdb.DefaultOptions`即可。当然你也可以根据需要自己定义。

例子：

```go
package main

import (
    "log"

    "github.com/nutsdb/nutsdb"
)

func main() {
    db, err := nutsdb.Open(
        nutsdb.DefaultOptions,
        nutsdb.WithDir("/tmp/nutsdb"), // 数据库会自动创建这个目录文件
    )
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    ...
}
```
