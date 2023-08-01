package nutsdb

import (
    "fmt"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "github.com/xujiajun/utils/time2"
    "io/ioutil"
    "log"
    "os"
    "testing"
)

var (
    bucket string
)

func init() {
    fileDir := "/tmp/nutsdb_batch_write"

    files, _ := ioutil.ReadDir(fileDir)
    for _, f := range files {
        name := f.Name()
        if name != "" {
            fmt.Println(fileDir + "/" + name)
            err := os.RemoveAll(fileDir + "/" + name)
            if err != nil {
                panic(err)
            }
        }
    }
    db, _ = Open(
        DefaultOptions,
        WithDir(fileDir),
    )
    bucket = "bucketForBatchWrite"
}

func TestWriteBatch(t *testing.T) {
    key := func(i int) []byte {
        return []byte(fmt.Sprintf("%10d", i))
    }
    val := func(i int) []byte {
        return []byte(fmt.Sprintf("%128d", i))
    }
    wb, err := db.NewWriteBatch()
    assert.NoError(t, err)

    N := 500000

    time2.Start()
    for i := 0; i < N; i++ {
        require.NoError(t, wb.Put(bucket, key(i), val(i), 0))
    }

    require.NoError(t, wb.Flush())

    fmt.Printf("Time taken via batch write %v keys: %v\n", N, time2.End())

    if err := db.View(
        func(tx *Tx) error {
            for i := 0; i < N; i++ {
                key := key(i)
                e, err := tx.Get(bucket, key)
                if err != nil {
                    return err
                }
                require.Equal(t, val(i), (e.Value))
            }
            return nil
        }); err != nil {
        log.Println(err)
    }

    fmt.Println("begin batch delete")
    err = wb.Reset()
    assert.NoError(t, err)
    time2.Start()
    //require.NoError(t, wb.Delete(bucket, key(0)))
    for i := 0; i < N; i++ {
        require.NoError(t, wb.Delete(bucket, key(i)))
    }
    fmt.Println("begin wait delete")
    require.NoError(t, wb.Flush())
    fmt.Printf("Time taken via batch delete %v keys: %v\n", N, time2.End())
}
