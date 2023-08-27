package nutsdb

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xujiajun/utils/time2"
)

var bucket string

const (
	fileDir  = "/tmp/nutsdb/"
	fileDir1 = "/tmp/nutsdb/nutsdb_batch_write1"
	fileDir2 = "/tmp/nutsdb/nutsdb_batch_write2"
	fileDir3 = "/tmp/nutsdb/nutsdb_batch_write3"
	fileDir4 = "/tmp/nutsdb/nutsdb_batch_write4"
	fileDir5 = "/tmp/nutsdb/nutsdb_batch_write5"
	fileDir6 = "/tmp/nutsdb/nutsdb_batch_write6"
	fileDir7 = "/tmp/nutsdb/nutsdb_batch_write7"
	fileDir8 = "/tmp/nutsdb/nutsdb_batch_write8"
	fileDir9 = "/tmp/nutsdb/nutsdb_batch_write9"
	N        = 10000
)

func init() {
	bucket = "bucketForBatchWrite"
	files, err := ioutil.ReadDir(fileDir)
	if err != nil {
		return
	}
	for _, f := range files {
		name := f.Name()
		if name != "" {
			filePath := fmt.Sprintf("%s/%s", fileDir, name)
			err := os.RemoveAll(filePath)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func TestBatchWrite(t *testing.T) {
	TestFlushPanic := func(t *testing.T, db *DB) {
		wb, err := db.NewWriteBatch()
		require.NoError(t, err)
		require.NoError(t, wb.Flush())
		require.Error(t, ErrCommitAfterFinish, wb.Flush())
		wb, err = db.NewWriteBatch()
		require.NoError(t, err)
		require.NoError(t, wb.Cancel())
		require.Error(t, ErrCommitAfterFinish, wb.Flush())
	}

	testEmptyWrite := func(t *testing.T, db *DB) {
		wb, err := db.NewWriteBatch()
		require.NoError(t, err)
		require.NoError(t, wb.Flush())
		wb, err = db.NewWriteBatch()
		require.NoError(t, err)
		require.NoError(t, wb.Flush())
		wb, err = db.NewWriteBatch()
		require.NoError(t, err)
		require.NoError(t, wb.Flush())
	}

	testWrite := func(t *testing.T, db *DB) {
		key := func(i int) []byte {
			return []byte(fmt.Sprintf("%10d", i))
		}
		val := func(i int) []byte {
			return []byte(fmt.Sprintf("%128d", i))
		}
		wb, err := db.NewWriteBatch()
		require.NoError(t, err)
		time2.Start()
		for i := 0; i < N; i++ {
			require.NoError(t, wb.Put(bucket, key(i), val(i), 0))
		}
		require.NoError(t, wb.Flush())
		// fmt.Printf("Time taken via batch write %v keys: %v\n", N, time2.End())

		time2.Start()
		if err := db.View(
			func(tx *Tx) error {
				for i := 0; i < N; i++ {
					key := key(i)
					e, err := tx.Get(bucket, key)
					if err != nil {
						return err
					}
					require.Equal(t, val(i), e.Value)
				}
				return nil
			}); err != nil {
			log.Println(err)
		}

		// fmt.Printf("Time taken via db.View %v keys: %v\n", N, time2.End())
		// err = wb.Reset()
		wb, err = db.NewWriteBatch()
		require.NoError(t, err)
		time2.Start()
		for i := 0; i < N; i++ {
			require.NoError(t, wb.Delete(bucket, key(i)))
		}
		require.NoError(t, wb.Flush())
		// fmt.Printf("Time taken via batch delete %v keys: %v\n", N, time2.End())

		if err := db.View(
			func(tx *Tx) error {
				for i := 0; i < N; i++ {
					key := key(i)
					_, err := tx.Get(bucket, key)
					require.Error(t, ErrNotFoundKey, err)
				}
				return nil
			}); err != nil {
			log.Println(err)
		}
	}

	dbs := make([]*DB, 6)
	dbs[0], _ = Open(
		DefaultOptions,
		WithDir(fileDir1),
		WithEntryIdxMode(HintKeyValAndRAMIdxMode),
	)
	dbs[1], _ = Open(
		DefaultOptions,
		WithDir(fileDir2),
		WithEntryIdxMode(HintKeyAndRAMIdxMode),
	)
	dbs[2], _ = Open(
		DefaultOptions,
		WithDir(fileDir4),
		WithEntryIdxMode(HintKeyValAndRAMIdxMode),
		WithMaxBatchCount(195),
	)
	dbs[3], _ = Open(
		DefaultOptions,
		WithDir(fileDir5),
		WithEntryIdxMode(HintKeyAndRAMIdxMode),
		WithMaxBatchCount(195),
	)
	dbs[4], _ = Open(
		DefaultOptions,
		WithDir(fileDir7),
		WithEntryIdxMode(HintKeyValAndRAMIdxMode),
		WithMaxBatchSize(1000), // change to 1000, unit test is not ok, 1000000 is ok
	)
	dbs[5], _ = Open(
		DefaultOptions,
		WithDir(fileDir8),
		WithEntryIdxMode(HintKeyAndRAMIdxMode),
		WithMaxBatchSize(1000),
	)

	for _, db := range dbs {
		require.NotEqual(t, db, nil)
		testWrite(t, db)
		testEmptyWrite(t, db)
		TestFlushPanic(t, db)
	}
}

func TestWriteBatch_SetMaxPendingTxns(t *testing.T) {
	max := 10
	db, err := Open(
		DefaultOptions,
		WithDir("/tmp"),
	)
	require.NoError(t, err)
	wb, err := db.NewWriteBatch()
	require.NoError(t, err)
	wb.SetMaxPendingTxns(max)
	if wb.throttle == nil {
		t.Error("Expected throttle to be initialized, but it was nil")
	}
	if cap(wb.throttle.ch) != max {
		t.Errorf("Expected channel length to be %d, but got %d", max, len(wb.throttle.ch))
	}
	if cap(wb.throttle.errCh) != max {
		t.Errorf("Expected error channel length to be %d, but got %d", max, len(wb.throttle.errCh))
	}
}
