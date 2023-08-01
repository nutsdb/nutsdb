package nutsdb

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

var (
	bucket string
)

const (
	fileDir  = "/tmp/nutsdb/"
	fileDir1 = "/tmp/nutsdb/nutsdb_batch_write1"
	fileDir2 = "/tmp/nutsdb/nutsdb_batch_write2"
	fileDir3 = "/tmp/nutsdb/nutsdb_batch_write3"
	N        = 10000
)

func init() {
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
	bucket = "bucketForBatchWrite"
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
	/*
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
		}*/
	/*
		testWrite := func(t *testing.T, db *DB) {
			key := func(i int) []byte {
				return []byte(fmt.Sprintf("%10d", i))
			}
			val := func(i int) []byte {
				return []byte(fmt.Sprintf("%128d", i))
			}
			wb, err := db.NewWriteBatch()
			assert.NoError(t, err)

			time2.Start()
			for i := 0; i < N; i++ {
				require.NoError(t, wb.Put(bucket, key(i), val(i), 0))
			}

			require.NoError(t, wb.Flush())

			fmt.Printf("Time taken via batch write %v keys: %v\n", N, time2.End())

			time2.Start()
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
			fmt.Printf("Time taken via db.View %v keys: %v\n", N, time2.End())

			err = wb.Reset()
			assert.NoError(t, err)
			time2.Start()
			for i := 0; i < N; i++ {
				require.NoError(t, wb.Delete(bucket, key(i)))
			}
			require.NoError(t, wb.Flush())
			fmt.Printf("Time taken via batch delete %v keys: %v\n", N, time2.End())

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
	*/
	dbs := make([]*DB, 9)
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
		WithDir(fileDir3),
		WithEntryIdxMode(HintBPTSparseIdxMode),
	)

	dbs[3], _ = Open(
		DefaultOptions,
		WithDir(fileDir1),
		WithEntryIdxMode(HintKeyValAndRAMIdxMode),
		WithMaxBatchCount(1000),
	)
	dbs[4], _ = Open(
		DefaultOptions,
		WithDir(fileDir2),
		WithEntryIdxMode(HintKeyAndRAMIdxMode),
		WithMaxBatchCount(1000),
	)
	dbs[5], _ = Open(
		DefaultOptions,
		WithDir(fileDir3),
		WithEntryIdxMode(HintBPTSparseIdxMode),
		WithMaxBatchCount(1000),
	)

	dbs[6], _ = Open(
		DefaultOptions,
		WithDir(fileDir1),
		WithEntryIdxMode(HintKeyValAndRAMIdxMode),
		WithMaxBatchSize(1000),
	)
	dbs[7], _ = Open(
		DefaultOptions,
		WithDir(fileDir2),
		WithEntryIdxMode(HintKeyAndRAMIdxMode),
		WithMaxBatchSize(1000),
	)
	dbs[8], _ = Open(
		DefaultOptions,
		WithDir(fileDir3),
		WithEntryIdxMode(HintBPTSparseIdxMode),
		WithMaxBatchSize(1000),
	)

	for _, db := range dbs {
		//testWrite(t, db)
		//testEmptyWrite(t, db)
		TestFlushPanic(t, db)
	}
}
