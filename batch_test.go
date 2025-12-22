package nutsdb

import (
	"fmt"
	"log"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xujiajun/utils/time2"
)

var bucket string

const (
	N = 100
)

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
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
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
					value, err := tx.Get(bucket, key)
					if err != nil {
						return err
					}
					require.Equal(t, val(i), value)
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
	tmpdir := t.TempDir()
	dbs[0], _ = Open(
		DefaultOptions,
		WithDir(filepath.Join(tmpdir, "nutsdb_batch_write1")),
		WithEntryIdxMode(HintKeyValAndRAMIdxMode),
	)
	dbs[1], _ = Open(
		DefaultOptions,
		WithDir(filepath.Join(tmpdir, "nutsdb_batch_write2")),
		WithEntryIdxMode(HintKeyAndRAMIdxMode),
	)
	dbs[2], _ = Open(
		DefaultOptions,
		WithDir(filepath.Join(tmpdir, "nutsdb_batch_write4")),
		WithEntryIdxMode(HintKeyValAndRAMIdxMode),
		WithMaxBatchCount(35),
	)
	dbs[3], _ = Open(
		DefaultOptions,
		WithDir(filepath.Join(tmpdir, "nutsdb_batch_write5")),
		WithEntryIdxMode(HintKeyAndRAMIdxMode),
		WithMaxBatchCount(35),
	)
	dbs[4], _ = Open(
		DefaultOptions,
		WithDir(filepath.Join(tmpdir, "nutsdb_batch_write7")),
		WithEntryIdxMode(HintKeyValAndRAMIdxMode),
		WithMaxBatchSize(20), // change to 1000, unit test is not ok, 1000000 is ok
	)
	dbs[5], _ = Open(
		DefaultOptions,
		WithDir(filepath.Join(tmpdir, "nutsdb_batch_write8")),
		WithEntryIdxMode(HintKeyAndRAMIdxMode),
		WithMaxBatchSize(20),
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
		WithDir(t.TempDir()),
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
