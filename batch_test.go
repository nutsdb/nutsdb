package nutsdb

import (
	"fmt"
	"log"
	"path/filepath"
	"sync"
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
	for _, db := range dbs {
		require.NoError(t, db.Close())
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

	// TODO:
	// not sure if this usage is available.
	db.statusMgr.cancel()
	db.statusMgr.wg.Wait()
	// Simulate shutdown so the new transaction cannot be opened.
	db.statusMgr.closing.Store(true)
	db.release()
}

func TestWriteBatchCommit_UnregistersOnBeginTxFailure(t *testing.T) {
	db, err := Open(
		DefaultOptions,
		WithDir(t.TempDir()),
	)
	require.NoError(t, err)
	defer func() { _ = db.release() }()

	wb, err := db.NewWriteBatch()
	require.NoError(t, err)

	// Stop the background writer so the async commit request is not processed.
	db.statusMgr.cancel()
	db.statusMgr.wg.Wait()

	// Simulate shutdown so the new transaction cannot be opened.
	db.statusMgr.closing.Store(true)

	err = wb.commit()
	require.ErrorIs(t, err, ErrDBClosed)

	// Drain the queued request so the CommitWith callback can return.
	select {
	case req := <-db.writeCh:
		req.Wg.Done()
	default:
		t.Fatal("expected commit request in writeCh")
	}

	require.NoError(t, wb.throttle.Finish())

	require.Equal(t, int64(0), db.transactionMgr.GetActiveTxCount(), "committing tx should be unregistered on BeginTx failure")
}

func TestWriteBatchCancel_UnregistersTransaction(t *testing.T) {
	db, err := Open(
		DefaultOptions,
		WithDir(t.TempDir()),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	wb, err := db.NewWriteBatch()
	require.NoError(t, err)
	require.Equal(t, int64(1), db.transactionMgr.GetActiveTxCount(), "write batch should register a tx")

	require.NoError(t, wb.Cancel())
	require.Equal(t, int64(0), db.transactionMgr.GetActiveTxCount(), "Cancel should unregister the tx")

	// Subsequent Cancel should be a no-op.
	require.NoError(t, wb.Cancel())
}

func TestWriteBatchFlush_UnregistersFinalTransaction(t *testing.T) {
	db, err := Open(
		DefaultOptions,
		WithDir(t.TempDir()),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	const bucket = "flush_bucket"
	require.NoError(t, db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, bucket)
	}))

	wb, err := db.NewWriteBatch()
	require.NoError(t, err)
	require.Equal(t, int64(1), db.transactionMgr.GetActiveTxCount())

	require.NoError(t, wb.Put(bucket, []byte("key"), []byte("value"), 0))
	require.NoError(t, wb.Flush())

	require.Equal(t, int64(0), db.transactionMgr.GetActiveTxCount(), "Flush should unregister all transactions")

	require.NoError(t, db.View(func(tx *Tx) error {
		val, err := tx.Get(bucket, []byte("key"))
		require.NoError(t, err)
		require.Equal(t, []byte("value"), val)
		return nil
	}))
}

func TestWriteBatchReset_ReplacesTransaction(t *testing.T) {
	db, err := Open(
		DefaultOptions,
		WithDir(t.TempDir()),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	wb, err := db.NewWriteBatch()
	require.NoError(t, err)
	require.Equal(t, int64(1), db.transactionMgr.GetActiveTxCount())

	firstTxID := wb.txID
	require.NoError(t, wb.Reset())

	require.Equal(t, int64(1), db.transactionMgr.GetActiveTxCount(), "Reset should keep exactly one active transaction")
	require.NotEqual(t, firstTxID, wb.txID, "Reset should allocate a fresh transaction ID")
	activeIDs := db.transactionMgr.GetActiveTxs()
	require.NotContains(t, activeIDs, firstTxID, "old transaction must be unregistered on Reset")
	require.Contains(t, activeIDs, wb.txID, "new transaction must be registered")

	_ = wb.Flush()
}

func TestWriteBatch_ConcurrentWithUpdate(t *testing.T) {
	db, err := Open(
		DefaultOptions,
		WithDir(t.TempDir()),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	txCreateBucket(t, db, DataStructureBTree, "test-bucket", nil)

	const iterations = 1000
	const goroutines = 2
	var ready sync.WaitGroup
	var start sync.WaitGroup
	done := make(chan error, goroutines)

	ready.Add(goroutines)
	start.Add(1)

	// WriteBatch goroutine
	go func() {
		ready.Done()
		start.Wait()
		for i := 0; i < iterations; i++ {
			wb, err := db.NewWriteBatch()
			if err != nil {
				done <- err
				return
			}
			key := []byte(fmt.Sprintf("wb-key-%d", i))
			if err := wb.Put("test-bucket", key, []byte("value"), 0); err != nil {
				_ = wb.Cancel()
				done <- err
				return
			}
			if err := wb.Flush(); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	// db.Update goroutine
	go func() {
		ready.Done()
		start.Wait()
		for i := 0; i < iterations; i++ {
			err := db.Update(func(tx *Tx) error {
				key := []byte(fmt.Sprintf("update-key-%d", i))
				return tx.Put("test-bucket", key, []byte("value"), 0)
			})
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	ready.Wait()
	start.Done()

	for i := 0; i < goroutines; i++ {
		require.NoError(t, <-done)
	}
}
