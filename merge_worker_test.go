package nutsdb

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMergeWorker_Creation(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	mw := db.mergeWorker
	require.NotNil(t, mw)
	require.Equal(t, "MergeWorker", mw.Name())
	require.False(t, mw.IsMerging())
}

func TestMergeWorker_StartStop(t *testing.T) {
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer func() { _ = sm.Close() }()

	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	config := DefaultMergeConfig()
	mw := newMergeWorker(db, sm, config)

	ctx := context.Background()
	require.NoError(t, mw.Start(ctx))

	require.True(t, mw.lifecycle.IsRunning())

	require.NoError(t, mw.Stop(5*time.Second))

	require.NoError(t, mw.Stop(5*time.Second))
}

func TestMergeWorker_TriggerMerge(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	err = db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, "bucket1")
	})
	require.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		for i := 0; i < 100; i++ {
			key := []byte("key" + string(rune(i)))
			val := []byte("value" + string(rune(i)))
			if err := tx.Put("bucket1", key, val, 0); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	err = db.mergeWorker.TriggerMerge()
	if err != nil && err != ErrDontNeedMerge {
		t.Fatalf("Unexpected error from TriggerMerge: %v", err)
	}

	require.False(t, db.mergeWorker.IsMerging())
}

func TestMergeWorker_RejectMergeWhenClosing(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, "bucket1")
	})
	require.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		return tx.Put("bucket1", []byte("key1"), []byte("value1"), 0)
	})
	require.NoError(t, err)

	err = db.mergeWorker.TriggerMerge()
	if err != nil && err != ErrDontNeedMerge {
		t.Fatalf("Expected merge to succeed or return ErrDontNeedMerge in Open state, got: %v", err)
	}

	db.statusMgr.closing.Store(true)

	err = db.mergeWorker.TriggerMerge()
	require.Equal(t, ErrDBClosed, err, "Expected ErrDBClosed when triggering merge in Closing state")

	db.statusMgr.closing.Store(false)
	db.statusMgr.closed.Store(true)

	err = db.mergeWorker.TriggerMerge()
	require.Equal(t, ErrDBClosed, err, "Expected ErrDBClosed when triggering merge in Closed state")

	_ = db.mergeWorker.Stop(5 * time.Second)
	_ = db.transactionMgr.Stop(5 * time.Second)
	_ = db.ttlService.Stop(5 * time.Second)
	if db.watchMgr != nil {
		_ = db.watchMgr.Stop(5 * time.Second)
	}
}

func TestMergeWorker_RejectConcurrentMerge(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	err = db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, "bucket1")
	})
	require.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		for i := 0; i < 1000; i++ {
			key := []byte("key" + string(rune(i)))
			val := []byte("value" + string(rune(i)))
			if err := tx.Put("bucket1", key, val, 0); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	mergeStarted := make(chan struct{})

	var wg sync.WaitGroup
	var firstMergeErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(mergeStarted)
		firstMergeErr = db.mergeWorker.TriggerMerge()
	}()

	<-mergeStarted

	for i := 0; i < 100; i++ {
		if db.mergeWorker.IsMerging() {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	err = db.mergeWorker.TriggerMerge()
	if err != nil && err != ErrIsMerging && err != ErrDontNeedMerge {
		t.Errorf("Expected ErrIsMerging or ErrDontNeedMerge, got: %v", err)
	}

	wg.Wait()

	if firstMergeErr != nil && firstMergeErr != ErrDontNeedMerge {
		t.Errorf("First merge failed with unexpected error: %v", firstMergeErr)
	}
}

func TestMergeWorker_ContextCancellation(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)

	require.NoError(t, db.mergeWorker.Stop(5*time.Second))

	err = db.mergeWorker.TriggerMerge()
	require.Equal(t, ErrDBClosed, err)

	start := time.Now()
	require.NoError(t, db.mergeWorker.Stop(5*time.Second))
	elapsed := time.Since(start)

	if elapsed > 1*time.Second {
		t.Errorf("Expected Stop to complete quickly after already stopped, took %v", elapsed)
	}

	require.NoError(t, db.Close())
}

func TestMergeWorker_AutoMerge(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	opts.SegmentSize = KB // Use small segment size to trigger merge with small data
	db, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	err = db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, "bucket1")
	})
	require.NoError(t, err)

	n := 1000
	for i := 0; i < n; i++ {
		err = db.Update(func(tx *Tx) error {
			key := []byte(fmt.Sprintf("key%d", i))
			val := []byte(fmt.Sprintf("value%d", i))
			return tx.Put("bucket1", key, val, 0)
		})
		require.NoError(t, err)
	}

	for i := 0; i < n/2; i++ {
		err = db.Update(func(tx *Tx) error {
			key := []byte(fmt.Sprintf("key%d", i))
			return tx.Delete("bucket1", key)
		})
		require.NoError(t, err)
	}

	require.NoError(t, db.Close())
	db, err = Open(opts)
	require.NoError(t, err)

	db.mergeWorker.SetMergeInterval(200 * time.Millisecond)

	time.Sleep(1 * time.Second)

	db.mergeWorker.SetMergeInterval(0)

	dbCnt, err := db.getRecordCount()
	require.NoError(t, err)
	require.Equal(t, int64(n/2), dbCnt)

	for i := 0; i < n/2; i++ {
		err = db.View(func(tx *Tx) error {
			key := []byte(fmt.Sprintf("key%d", i))
			_, err := tx.Get("bucket1", key)
			if err != ErrKeyNotFound {
				return fmt.Errorf("expected ErrKeyNotFound for key%d, got: %v", i, err)
			}
			return nil
		})
		require.NoError(t, err)
	}

	for i := n / 2; i < n; i++ {
		err = db.View(func(tx *Tx) error {
			key := []byte(fmt.Sprintf("key%d", i))
			val := []byte(fmt.Sprintf("value%d", i))
			got, err := tx.Get("bucket1", key)
			if err != nil {
				return err
			}
			if !bytes.Equal(got, val) {
				return fmt.Errorf("value mismatch for key%d", i)
			}
			return nil
		})
		require.NoError(t, err)
	}
}

func TestMergeWorker_SetMergeInterval(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	mw := db.mergeWorker

	require.Equal(t, 2*time.Hour, mw.GetMergeInterval())

	newInterval := 1 * time.Second
	mw.SetMergeInterval(newInterval)
	require.Equal(t, newInterval, mw.GetMergeInterval())

	mw.SetMergeInterval(0)
	require.Equal(t, time.Duration(0), mw.GetMergeInterval())
}

func TestMergeWorker_ResourceCleanup(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)

	mw := db.mergeWorker

	mw.SetMergeInterval(100 * time.Millisecond)

	time.Sleep(250 * time.Millisecond)

	require.NoError(t, db.Close())

	require.True(t, mw.lifecycle.IsStopped())

	select {
	case <-mw.lifecycle.Context().Done():
	default:
		t.Error("Expected context to be cancelled after Close()")
	}
}

func TestMergeWorker_StopDuringMerge(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		if err := tx.NewBucket(DataStructureBTree, "bucket1"); err != nil {
			return err
		}
		for i := 0; i < 1000; i++ {
			key := []byte("key" + string(rune(i)))
			val := []byte("value" + string(rune(i)))
			if err := tx.Put("bucket1", key, val, 0); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	mergeStarted := make(chan struct{})

	go func() {
		close(mergeStarted)
		_ = db.mergeWorker.TriggerMerge()
	}()

	<-mergeStarted

	for i := 0; i < 100; i++ {
		if db.mergeWorker.IsMerging() {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	start := time.Now()
	require.NoError(t, db.Close())
	elapsed := time.Since(start)

	if elapsed > 10*time.Second {
		t.Errorf("Expected Close to complete within 10s, took %v", elapsed)
	}
}

func TestMergeWorker_ConcurrentStops(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)

	mw := db.mergeWorker

	const numGoroutines = 10
	var wg sync.WaitGroup
	errors := make([]error, numGoroutines)

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			errors[idx] = mw.Stop(5 * time.Second)
		}(i)
	}

	wg.Wait()

	for i, err := range errors {
		if err != nil {
			t.Errorf("Stop call %d failed: %v", i, err)
		}
	}

	require.NoError(t, db.Close())
}

func TestMergeWorker_StopTimeout(t *testing.T) {
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer func() { _ = sm.Close() }()

	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	config := DefaultMergeConfig()
	mw := newMergeWorker(db, sm, config)

	ctx := context.Background()
	require.NoError(t, mw.Start(ctx))

	start := time.Now()
	err = mw.Stop(1 * time.Millisecond)
	elapsed := time.Since(start)

	if elapsed > 1*time.Second {
		t.Errorf("Expected Stop to complete quickly, took %v", elapsed)
	}

	if err != nil {
		t.Logf("Stop returned error (expected for short timeout): %v", err)
	}
}
