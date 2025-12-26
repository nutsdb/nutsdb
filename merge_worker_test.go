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

// TestMergeWorker_Creation tests MergeWorker creation
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

// TestMergeWorker_StartStop tests starting and stopping MergeWorker
func TestMergeWorker_StartStop(t *testing.T) {
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	config := DefaultMergeConfig()
	mw := newMergeWorker(db, sm, config)

	// Test starting
	ctx := context.Background()
	require.NoError(t, mw.Start(ctx))

	// Verify lifecycle is running
	require.True(t, mw.lifecycle.IsRunning())

	// Test stopping
	require.NoError(t, mw.Stop(5*time.Second))

	// Test idempotent stop
	require.NoError(t, mw.Stop(5*time.Second))
}

// TestMergeWorker_TriggerMerge tests manual merge triggering
func TestMergeWorker_TriggerMerge(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create bucket first
	err = db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, "bucket1")
	})
	require.NoError(t, err)

	// Add some data to make merge meaningful
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

	// Trigger merge - it may return ErrDontNeedMerge if not enough files
	err = db.mergeWorker.TriggerMerge()
	// Either success or ErrDontNeedMerge is acceptable
	if err != nil && err != ErrDontNeedMerge {
		t.Fatalf("Unexpected error from TriggerMerge: %v", err)
	}

	// Verify not merging after completion
	require.False(t, db.mergeWorker.IsMerging())
}

// TestMergeWorker_RejectMergeWhenClosing tests merge rejection during closing
func TestMergeWorker_RejectMergeWhenClosing(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)

	// Create bucket first
	err = db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, "bucket1")
	})
	require.NoError(t, err)

	// Add some data
	err = db.Update(func(tx *Tx) error {
		return tx.Put("bucket1", []byte("key1"), []byte("value1"), 0)
	})
	require.NoError(t, err)

	// Test 1: Trigger merge in Open state - should succeed or return ErrDontNeedMerge
	err = db.mergeWorker.TriggerMerge()
	if err != nil && err != ErrDontNeedMerge {
		t.Fatalf("Expected merge to succeed or return ErrDontNeedMerge in Open state, got: %v", err)
	}

	// Test 2: Simulate Closing state by transitioning StatusManager
	// This tests that MergeWorker checks the status before accepting merge requests
	db.statusMgr.transitionTo(StatusClosing)

	// Trigger merge in Closing state - should fail with ErrDBClosed
	err = db.mergeWorker.TriggerMerge()
	require.Equal(t, ErrDBClosed, err, "Expected ErrDBClosed when triggering merge in Closing state")

	// Test 3: Simulate Closed state
	db.statusMgr.transitionTo(StatusClosed)

	// Trigger merge in Closed state - should also fail with ErrDBClosed
	err = db.mergeWorker.TriggerMerge()
	require.Equal(t, ErrDBClosed, err, "Expected ErrDBClosed when triggering merge in Closed state")

	// Clean up: manually stop components since we bypassed normal Close()
	db.mergeWorker.Stop(5 * time.Second)
	db.transactionMgr.Stop(5 * time.Second)
	db.ttlService.Stop(5 * time.Second)
	if db.watchMgr != nil {
		db.watchMgr.Stop(5 * time.Second)
	}
}

// TestMergeWorker_RejectConcurrentMerge tests rejection of concurrent merge requests
func TestMergeWorker_RejectConcurrentMerge(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create bucket first
	err = db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, "bucket1")
	})
	require.NoError(t, err)

	// Add enough data to create multiple files for merge
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

	// Use channel to signal when first merge has actually started
	mergeStarted := make(chan struct{})

	// Start first merge in background
	var wg sync.WaitGroup
	var firstMergeErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Signal that we're about to start merge
		close(mergeStarted)
		firstMergeErr = db.mergeWorker.TriggerMerge()
	}()

	// Wait for first merge to actually start
	<-mergeStarted

	// Ensure the merge flag is set by checking IsMerging in a tight loop
	// This is acceptable as we're verifying the actual state, not bypassing synchronization
	for i := 0; i < 100; i++ {
		if db.mergeWorker.IsMerging() {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	// Try to trigger second merge while first is in progress
	err = db.mergeWorker.TriggerMerge()
	// Should either get ErrIsMerging or succeed if first merge completed
	if err != nil && err != ErrIsMerging && err != ErrDontNeedMerge {
		t.Errorf("Expected ErrIsMerging or ErrDontNeedMerge, got: %v", err)
	}

	wg.Wait()

	// First merge should have completed (success or ErrDontNeedMerge)
	if firstMergeErr != nil && firstMergeErr != ErrDontNeedMerge {
		t.Errorf("First merge failed with unexpected error: %v", firstMergeErr)
	}
}

// TestMergeWorker_ContextCancellation tests context cancellation handling
func TestMergeWorker_ContextCancellation(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)

	// Stop the merge worker (which cancels the context)
	require.NoError(t, db.mergeWorker.Stop(5*time.Second))

	// Try to trigger merge after stop - should fail immediately
	err = db.mergeWorker.TriggerMerge()
	require.Equal(t, ErrDBClosed, err)

	// Stop should be idempotent and complete quickly
	start := time.Now()
	require.NoError(t, db.mergeWorker.Stop(5*time.Second))
	elapsed := time.Since(start)

	if elapsed > 1*time.Second {
		t.Errorf("Expected Stop to complete quickly after already stopped, took %v", elapsed)
	}

	// Close the database
	require.NoError(t, db.Close())
}

// TestMergeWorker_AutoMerge tests automatic merge triggering
func TestMergeWorker_AutoMerge(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	opts.SegmentSize = KB // Use small segment size to trigger merge with small data
	db, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create bucket first
	err = db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, "bucket1")
	})
	require.NoError(t, err)

	// Add enough data to create multiple files and trigger merge
	// With SegmentSize = 1KB, we need more data to fill multiple segments
	n := 1000
	for i := 0; i < n; i++ {
		err = db.Update(func(tx *Tx) error {
			key := []byte(fmt.Sprintf("key%d", i))
			val := []byte(fmt.Sprintf("value%d", i))
			return tx.Put("bucket1", key, val, 0)
		})
		require.NoError(t, err)
	}

	// Delete some data to create dirty entries that need merging
	for i := 0; i < n/2; i++ {
		err = db.Update(func(tx *Tx) error {
			key := []byte(fmt.Sprintf("key%d", i))
			return tx.Delete("bucket1", key)
		})
		require.NoError(t, err)
	}

	// Close and reopen to create multiple data files
	require.NoError(t, db.Close())
	db, err = Open(opts)
	require.NoError(t, err)

	// Enable auto merge with short interval
	db.mergeWorker.SetMergeInterval(200 * time.Millisecond)

	// Wait for auto merge to trigger
	// Auto merge should trigger at least once given the dirty data
	time.Sleep(1 * time.Second)

	// Disable auto merge
	db.mergeWorker.SetMergeInterval(0)

	// Verify that data is still correct after auto merge
	dbCnt, err := db.getRecordCount()
	require.NoError(t, err)
	require.Equal(t, int64(n/2), dbCnt)

	// Check that deleted data is gone
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

	// Check that remaining data exists
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

// TestMergeWorker_SetMergeInterval tests dynamic merge interval changes
func TestMergeWorker_SetMergeInterval(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	mw := db.mergeWorker

	// Initially set to default (2 hours from DefaultOptions)
	require.Equal(t, 2*time.Hour, mw.GetMergeInterval())

	// Enable auto merge with different interval
	newInterval := 1 * time.Second
	mw.SetMergeInterval(newInterval)
	require.Equal(t, newInterval, mw.GetMergeInterval())

	// Disable auto merge
	mw.SetMergeInterval(0)
	require.Equal(t, time.Duration(0), mw.GetMergeInterval())
}

// TestMergeWorker_ResourceCleanup tests resource cleanup on stop
func TestMergeWorker_ResourceCleanup(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)

	mw := db.mergeWorker

	// Enable auto merge
	mw.SetMergeInterval(100 * time.Millisecond)

	// Let it run for a bit
	time.Sleep(250 * time.Millisecond)

	// Close the database (which stops the merge worker)
	require.NoError(t, db.Close())

	// Verify lifecycle is stopped
	require.True(t, mw.lifecycle.IsStopped())

	// Verify context is cancelled
	select {
	case <-mw.lifecycle.Context().Done():
		// Good - context is cancelled
	default:
		t.Error("Expected context to be cancelled after Close()")
	}
}

// TestMergeWorker_StopDuringMerge tests stopping while merge is in progress
func TestMergeWorker_StopDuringMerge(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)

	// Add enough data to make merge take some time
	err = db.Update(func(tx *Tx) error {
		// Create bucket first
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

	// Use channel to signal when merge has actually started
	mergeStarted := make(chan struct{})

	// Start merge in background
	go func() {
		close(mergeStarted)
		_ = db.mergeWorker.TriggerMerge()
	}()

	// Wait for merge goroutine to start
	<-mergeStarted

	// Verify merge is actually running by checking the flag
	for i := 0; i < 100; i++ {
		if db.mergeWorker.IsMerging() {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	// Close database while merge may be in progress
	start := time.Now()
	require.NoError(t, db.Close())
	elapsed := time.Since(start)

	// Close should complete within reasonable time
	if elapsed > 10*time.Second {
		t.Errorf("Expected Close to complete within 10s, took %v", elapsed)
	}
}

// TestMergeWorker_ConcurrentStops tests concurrent Stop calls
func TestMergeWorker_ConcurrentStops(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-test")
	db, err := Open(opts)
	require.NoError(t, err)

	mw := db.mergeWorker

	// Call Stop concurrently from multiple goroutines
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

	// All stops should succeed (idempotent)
	for i, err := range errors {
		if err != nil {
			t.Errorf("Stop call %d failed: %v", i, err)
		}
	}

	// Close the database
	require.NoError(t, db.Close())
}

// TestMergeWorker_StopTimeout tests stop timeout behavior
func TestMergeWorker_StopTimeout(t *testing.T) {
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

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

	// Stop with very short timeout
	start := time.Now()
	err = mw.Stop(1 * time.Millisecond)
	elapsed := time.Since(start)

	// Should timeout or complete quickly
	if elapsed > 1*time.Second {
		t.Errorf("Expected Stop to complete quickly, took %v", elapsed)
	}

	// Error is acceptable for timeout
	if err != nil {
		t.Logf("Stop returned error (expected for short timeout): %v", err)
	}
}
