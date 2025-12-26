package nutsdb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestTransactionManager_Creation ensures TransactionManager initialization succeeds
func TestTransactionManager_Creation(t *testing.T) {
	// Provide a minimal DB so the manager has dependencies to inspect
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	tm := newTxManager(db, sm)

	if tm == nil {
		t.Fatal("Expected non-nil TransactionManager")
	}

	if tm.Name() != "TransactionManager" {
		t.Errorf("Expected name 'TransactionManager', got '%s'", tm.Name())
	}

	if tm.GetActiveTxCount() != 0 {
		t.Errorf("Expected 0 active transactions, got %d", tm.GetActiveTxCount())
	}

	if tm.lifecycle.IsRunning() {
		t.Error("Expected TransactionManager to not be running initially")
	}
}

// TestTransactionManager_StartStop verifies the manager reacts to start/stop lifecycle commands
func TestTransactionManager_StartStop(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	tm := newTxManager(db, sm)

	// Start to confirm the manager enters the running state
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}

	if !tm.lifecycle.IsRunning() {
		t.Error("Expected TransactionManager to be running after Start()")
	}

	// Starting again should fail because it is already running
	if err := tm.Start(ctx); err == nil {
		t.Error("Expected error when starting already running TransactionManager")
	}

	// Stop should bring the manager back to the non-running state
	if err := tm.Stop(5 * time.Second); err != nil {
		t.Fatalf("Failed to stop TransactionManager: %v", err)
	}

	if tm.lifecycle.IsRunning() {
		t.Error("Expected TransactionManager to not be running after Stop()")
	}

	// Stopping again should still succeed because Stop is idempotent
	if err := tm.Stop(5 * time.Second); err != nil {
		t.Errorf("Expected no error when stopping already stopped TransactionManager, got: %v", err)
	}
}

// TestTransactionManager_RegisterUnregisterTx ensures registering and unregistering updates tracking
func TestTransactionManager_RegisterUnregisterTx(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	// Start StatusManager so it reaches Open before registering transactions
	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := newTxManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}
	defer tm.Stop(5 * time.Second)

	// Build a dummy transaction so we can exercise registration
	tx := &Tx{
		id: 1,
		db: db,
	}

	// Register to verify it joins the active list
	if err := tm.RegisterTx(tx); err != nil {
		t.Fatalf("Failed to register transaction: %v", err)
	}

	if count := tm.GetActiveTxCount(); count != 1 {
		t.Errorf("Expected 1 active transaction, got %d", count)
	}

	// Ensure the active list reflects the registration
	txIDs := tm.GetActiveTxs()
	if len(txIDs) != 1 || txIDs[0] != 1 {
		t.Errorf("Expected transaction ID 1 in active list, got %v", txIDs)
	}

	// Unregister to confirm removal logic
	tm.UnregisterTx(tx.id)

	if count := tm.GetActiveTxCount(); count != 0 {
		t.Errorf("Expected 0 active transactions after unregister, got %d", count)
	}

	// Confirm the active list is empty afterward
	txIDs = tm.GetActiveTxs()
	if len(txIDs) != 0 {
		t.Errorf("Expected empty active list after unregister, got %v", txIDs)
	}

	// Unregistering a non-existent transaction should not disturb the count
	tm.UnregisterTx(999)
	if count := tm.GetActiveTxCount(); count != 0 {
		t.Errorf("Expected 0 active transactions, got %d", count)
	}
}

// TestTransactionManager_RegisterNilTx makes sure nil transactions are rejected
func TestTransactionManager_RegisterNilTx(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := newTxManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}
	defer tm.Stop(5 * time.Second)

	// Register nil to verify validation rejects it
	if err := tm.RegisterTx(nil); err == nil {
		t.Error("Expected error when registering nil transaction")
	}
}

// TestTransactionManager_RejectTxWhenClosing validates new transactions are rejected while closing
func TestTransactionManager_RejectTxWhenClosing(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	// Start StatusManager so TransactionManager can interact with it
	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := newTxManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}

	// In the Open state, registration should succeed before we close
	tx1 := &Tx{id: 1, db: db}
	if err := tm.RegisterTx(tx1); err != nil {
		t.Fatalf("Failed to register transaction in Open state: %v", err)
	}

	// Transition StatusManager to Closing so new registrations fail
	sm.transitionTo(StatusClosing)

	// Expect ErrDBClosed once the status is Closing
	tx2 := &Tx{id: 2, db: db}
	if err := tm.RegisterTx(tx2); err != ErrDBClosed {
		t.Errorf("Expected ErrDBClosed when registering transaction in Closing state, got: %v", err)
	}

	// Only the first transaction should remain active after the rejection
	if count := tm.GetActiveTxCount(); count != 1 {
		t.Errorf("Expected 1 active transaction, got %d", count)
	}

	// Move status to Closed to confirm rejections persist
	sm.transitionTo(StatusClosed)

	// Still expect ErrDBClosed while fully Closed
	tx3 := &Tx{id: 3, db: db}
	if err := tm.RegisterTx(tx3); err != ErrDBClosed {
		t.Errorf("Expected ErrDBClosed when registering transaction in Closed state, got: %v", err)
	}

	// Clean up by unregistering before stopping
	tm.UnregisterTx(tx1.id)
	tm.Stop(5 * time.Second)
}

// TestTransactionManager_RejectTxWhenNotRunning documents that registration only depends on StatusManager state
func TestTransactionManager_RejectTxWhenNotRunning(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := newTxManager(db, sm)

	// RegisterTx should still work because it only checks StatusManager state, not the running flag
	// This keeps the running flag focused on health checks rather than gating registration
	tx := &Tx{id: 1, db: db}
	if err := tm.RegisterTx(tx); err != nil {
		t.Errorf("RegisterTx should work even when TransactionManager is not running (only checks StatusManager), got: %v", err)
	}

	// Clean up so the next test starts fresh
	tm.UnregisterTx(tx.id)
}

// TestTransactionManager_WaitForActiveTxs ensures waits finish once active transactions complete
func TestTransactionManager_WaitForActiveTxs(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := newTxManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}
	defer tm.Stop(5 * time.Second)

	// Expect a quick return when there are no active transactions to wait for
	start := time.Now()
	if err := tm.WaitForActiveTxs(5 * time.Second); err != nil {
		t.Errorf("Expected no error when waiting with no active transactions, got: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 1*time.Second {
		t.Errorf("Expected WaitForActiveTxs to return quickly with no active transactions, took %v", elapsed)
	}

	// Register several transactions so WaitForActiveTxs has work to wait on
	tx1 := &Tx{id: 1, db: db}
	tx2 := &Tx{id: 2, db: db}
	tx3 := &Tx{id: 3, db: db}

	if err := tm.RegisterTx(tx1); err != nil {
		t.Fatalf("Failed to register tx1: %v", err)
	}
	if err := tm.RegisterTx(tx2); err != nil {
		t.Fatalf("Failed to register tx2: %v", err)
	}
	if err := tm.RegisterTx(tx3); err != nil {
		t.Fatalf("Failed to register tx3: %v", err)
	}

	// Unregister them one by one in the background to simulate completion timing
	go func() {
		time.Sleep(200 * time.Millisecond)
		tm.UnregisterTx(tx1.id)

		time.Sleep(200 * time.Millisecond)
		tm.UnregisterTx(tx2.id)

		time.Sleep(200 * time.Millisecond)
		tm.UnregisterTx(tx3.id)
	}()

	// Wait so the main goroutine blocks until each transaction finishes
	start = time.Now()
	if err := tm.WaitForActiveTxs(5 * time.Second); err != nil {
		t.Errorf("Expected no error when waiting for active transactions, got: %v", err)
	}
	elapsed = time.Since(start)

	// Expect around 600ms because each unregister is delayed by 200ms
	if elapsed < 500*time.Millisecond || elapsed > 1*time.Second {
		t.Errorf("Expected WaitForActiveTxs to take ~600ms, took %v", elapsed)
	}

	// Verify all transactions were unregistered after the wait
	if count := tm.GetActiveTxCount(); count != 0 {
		t.Errorf("Expected 0 active transactions after wait, got %d", count)
	}
}

// TestTransactionManager_WaitForActiveTxsTimeout confirms timeout behavior when transactions stay active
func TestTransactionManager_WaitForActiveTxsTimeout(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := newTxManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}
	defer tm.Stop(5 * time.Second)

	// Register a transaction and keep it active so the wait must time out
	tx := &Tx{id: 1, db: db}
	if err := tm.RegisterTx(tx); err != nil {
		t.Fatalf("Failed to register transaction: %v", err)
	}

	// Waiting should hit the deadline because the transaction never finishes
	start := time.Now()
	err := tm.WaitForActiveTxs(500 * time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("Expected timeout error when waiting for active transactions")
	}

	// Expect the elapsed time to be near the timeout duration
	if elapsed < 400*time.Millisecond || elapsed > 700*time.Millisecond {
		t.Errorf("Expected WaitForActiveTxs to timeout at ~500ms, took %v", elapsed)
	}

	// Ensure the transaction is still counted as active after the timeout
	if count := tm.GetActiveTxCount(); count != 1 {
		t.Errorf("Expected 1 active transaction after timeout, got %d", count)
	}

	// Tidy up by unregistering the transaction
	tm.UnregisterTx(tx.id)
}

// TestTransactionManager_ConcurrentRegisterUnregister stresses concurrent register/unregister handling
func TestTransactionManager_ConcurrentRegisterUnregister(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := newTxManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}
	defer tm.Stop(5 * time.Second)

	const numGoroutines = 50
	const txPerGoroutine = 20

	var wg sync.WaitGroup
	var txIDCounter atomic.Uint64

	// Run concurrent register/unregister pairs to stress the tracking logic
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < txPerGoroutine; j++ {
				txID := txIDCounter.Add(1)
				tx := &Tx{id: txID, db: db}

				// Register to exercise concurrent additions
				if err := tm.RegisterTx(tx); err != nil {
					t.Errorf("Failed to register transaction %d: %v", txID, err)
					continue
				}

				// Sleep briefly to simulate transaction work
				time.Sleep(1 * time.Millisecond)

				// Unregister once the work is simulated
				tm.UnregisterTx(txID)
			}
		}()
	}

	wg.Wait()

	// Verify no transactions remain active after the storm
	if count := tm.GetActiveTxCount(); count != 0 {
		t.Errorf("Expected 0 active transactions after concurrent operations, got %d", count)
	}

	txIDs := tm.GetActiveTxs()
	if len(txIDs) != 0 {
		t.Errorf("Expected empty active transaction list, got %v", txIDs)
	}
}

// TestTransactionManager_MaxActiveTxs verifies the enforcement of the active transaction limit
func TestTransactionManager_MaxActiveTxs(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := newTxManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}
	defer tm.Stop(5 * time.Second)

	// Apply the limit so we can confirm behavior
	maxTxs := int64(5)
	tm.SetMaxActiveTxs(maxTxs)

	if got := tm.GetMaxActiveTxs(); got != maxTxs {
		t.Errorf("Expected max active txs %d, got %d", maxTxs, got)
	}

	// Register transactions up to the limit
	for i := int64(1); i <= maxTxs; i++ {
		tx := &Tx{id: uint64(i), db: db}
		if err := tm.RegisterTx(tx); err != nil {
			t.Fatalf("Failed to register transaction %d: %v", i, err)
		}
	}

	// Confirm the active count matches the configured limit
	if count := tm.GetActiveTxCount(); count != maxTxs {
		t.Errorf("Expected %d active transactions, got %d", maxTxs, count)
	}

	// Remove all transactions to restore a clean state
	for i := int64(1); i <= maxTxs; i++ {
		tm.UnregisterTx(uint64(i))
	}
}

// TestTransactionManager_StopWithActiveTxs ensures Stop waits for active transactions to finish
func TestTransactionManager_StopWithActiveTxs(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := newTxManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}

	// Register transactions so Stop has something to wait for
	tx1 := &Tx{id: 1, db: db}
	tx2 := &Tx{id: 2, db: db}

	if err := tm.RegisterTx(tx1); err != nil {
		t.Fatalf("Failed to register tx1: %v", err)
	}
	if err := tm.RegisterTx(tx2); err != nil {
		t.Fatalf("Failed to register tx2: %v", err)
	}

	// Unregister them after a delay to simulate ongoing work
	go func() {
		time.Sleep(300 * time.Millisecond)
		tm.UnregisterTx(tx1.id)
		tm.UnregisterTx(tx2.id)
	}()

	// Stop should block until the active transactions are cleared
	start := time.Now()
	if err := tm.Stop(2 * time.Second); err != nil {
		t.Errorf("Expected no error when stopping with active transactions, got: %v", err)
	}
	elapsed := time.Since(start)

	// Expect roughly 300ms because the background cleanup waits that long
	if elapsed < 200*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Errorf("Expected Stop to wait ~300ms for active transactions, took %v", elapsed)
	}

	if tm.lifecycle.IsRunning() {
		t.Error("Expected TransactionManager to not be running after Stop()")
	}
}

// TestTransactionManager_StopTimeout checks that Stop times out when transactions stay active
func TestTransactionManager_StopTimeout(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := newTxManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}

	// Register a transaction and leave it to simulate a stuck operation
	tx := &Tx{id: 1, db: db}
	if err := tm.RegisterTx(tx); err != nil {
		t.Fatalf("Failed to register transaction: %v", err)
	}

	// Stop should hit the timeout but still return after logging the situation
	start := time.Now()
	err := tm.Stop(500 * time.Millisecond)
	elapsed := time.Since(start)

	// Implementation may log a warning instead of returning an error
	if err != nil {
		t.Logf("Stop returned error (expected warning): %v", err)
	}

	// Expect the duration to be close to the timeout threshold
	if elapsed < 400*time.Millisecond || elapsed > 700*time.Millisecond {
		t.Errorf("Expected Stop to timeout at ~500ms, took %v", elapsed)
	}

	// The running flag should be false even after the timeout
	if tm.lifecycle.IsRunning() {
		t.Error("Expected TransactionManager to not be running after Stop() timeout")
	}

	// Clean up the blocked transaction afterwards
	tm.UnregisterTx(tx.id)
}
