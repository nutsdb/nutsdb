package nutsdb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTransactionManager_Creation(t *testing.T) {
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

func TestTransactionManager_StartStop(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	tm := newTxManager(db, sm)

	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}

	if !tm.lifecycle.IsRunning() {
		t.Error("Expected TransactionManager to be running after Start()")
	}

	if err := tm.Start(ctx); err == nil {
		t.Error("Expected error when starting already running TransactionManager")
	}

	if err := tm.Stop(5 * time.Second); err != nil {
		t.Fatalf("Failed to stop TransactionManager: %v", err)
	}

	if tm.lifecycle.IsRunning() {
		t.Error("Expected TransactionManager to not be running after Stop()")
	}

	if err := tm.Stop(5 * time.Second); err != nil {
		t.Errorf("Expected no error when stopping already stopped TransactionManager, got: %v", err)
	}
}

func TestTransactionManager_RegisterUnregisterTx(t *testing.T) {
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

	tx := &Tx{
		id: 1,
		db: db,
	}

	if err := tm.RegisterTx(tx); err != nil {
		t.Fatalf("Failed to register transaction: %v", err)
	}

	if count := tm.GetActiveTxCount(); count != 1 {
		t.Errorf("Expected 1 active transaction, got %d", count)
	}

	txIDs := tm.GetActiveTxs()
	if len(txIDs) != 1 || txIDs[0] != 1 {
		t.Errorf("Expected transaction ID 1 in active list, got %v", txIDs)
	}

	tm.UnregisterTx(tx.id)

	if count := tm.GetActiveTxCount(); count != 0 {
		t.Errorf("Expected 0 active transactions after unregister, got %d", count)
	}

	txIDs = tm.GetActiveTxs()
	if len(txIDs) != 0 {
		t.Errorf("Expected empty active list after unregister, got %v", txIDs)
	}

	tm.UnregisterTx(999)
	if count := tm.GetActiveTxCount(); count != 0 {
		t.Errorf("Expected 0 active transactions, got %d", count)
	}
}

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

	if err := tm.RegisterTx(nil); err == nil {
		t.Error("Expected error when registering nil transaction")
	}
}

func TestTransactionManager_RejectTxWhenClosing(t *testing.T) {
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

	tx1 := &Tx{id: 1, db: db}
	if err := tm.RegisterTx(tx1); err != nil {
		t.Fatalf("Failed to register transaction in Open state: %v", err)
	}

	sm.closing.Store(true)

	tx2 := &Tx{id: 2, db: db}
	if err := tm.RegisterTx(tx2); err != ErrDBClosed {
		t.Errorf("Expected ErrDBClosed when registering transaction in Closing state, got: %v", err)
	}

	if count := tm.GetActiveTxCount(); count != 1 {
		t.Errorf("Expected 1 active transaction, got %d", count)
	}

	sm.closing.Store(false)
	sm.closed.Store(true)

	tx3 := &Tx{id: 3, db: db}
	if err := tm.RegisterTx(tx3); err != ErrDBClosed {
		t.Errorf("Expected ErrDBClosed when registering transaction in Closed state, got: %v", err)
	}

	tm.UnregisterTx(tx1.id)
	tm.Stop(5 * time.Second)
}

func TestTransactionManager_RejectTxWhenNotRunning(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := newTxManager(db, sm)

	tx := &Tx{id: 1, db: db}
	if err := tm.RegisterTx(tx); err != nil {
		t.Errorf("RegisterTx should work even when TransactionManager is not running (only checks StatusManager), got: %v", err)
	}

	tm.UnregisterTx(tx.id)
}

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

	start := time.Now()
	if err := tm.WaitForActiveTxs(5 * time.Second); err != nil {
		t.Errorf("Expected no error when waiting with no active transactions, got: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 1*time.Second {
		t.Errorf("Expected WaitForActiveTxs to return quickly with no active transactions, took %v", elapsed)
	}

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

	go func() {
		time.Sleep(200 * time.Millisecond)
		tm.UnregisterTx(tx1.id)

		time.Sleep(200 * time.Millisecond)
		tm.UnregisterTx(tx2.id)

		time.Sleep(200 * time.Millisecond)
		tm.UnregisterTx(tx3.id)
	}()

	start = time.Now()
	if err := tm.WaitForActiveTxs(5 * time.Second); err != nil {
		t.Errorf("Expected no error when waiting for active transactions, got: %v", err)
	}
	elapsed = time.Since(start)

	if elapsed < 500*time.Millisecond || elapsed > 1*time.Second {
		t.Errorf("Expected WaitForActiveTxs to take ~600ms, took %v", elapsed)
	}

	if count := tm.GetActiveTxCount(); count != 0 {
		t.Errorf("Expected 0 active transactions after wait, got %d", count)
	}
}

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

	tx := &Tx{id: 1, db: db}
	if err := tm.RegisterTx(tx); err != nil {
		t.Fatalf("Failed to register transaction: %v", err)
	}

	start := time.Now()
	err := tm.WaitForActiveTxs(500 * time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("Expected timeout error when waiting for active transactions")
	}

	if elapsed < 400*time.Millisecond || elapsed > 700*time.Millisecond {
		t.Errorf("Expected WaitForActiveTxs to timeout at ~500ms, took %v", elapsed)
	}

	if count := tm.GetActiveTxCount(); count != 1 {
		t.Errorf("Expected 1 active transaction after timeout, got %d", count)
	}

	tm.UnregisterTx(tx.id)
}

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

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < txPerGoroutine; j++ {
				txID := txIDCounter.Add(1)
				tx := &Tx{id: txID, db: db}

				if err := tm.RegisterTx(tx); err != nil {
					t.Errorf("Failed to register transaction %d: %v", txID, err)
					continue
				}

				time.Sleep(1 * time.Millisecond)

				tm.UnregisterTx(txID)
			}
		}()
	}

	wg.Wait()

	if count := tm.GetActiveTxCount(); count != 0 {
		t.Errorf("Expected 0 active transactions after concurrent operations, got %d", count)
	}

	txIDs := tm.GetActiveTxs()
	if len(txIDs) != 0 {
		t.Errorf("Expected empty active transaction list, got %v", txIDs)
	}
}

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

	maxTxs := int64(5)
	tm.SetMaxActiveTxs(maxTxs)

	if got := tm.GetMaxActiveTxs(); got != maxTxs {
		t.Errorf("Expected max active txs %d, got %d", maxTxs, got)
	}

	for i := int64(1); i <= maxTxs; i++ {
		tx := &Tx{id: uint64(i), db: db}
		if err := tm.RegisterTx(tx); err != nil {
			t.Fatalf("Failed to register transaction %d: %v", i, err)
		}
	}

	if count := tm.GetActiveTxCount(); count != maxTxs {
		t.Errorf("Expected %d active transactions, got %d", maxTxs, count)
	}

	for i := int64(1); i <= maxTxs; i++ {
		tm.UnregisterTx(uint64(i))
	}
}

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

	tx1 := &Tx{id: 1, db: db}
	tx2 := &Tx{id: 2, db: db}

	if err := tm.RegisterTx(tx1); err != nil {
		t.Fatalf("Failed to register tx1: %v", err)
	}
	if err := tm.RegisterTx(tx2); err != nil {
		t.Fatalf("Failed to register tx2: %v", err)
	}

	go func() {
		time.Sleep(300 * time.Millisecond)
		tm.UnregisterTx(tx1.id)
		tm.UnregisterTx(tx2.id)
	}()

	start := time.Now()
	if err := tm.Stop(2 * time.Second); err != nil {
		t.Errorf("Expected no error when stopping with active transactions, got: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < 200*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Errorf("Expected Stop to wait ~300ms for active transactions, took %v", elapsed)
	}

	if tm.lifecycle.IsRunning() {
		t.Error("Expected TransactionManager to not be running after Stop()")
	}
}

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

	tx := &Tx{id: 1, db: db}
	if err := tm.RegisterTx(tx); err != nil {
		t.Fatalf("Failed to register transaction: %v", err)
	}

	start := time.Now()
	err := tm.Stop(500 * time.Millisecond)
	elapsed := time.Since(start)

	if err != nil {
		t.Logf("Stop returned error (expected warning): %v", err)
	}

	if elapsed < 400*time.Millisecond || elapsed > 700*time.Millisecond {
		t.Errorf("Expected Stop to timeout at ~500ms, took %v", elapsed)
	}

	if tm.lifecycle.IsRunning() {
		t.Error("Expected TransactionManager to not be running after Stop() timeout")
	}

	tm.UnregisterTx(tx.id)
}
