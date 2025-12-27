package nutsdb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
)

type txManager struct {
	lifecycle core.ComponentLifecycle

	db            *DB
	statusManager *StatusManager

	activeTxs     sync.Map
	activeTxCount atomic.Int64

	maxActiveTxs int64
	configMu     sync.RWMutex
}

func newTxManager(db *DB, sm *StatusManager) *txManager {
	return &txManager{
		db:            db,
		statusManager: sm,
		maxActiveTxs:  0,
	}
}

func (tm *txManager) Name() string {
	return "TransactionManager"
}

func (tm *txManager) Start(ctx context.Context) error {
	if err := tm.lifecycle.Start(ctx); err != nil {
		return err
	}

	tm.activeTxCount.Store(0)

	return nil
}

func (tm *txManager) Stop(timeout time.Duration) error {
	if err := tm.WaitForActiveTxs(timeout); err != nil {
		tm.forceAbortActiveTxs()
	}

	return tm.lifecycle.Stop(timeout)
}

// acquireLock is false when WriteBatch already holds db.mu.
func (tm *txManager) BeginTx(writable bool, acquireLock bool) (*Tx, error) {
	if !tm.lifecycle.IsRunning() {
		return nil, ErrDBClosed
	}

	if tm.statusManager.isClosingOrClosed() {
		return nil, ErrDBClosed
	}

	if tm.maxActiveTxs > 0 && tm.activeTxCount.Load() >= tm.maxActiveTxs {
		return nil, fmt.Errorf("too many active transactions: %d (max: %d)", tm.activeTxCount.Load(), tm.maxActiveTxs)
	}

	tx, err := newTx(tm.db, writable)
	if err != nil {
		return nil, err
	}

	if acquireLock {
		tx.lock()
	} else {
		tx.lockAcquired.Store(false)
	}

	if err := tm.RegisterTx(tx); err != nil {
		return nil, err
	}

	return tx, nil
}

func (tm *txManager) RegisterTx(tx *Tx) error {
	if tx == nil {
		return fmt.Errorf("cannot register nil transaction")
	}

	if tm.statusManager.isClosingOrClosed() {
		return ErrDBClosed
	}

	tm.activeTxs.Store(tx.id, tx)
	tm.activeTxCount.Add(1)

	return nil
}

func (tm *txManager) UnregisterTx(txID uint64) {
	if _, loaded := tm.activeTxs.LoadAndDelete(txID); loaded {
		tm.activeTxCount.Add(-1)
	}
}

func (tm *txManager) GetActiveTxCount() int64 {
	return tm.activeTxCount.Load()
}

func (tm *txManager) WaitForActiveTxs(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		count := tm.activeTxCount.Load()
		if count == 0 {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for %d active transactions to complete", tm.activeTxCount.Load())
		}

		select {
		case <-ticker.C:
		case <-time.After(time.Until(deadline)):
			return fmt.Errorf("timeout waiting for %d active transactions to complete", tm.activeTxCount.Load())
		}
	}
}

// 0 means unlimited.
func (tm *txManager) SetMaxActiveTxs(max int64) {
	tm.configMu.Lock()
	defer tm.configMu.Unlock()
	tm.maxActiveTxs = max
}

func (tm *txManager) GetMaxActiveTxs() int64 {
	tm.configMu.RLock()
	defer tm.configMu.RUnlock()
	return tm.maxActiveTxs
}

func (tm *txManager) GetActiveTxs() []uint64 {
	txIDs := make([]uint64, 0)
	tm.activeTxs.Range(func(key, value any) bool {
		txIDs = append(txIDs, key.(uint64))
		return true
	})
	return txIDs
}

// forceAbortActiveTxs unregisters and closes all active transactions.
// Used during shutdown when transactions fail to finish within the timeout.
func (tm *txManager) forceAbortActiveTxs() {
	tm.activeTxs.Range(func(key, value any) bool {
		tx := value.(*Tx)
		tx.setStatusClosed()
		tm.UnregisterTx(tx.id)
		return true
	})
}
