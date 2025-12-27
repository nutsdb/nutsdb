package nutsdb

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// ErrCommitAfterFinish indicates that write batch commit was called after
var ErrCommitAfterFinish = errors.New("batch commit not permitted after finish")

const (
	DefaultThrottleSize = 16
)

// WriteBatch holds the necessary info to perform batched writes.
type WriteBatch struct {
	sync.Mutex
	tx       *Tx
	txID     uint64 // track tx ID for proper cleanup with TransactionManager
	db       *DB
	throttle *Throttle
	err      atomic.Value
	finished bool
}

func (db *DB) NewWriteBatch() (*WriteBatch, error) {
	wb := &WriteBatch{
		db:       db,
		throttle: NewThrottle(DefaultThrottleSize),
	}

	// Use TransactionManager to create and register the transaction
	// needLock=false because Put()/Delete() will acquire lock before operations
	tx, err := db.transactionMgr.BeginTx(true, false)
	if err != nil {
		return nil, err
	}
	wb.tx = tx
	wb.txID = tx.id
	return wb, nil
}

// SetMaxPendingTxns sets a limit on maximum number of pending transactions while writing batches.
// This function should be called before using WriteBatch. Default value of MaxPendingTxns is
// 16 to minimise memory usage.
func (wb *WriteBatch) SetMaxPendingTxns(max int) {
	wb.throttle = NewThrottle(max)
}

func (wb *WriteBatch) Cancel() error {
	wb.Lock()
	defer wb.Unlock()

	if wb.finished {
		return nil
	}

	wb.finished = true

	// Unregister and close the transaction
	if wb.tx != nil {
		wb.db.transactionMgr.UnregisterTx(wb.txID)
		wb.tx.setStatusClosed()
	}

	if err := wb.throttle.Finish(); err != nil {
		return fmt.Errorf("WriteBatch.Cancel error while finishing: %v", err)
	}

	return nil
}

func (wb *WriteBatch) Put(bucket string, key, value []byte, ttl uint32) error {
	wb.Lock()
	defer wb.Unlock()

	wb.tx.lock()
	err := wb.tx.Put(bucket, key, value, ttl)
	if err != nil {
		wb.tx.unlock()
		return err
	}

	// Check if batch is full (checkSize returns ErrTxnTooBig when full)
	if err := wb.tx.checkSize(); err != nil {
		// Batch is full, commit now
		wb.tx.unlock()
		return wb.commit()
	}
	// Batch not full, write is pending, unlock and return
	wb.tx.unlock()
	return nil
}

// func (tx *Tx) Delete(bucket string, key []byte) error
func (wb *WriteBatch) Delete(bucket string, key []byte) error {
	wb.Lock()
	defer wb.Unlock()

	wb.tx.lock()
	err := wb.tx.Delete(bucket, key)
	if err != nil {
		wb.tx.unlock()
		return err
	}

	// Check if batch is full (checkSize returns ErrTxnTooBig when full)
	if err := wb.tx.checkSize(); err != nil {
		// Batch is full, commit now
		wb.tx.unlock()
		return wb.commit()
	}
	// Batch not full, write is pending, unlock and return
	wb.tx.unlock()
	return nil
}

func (wb *WriteBatch) commit() error {
	if err := wb.Error(); err != nil {
		return err
	}

	if wb.finished {
		return ErrCommitAfterFinish
	}

	if err := wb.throttle.Do(); err != nil {
		wb.err.Store(err)
		return err
	}

	// Record the current tx ID before async commit
	committingTxID := wb.txID

	// Async commit, callback will run in goroutine
	wb.tx.CommitWith(func(cbErr error) {
		defer wb.throttle.Done(cbErr)
		if cbErr != nil {
			wb.err.Store(cbErr)
		}
	})

	// Create new tx for next batch operations via TransactionManager
	// needLock=false because Put()/Delete() will acquire lock before operations
	var err error
	wb.tx, err = wb.db.transactionMgr.BeginTx(true, false)
	if err != nil {
		// Even if we cannot open a new transaction (e.g., during shutdown),
		// ensure the committing transaction is unregistered so shutdown logic
		// does not wait on it forever.
		wb.db.transactionMgr.UnregisterTx(committingTxID)
		return err
	}
	wb.txID = wb.tx.id

	// Unregister the committed transaction
	wb.db.transactionMgr.UnregisterTx(committingTxID)

	return wb.Error()
}

func (wb *WriteBatch) Flush() error {
	wb.Lock()
	err := wb.commit()
	if err != nil {
		wb.Unlock()
		return err
	}
	wb.finished = true
	// Unregister and close the final transaction
	wb.db.transactionMgr.UnregisterTx(wb.txID)
	wb.tx.setStatusClosed()
	wb.Unlock()
	if err := wb.throttle.Finish(); err != nil {
		if wb.Error() != nil {
			return fmt.Errorf("wb.err: %s err: %s", wb.Error(), err)
		}
		return err
	}

	return wb.Error()
}

func (wb *WriteBatch) Reset() error {
	wb.Lock()
	defer wb.Unlock()

	wb.finished = false

	// Unregister old transaction
	if wb.tx != nil {
		wb.db.transactionMgr.UnregisterTx(wb.txID)
	}

	// Create new transaction via TransactionManager
	// needLock=false because Put()/Delete() will acquire lock before operations
	var err error
	wb.tx, err = wb.db.transactionMgr.BeginTx(true, false)
	if err != nil {
		return err
	}
	wb.txID = wb.tx.id
	wb.throttle = NewThrottle(DefaultThrottleSize)
	return err
}

// Error returns any errors encountered so far. No commits would be run once an error is detected.
func (wb *WriteBatch) Error() error {
	// If the interface conversion fails, the err will be nil.
	err, _ := wb.err.Load().(error)
	return err
}
