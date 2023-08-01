package nutsdb

import (
    "errors"
    "fmt"
    "sync"
    "sync/atomic"
)

var (
    // ErrCommitAfterFinish indicates that write batch commit was called after
    // finish
    ErrCommitAfterFinish = errors.New("Batch commit not permitted after finish")
)

// WriteBatch holds the necessary info to perform batched writes.
type WriteBatch struct {
    sync.Mutex
    txn      *Tx
    db       *DB
    throttle *Throttle
    err      atomic.Value
    finished bool
}

func (db *DB) NewWriteBatch() (*WriteBatch, error) {
    wb := &WriteBatch{
        db:       db,
        throttle: NewThrottle(16),
    }

    var err error
    wb.txn, err = newTx(db, true)
    return wb, err
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
    wb.finished = true
    if err := wb.throttle.Finish(); err != nil {
        return fmt.Errorf("WatchBatch.Cancel error while finishing: %v", err)
    }
    wb.txn.setStatusClosed()
    return nil
}

func (wb *WriteBatch) Put(bucket string, key, value []byte, ttl uint32) error {
    wb.Lock()
    defer wb.Unlock()

    err := wb.txn.Put(bucket, key, value, ttl)
    if err != nil {
        return err
    }

    if nil == wb.txn.checkSize() {
        return err
    }

    if cerr := wb.commit(); cerr != nil {
        return cerr
    }

    return err
}

// func (tx *Tx) Delete(bucket string, key []byte) error
func (wb *WriteBatch) Delete(bucket string, key []byte) error {
    wb.Lock()
    defer wb.Unlock()

    err := wb.txn.Delete(bucket, key)
    if err != nil {
        return err
    }

    if nil == wb.txn.checkSize() {
        return err
    }

    if cerr := wb.commit(); cerr != nil {
        return cerr
    }

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

    wb.txn.CommitWith(wb.callback)

    // new a new tx
    var err error
    wb.txn, err = newTx(wb.db, true)
    if err != nil {
        return err
    }

    return wb.Error()
}

func (wb *WriteBatch) callback(err error) {
    // sync.WaitGroup is thread-safe, so it doesn't need to be run inside wb.Lock.
    defer wb.throttle.Done(err)
    if err == nil {
        return
    }
    if err := wb.Error(); err != nil {
        return
    }

    wb.err.Store(err)
    return
}

func (wb *WriteBatch) Flush() error {
    wb.Lock()
    err := wb.commit()
    if err != nil {
        wb.Unlock()
        return err
    }
    wb.finished = true
    wb.txn.setStatusClosed()
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
    var err error
    wb.finished = false
    //wb.err.Store(err)
    wb.txn, err = newTx(wb.db, true)
    wb.throttle = NewThrottle(16)
    return err
}

// Error returns any errors encountered so far. No commits would be run once an error is detected.
func (wb *WriteBatch) Error() error {
    // If the interface conversion fails, the err will be nil.
    err, _ := wb.err.Load().(error)
    return err
}
