// Copyright 2019 The nutsdb Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nutsdb

import (
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/bwmarrin/snowflake"
)

var (
	// ErrKeyAndValSize is returned when given key and value size is too big.
	ErrKeyAndValSize = errors.New("key and value size too big")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx is closed")

	// ErrTxNotWritable is returned when performing a write operation on
	// a read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrKeyEmpty is returned if an empty key is passed on an update function.
	ErrKeyEmpty = errors.New("key cannot be empty")

	// ErrRangeScan is returned when range scanning not found the result
	ErrRangeScan = errors.New("range scans not found")

	// ErrPrefixScan is returned when prefix scanning not found the result
	ErrPrefixScan = errors.New("prefix scans not found")

	// ErrNotFoundKey is returned when key not found int the bucket on an view function.
	ErrNotFoundKey = errors.New("key not found in the bucket")
)

// Tx represents a transaction.
type Tx struct {
	id            uint64
	db            *DB
	writable      bool
	pendingWrites []*Entry
}

// Begin opens a new transaction.
// Multiple read-only transactions can be opened at the same time but there can
// only be one read/write transaction at a time. Attempting to open a read/write
// transactions while another one is in progress will result in blocking until
// the current read/write transaction is completed.
// All transactions must be closed by calling Commit() or Rollback() when done.
func (db *DB) Begin(writable bool) (tx *Tx, err error) {
	tx, err = newTx(db, writable)
	if err != nil {
		return nil, err
	}

	tx.lock()

	if db.closed {
		tx.unlock()
		return nil, ErrDBClosed
	}

	return
}

// newTx returns a newly initialized Tx object at given writable.
func newTx(db *DB, writable bool) (tx *Tx, err error) {
	var txId uint64

	tx = &Tx{
		db:            db,
		writable:      writable,
		pendingWrites: []*Entry{},
	}

	txId, err = tx.getTxId()
	if err != nil {
		return nil, err
	}

	tx.id = txId
	if err != nil {
		return nil, err
	}

	return
}

// getTxId returns the tx id.
func (tx *Tx) getTxId() (id uint64, err error) {
	node, err := snowflake.NewNode(tx.db.opt.NodeNum)
	if err != nil {
		return 0, err
	}

	id = uint64(node.Generate().Int64())
	return
}

// Commit commits the transaction, following these steps:
//
// 1. check the length of pendingWrites.If there are no writes, return immediately.
//
// 2. check if the ActiveFile has not enough space to store entry. if not, call rotateActiveFile function.
//
// 3. write pendingWrites to disk, if a non-nil error,return the error.
//
// 4. build Hint index.
//
// 5. Unlock the database and clear the db field.
func (tx *Tx) Commit() error {
	var e *Entry

	if tx.db == nil {
		return ErrDBClosed
	}

	writesLen := len(tx.pendingWrites)

	if writesLen == 0 {
		tx.unlock()
		tx.db = nil
		return nil
	}

	for i := 0; i < writesLen; i++ {
		entry := tx.pendingWrites[i]
		entrySize := entry.Size()
		if entrySize > tx.db.opt.SegmentSize {
			return ErrKeyAndValSize
		}

		if tx.db.ActiveFile.ActualSize+entrySize > tx.db.opt.SegmentSize {
			if err := tx.rotateActiveFile(); err != nil {
				return err
			}
		}

		if i == writesLen-1 {
			entry.Meta.status = Committed
		}

		off := tx.db.ActiveFile.writeOff
		if _, err := tx.db.ActiveFile.WriteAt(entry.Encode(), off); err != nil {
			return err
		}

		tx.db.ActiveFile.ActualSize += entrySize
		tx.db.ActiveFile.writeOff += entrySize

		if tx.db.opt.EntryIdxMode == HintAndRAMIdxMode {
			entry.Meta.status = Committed
			e = entry
		} else {
			e = nil
		}

		countFlag := CountFlagEnabled
		if tx.db.isMerging {
			countFlag = CountFlagDisabled
		}
		bucket := string(entry.Meta.bucket)
		if _, ok := tx.db.HintIdx[bucket]; !ok {
			tx.db.HintIdx[bucket] = NewTree()
		}
		_ = tx.db.HintIdx[bucket].Insert(entry.Key, e, &Hint{
			fileId:  tx.db.ActiveFile.fileId,
			key:     entry.Key,
			meta:    entry.Meta,
			dataPos: uint64(off),
		}, countFlag)
		tx.db.KeyCount++
	}

	tx.unlock()

	tx.db = nil

	return nil
}

// rotateActiveFile rotates log file when active file is not enough space to store the entry.
func (tx *Tx) rotateActiveFile() error {
	var err error
	tx.db.MaxFileId++

	if err := tx.db.ActiveFile.m.Flush(syscall.SYS_SYNC); err != nil {
		return err
	}

	if err := tx.db.ActiveFile.m.Unmap(); err != nil {
		return err
	}

	path := tx.db.getDataPath(tx.db.MaxFileId)
	tx.db.ActiveFile, err = NewDataFile(path, tx.db.opt.SegmentSize)
	if err != nil {
		return err
	}

	tx.db.ActiveFile.fileId = tx.db.MaxFileId

	return nil
}

// Rollback closes the transaction.
func (tx *Tx) Rollback() error {
	if tx.db == nil {
		return ErrDBClosed
	}

	tx.unlock()

	tx.db = nil

	return nil
}

// lock locks the database based on the transaction type.
func (tx *Tx) lock() {
	if tx.writable {
		tx.db.mu.Lock()
	} else {
		tx.db.mu.RLock()
	}
}

// unlock unlocks the database based on the transaction type.
func (tx *Tx) unlock() {
	if tx.writable {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
}

// Put sets the value for a key in the bucket.
// a wrapper of the function put.
func (tx *Tx) Put(bucket string, key, value []byte, ttl uint32) error {
	return tx.put(bucket, key, value, ttl, DataSetFlag, uint64(time.Now().Unix()))
}

func (tx *Tx) checkTxIsClosed() error {
	if tx.db == nil {
		return ErrTxClosed
	}
	return nil
}

// put sets the value for a key in the bucket.
// Returns an error if tx is closed, if performing a write operation on a read-only transaction, if the key is empty.
func (tx *Tx) put(bucket string, key, value []byte, ttl uint32, flag uint16, timestamp uint64) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	if !tx.writable {
		return ErrTxNotWritable
	}

	if len(key) == 0 {
		return ErrKeyEmpty
	}

	tx.pendingWrites = append(tx.pendingWrites, &Entry{
		Key:   key,
		Value: value,
		Meta: &MetaData{
			keySize:    uint32(len(key)),
			valueSize:  uint32(len(value)),
			timestamp:  timestamp,
			Flag:       flag,
			TTL:        ttl,
			bucket:     []byte(bucket),
			bucketSize: uint32(len(bucket)),
			status:     UnCommitted,
			txId:       tx.id,
		},
	})

	return nil
}

// Get retrieves the value for a key in the bucket.
// The returned value is only valid for the life of the transaction.
func (tx *Tx) Get(bucket string, key []byte) (e *Entry, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	idxMode := tx.db.opt.EntryIdxMode
	if idxMode == HintAndRAMIdxMode || idxMode == HintAndMemoryMapIdxMode {
		if idx, ok := tx.db.HintIdx[bucket]; ok {
			r, err := idx.Find(key)
			if err != nil {
				return nil, err
			}

			if r.H.meta.Flag == DataDeleteFlag || r.isExpired() {
				return nil, ErrNotFoundKey
			}

			if idxMode == HintAndRAMIdxMode {
				return r.E, nil
			}

			if idxMode == HintAndMemoryMapIdxMode {
				path := tx.db.getDataPath(r.H.fileId)
				df, err := NewDataFile(path, tx.db.opt.SegmentSize)
				if err != nil {
					return nil, err
				}

				item, err := df.ReadAt(int(r.H.dataPos))
				if err != nil {
					return nil, fmt.Errorf("read err. pos %d, key %s, err %s", r.H.dataPos, string(key), err)
				}

				if err := df.m.Unmap(); err != nil {
					return nil, err
				}

				return item, nil
			}
		}
	}

	return nil, errors.New("not found bucket:" + bucket + ",key:" + string(key))
}

// RangeScan query a range at given bucket, start and end slice.
func (tx *Tx) RangeScan(bucket string, start, end []byte) (entries Entries, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	entries = make(Entries)

	if index, ok := tx.db.HintIdx[bucket]; ok {
		records, err := index.Range(start, end)
		if err != nil {
			return nil, ErrRangeScan
		}

		entries, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, entries, RangeScan)
		if err != nil {
			return nil, ErrRangeScan
		}
	}

	if len(entries) == 0 {
		return nil, ErrRangeScan
	}

	return
}

// PrefixScan iterates over a key prefix at given bucket, prefix and limitNum.
// LimitNum will limit the number of entries return.
func (tx *Tx) PrefixScan(bucket string, prefix []byte, limitNum int) (es Entries, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	es = make(Entries)

	if idx, ok := tx.db.HintIdx[bucket]; ok {
		records, err := idx.PrefixScan(prefix, limitNum)
		if err != nil {
			return nil, ErrPrefixScan
		}

		es, err = tx.getHintIdxDataItemsWrapper(records, limitNum, es, PrefixScan)
		if err != nil {
			return nil, ErrPrefixScan
		}
	}

	if len(es) == 0 {
		return nil, ErrPrefixScan
	}

	return
}

// Delete removes a key from the bucket at given bucket and key.
func (tx *Tx) Delete(bucket string, key []byte) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	return tx.put(bucket, key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()))
}

// getHintIdxDataItemsWrapper returns wrapped entries when prefix scanning or range scanning.
func (tx *Tx) getHintIdxDataItemsWrapper(records Records, limitNum int, es Entries, scanMode string) (Entries, error) {
	for k, r := range records {
		if r.H.meta.Flag == DataDeleteFlag || r.isExpired() {
			continue
		}

		if limitNum > 0 && len(es) < limitNum || limitNum == ScanNoLimit {
			idxMode := tx.db.opt.EntryIdxMode
			if idxMode == HintAndMemoryMapIdxMode {
				path := tx.db.getDataPath(r.H.fileId)
				df, err := NewDataFile(path, tx.db.opt.SegmentSize)
				if err != nil {
					return nil, err
				}
				if item, err := df.ReadAt(int(r.H.dataPos)); err == nil {
					es[k] = item
				} else {
					return nil, fmt.Errorf("HintIdx r.Hi.dataPos %d, err %s", r.H.dataPos, err)
				}
			}

			if idxMode == HintAndRAMIdxMode {
				es[k] = r.E
			}
		}
	}

	return es, nil
}
