// Copyright 2019 The nutsdb Author. All rights reserved.
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
	"bytes"
	"errors"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/xujiajun/utils/strconv2"
)

const (
	// txStatusRunning means the tx is running
	txStatusRunning = 1
	// txStatusCommitting means the tx is committing
	txStatusCommitting = 2
	// txStatusClosed means the tx is closed, ether committed or rollback
	txStatusClosed = 3
)

// Tx represents a transaction.
type Tx struct {
	id            uint64
	db            *DB
	writable      bool
	status        atomic.Value
	pendingWrites []*Entry
	size          int64
}

type txnCb struct {
	commit func() error
	user   func(error)
	err    error
}

func runTxnCallback(cb *txnCb) {
	switch {
	case cb == nil:
		panic("tx callback is nil")
	case cb.user == nil:
		panic("Must have caught a nil callback for tx.CommitWith")
	case cb.err != nil:
		cb.user(cb.err)
	case cb.commit != nil:
		err := cb.commit()
		cb.user(err)
	default:
		cb.user(nil)
	}
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
	tx.setStatusRunning()
	if db.closed {
		tx.unlock()
		tx.setStatusClosed()
		return nil, ErrDBClosed
	}

	return
}

// newTx returns a newly initialized Tx object at given writable.
func newTx(db *DB, writable bool) (tx *Tx, err error) {
	var txID uint64

	tx = &Tx{
		db:            db,
		writable:      writable,
		pendingWrites: []*Entry{},
	}

	txID, err = tx.getTxID()
	if err != nil {
		return nil, err
	}

	tx.id = txID

	return
}

func (tx *Tx) CommitWith(cb func(error)) {
	if cb == nil {
		panic("Nil callback provided to CommitWith")
	}

	if len(tx.pendingWrites) == 0 {
		// Do not run these callbacks from here, because the CommitWith and the
		// callback might be acquiring the same locks. Instead run the callback
		// from another goroutine.
		go runTxnCallback(&txnCb{user: cb, err: nil})
		return
	}
	//defer tx.setStatusClosed()  //must not add this code because another process is also accessing tx
	commitCb, err := tx.commitAndSend()
	if err != nil {
		go runTxnCallback(&txnCb{user: cb, err: err})
		return
	}

	go runTxnCallback(&txnCb{user: cb, commit: commitCb})
}

func (tx *Tx) commitAndSend() (func() error, error) {
	req, err := tx.db.sendToWriteCh(tx)
	if err != nil {
		return nil, err
	}
	ret := func() error {
		err := req.Wait()
		return err
	}

	return ret, nil
}

func (tx *Tx) checkSize() error {
	count := len(tx.pendingWrites)
	if int64(count) >= tx.db.getMaxBatchCount() || tx.size >= tx.db.getMaxBatchSize() {
		return ErrTxnTooBig
	}

	return nil
}

// getTxID returns the tx id.
func (tx *Tx) getTxID() (id uint64, err error) {
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
func (tx *Tx) Commit() (err error) {
	defer func() {
		if err != nil {
			tx.handleErr(err)
		}
		tx.unlock()
		tx.db = nil

		tx.pendingWrites = nil
	}()
	if tx.isClosed() {
		return ErrCannotCommitAClosedTx
	}

	if tx.db == nil {
		tx.setStatusClosed()
		return ErrDBClosed
	}

	tx.setStatusCommitting()
	defer tx.setStatusClosed()

	writesLen := len(tx.pendingWrites)

	if writesLen == 0 {
		return nil
	}

	lastIndex := writesLen - 1

	buff := tx.allocCommitBuffer()
	defer tx.db.commitBuffer.Reset()

	var records []*Record

	for i := 0; i < writesLen; i++ {
		entry := tx.pendingWrites[i]
		entrySize := entry.Size()
		if entrySize > tx.db.opt.SegmentSize {
			return ErrDataSizeExceed
		}

		bucket := string(entry.Bucket)

		if tx.db.ActiveFile.ActualSize+int64(buff.Len())+entrySize > tx.db.opt.SegmentSize {
			if _, err := tx.writeData(buff.Bytes()); err != nil {
				return err
			}
			buff.Reset()

			if err := tx.rotateActiveFile(); err != nil {
				return err
			}
		}

		offset := tx.db.ActiveFile.writeOff + int64(buff.Len())

		if i == lastIndex {
			entry.Meta.Status = Committed
		}

		if _, err := buff.Write(entry.Encode()); err != nil {
			return err
		}

		if i == lastIndex {
			if _, err := tx.writeData(buff.Bytes()); err != nil {
				return err
			}
		}

		hint := NewHint().WithKey(entry.Key).WithFileId(tx.db.ActiveFile.fileID).WithMeta(entry.Meta).WithDataPos(uint64(offset))
		record := NewRecord().WithBucket(bucket).WithValue(entry.Value).WithHint(hint)

		records = append(records, record)
	}
	if err := tx.buildIdxes(records); err != nil {
		return err
	}

	return nil
}

func (tx *Tx) allocCommitBuffer() *bytes.Buffer {
	var txSize int64
	for i := 0; i < len(tx.pendingWrites); i++ {
		txSize += tx.pendingWrites[i].Size()
	}

	var buff *bytes.Buffer

	if txSize < tx.db.opt.CommitBufferSize {
		buff = tx.db.commitBuffer
	} else {
		buff = new(bytes.Buffer)
		// avoid grow
		buff.Grow(int(txSize))
	}

	return buff
}

func (tx *Tx) buildTreeIdx(record *Record) {
	bucket, key, meta, offset := record.Bucket, record.H.Key, record.H.Meta, record.H.DataPos

	b := tx.db.Index.bTree.getWithDefault(bucket)

	if meta.Flag == DataSetFlag {
		var value []byte
		if tx.db.opt.EntryIdxMode == HintKeyValAndRAMIdxMode {
			value = record.V
		}

		if meta.TTL != Persistent {
			db := tx.db

			callback := func() {
				err := db.Update(func(tx *Tx) error {
					if db.tm.exist(bucket, string(key)) {
						return tx.Delete(bucket, key)
					}
					return nil
				})
				if err != nil {
					log.Printf("occur error when expired deletion, error: %v", err.Error())
				}
			}

			now := time.UnixMilli(time.Now().UnixMilli())
			expireTime := time.UnixMilli(int64(record.H.Meta.Timestamp))
			expireTime = expireTime.Add(time.Duration(record.H.Meta.TTL) * time.Second)

			if now.After(expireTime) {
				return
			}

			tx.db.tm.add(bucket, string(key), expireTime.Sub(now), callback)
		} else {
			tx.db.tm.del(bucket, string(key))
		}

		hint := NewHint().WithFileId(tx.db.ActiveFile.fileID).WithKey(key).WithMeta(meta).WithDataPos(offset)
		b.Insert(key, value, hint)
	} else if meta.Flag == DataDeleteFlag {
		tx.db.tm.del(bucket, string(key))
		b.Delete(key)
	}
}

func (tx *Tx) buildSetIdx(record *Record) {
	bucket, key, value, meta := record.Bucket, record.H.Key, record.V, record.H.Meta

	tx.db.resetRecordByMode(record)

	s := tx.db.Index.set.getWithDefault(bucket)

	if meta.Flag == DataDeleteFlag {
		_ = s.SRem(string(key), value)
	}

	if meta.Flag == DataSetFlag {
		_ = s.SAdd(string(key), [][]byte{value}, []*Record{record})
	}
}

func (tx *Tx) buildSortedSetIdx(record *Record) {
	bucket, key, value, meta := record.Bucket, record.H.Key, record.V, record.H.Meta

	tx.db.resetRecordByMode(record)

	ss := tx.db.Index.sortedSet.getWithDefault(bucket, tx.db)

	switch meta.Flag {
	case DataZAddFlag:
		keyAndScore := strings.Split(string(key), SeparatorForZSetKey)
		key := keyAndScore[0]
		score, _ := strconv2.StrToFloat64(keyAndScore[1])
		_ = ss.ZAdd(key, SCORE(score), value, record)
	case DataZRemFlag:
		_, _ = ss.ZRem(string(key), value)
	case DataZRemRangeByRankFlag:
		startAndEnd := strings.Split(string(value), SeparatorForZSetKey)
		start, _ := strconv2.StrToInt(startAndEnd[0])
		end, _ := strconv2.StrToInt(startAndEnd[1])
		_ = ss.ZRemRangeByRank(string(key), start, end)
	case DataZPopMaxFlag:
		_, _, _ = ss.ZPopMax(string(key))
	case DataZPopMinFlag:
		_, _, _ = ss.ZPopMin(string(key))
	}
}

func (tx *Tx) buildListIdx(record *Record) {
	bucket, key, value, meta := record.Bucket, record.H.Key, record.V, record.H.Meta

	tx.db.resetRecordByMode(record)

	l := tx.db.Index.list.getWithDefault(bucket)

	if IsExpired(meta.TTL, meta.Timestamp) {
		return
	}

	switch meta.Flag {
	case DataExpireListFlag:
		t, _ := strconv2.StrToInt64(string(value))
		ttl := uint32(t)
		l.TTL[string(key)] = ttl
		l.TimeStamp[string(key)] = meta.Timestamp
	case DataLPushFlag:
		_ = l.LPush(string(key), record)
	case DataRPushFlag:
		_ = l.RPush(string(key), record)
	case DataLRemFlag:
		countAndValue := strings.Split(string(value), SeparatorForListKey)
		count, _ := strconv2.StrToInt(countAndValue[0])
		newValue := countAndValue[1]

		_ = l.LRem(string(key), count, func(r *Record) (bool, error) {
			v, err := tx.db.getValueByRecord(r)
			if err != nil {
				return false, err
			}
			return bytes.Equal([]byte(newValue), v), nil
		})

	case DataLPopFlag:
		_, _ = l.LPop(string(key))
	case DataRPopFlag:
		_, _ = l.RPop(string(key))
	case DataLSetFlag:
		keyAndIndex := strings.Split(string(key), SeparatorForListKey)
		newKey := keyAndIndex[0]
		index, _ := strconv2.StrToInt(keyAndIndex[1])
		_ = l.LSet(newKey, index, record)
	case DataLTrimFlag:
		keyAndStartIndex := strings.Split(string(key), SeparatorForListKey)
		newKey := keyAndStartIndex[0]
		start, _ := strconv2.StrToInt(keyAndStartIndex[1])
		end, _ := strconv2.StrToInt(string(value))
		_ = l.LTrim(newKey, start, end)
	case DataLRemByIndex:
		indexes, _ := UnmarshalInts(value)
		_ = l.LRemByIndex(string(key), indexes)
	}
}

// rotateActiveFile rotates log file when active file is not enough space to store the entry.
func (tx *Tx) rotateActiveFile() error {
	var err error
	tx.db.MaxFileID++

	if !tx.db.opt.SyncEnable && tx.db.opt.RWMode == MMap {
		if err := tx.db.ActiveFile.rwManager.Sync(); err != nil {
			return err
		}
	}

	if err := tx.db.ActiveFile.rwManager.Release(); err != nil {
		return err
	}

	// reset ActiveFile
	path := getDataPath(tx.db.MaxFileID, tx.db.opt.Dir)
	tx.db.ActiveFile, err = tx.db.fm.getDataFile(path, tx.db.opt.SegmentSize)
	if err != nil {
		return err
	}

	tx.db.ActiveFile.fileID = tx.db.MaxFileID
	return nil
}

func (tx *Tx) writeData(data []byte) (n int, err error) {
	if len(data) == 0 {
		return
	}

	writeOffset := tx.db.ActiveFile.ActualSize

	l := len(data)
	if writeOffset+int64(l) > tx.db.opt.SegmentSize {
		return 0, errors.New("not enough file space")
	}

	if n, err = tx.db.ActiveFile.WriteAt(data, writeOffset); err != nil {
		return
	}

	tx.db.ActiveFile.writeOff += int64(l)
	tx.db.ActiveFile.ActualSize += int64(l)

	if tx.db.opt.SyncEnable {
		if err := tx.db.ActiveFile.rwManager.Sync(); err != nil {
			return 0, err
		}
	}

	return
}

// Rollback closes the transaction.
func (tx *Tx) Rollback() error {
	if tx.db == nil {
		tx.setStatusClosed()
		return ErrDBClosed
	}
	if tx.isCommitting() {
		return ErrCannotRollbackACommittingTx
	}

	if tx.isClosed() {
		return ErrCannotRollbackAClosedTx
	}

	tx.setStatusClosed()
	tx.unlock()

	tx.db = nil
	tx.pendingWrites = nil

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

func (tx *Tx) handleErr(err error) {
	if tx.db.opt.ErrorHandler != nil {
		tx.db.opt.ErrorHandler.HandleError(err)
	}
}

func (tx *Tx) checkTxIsClosed() error {
	if tx.db == nil {
		return ErrTxClosed
	}
	return nil
}

// put sets the value for a key in the bucket.
// Returns an error if tx is closed, if performing a write operation on a read-only transaction, if the key is empty.
func (tx *Tx) put(bucket string, key, value []byte, ttl uint32, flag uint16, timestamp uint64, ds uint16) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	if !tx.writable {
		return ErrTxNotWritable
	}

	meta := NewMetaData().WithTimeStamp(timestamp).WithKeySize(uint32(len(key))).WithValueSize(uint32(len(value))).WithFlag(flag).
		WithTTL(ttl).WithBucketSize(uint32(len(bucket))).WithStatus(UnCommitted).WithDs(ds).WithTxID(tx.id)

	e := NewEntry().WithKey(key).WithBucket([]byte(bucket)).WithMeta(meta).WithValue(value)

	err := e.valid()
	if err != nil {
		return err
	}
	tx.pendingWrites = append(tx.pendingWrites, e)
	tx.size += e.Size()

	return nil
}

func (tx *Tx) putDeleteLog(bucket string, key, value []byte, ttl uint32, flag uint16, timestamp uint64, ds uint16) {
	meta := NewMetaData().WithTimeStamp(timestamp).WithKeySize(uint32(len(key))).WithValueSize(uint32(len(value))).WithFlag(flag).
		WithTTL(ttl).WithBucketSize(uint32(len(bucket))).WithStatus(UnCommitted).WithDs(ds).WithTxID(tx.id)

	e := NewEntry().WithKey(key).WithBucket([]byte(bucket)).WithMeta(meta).WithValue(value)
	tx.pendingWrites = append(tx.pendingWrites, e)
	tx.size += e.Size()
}

// setStatusCommitting will change the tx status to txStatusCommitting
func (tx *Tx) setStatusCommitting() {
	status := txStatusCommitting
	tx.status.Store(status)
}

// setStatusClosed will change the tx status to txStatusClosed
func (tx *Tx) setStatusClosed() {
	status := txStatusClosed
	tx.status.Store(status)
}

// setStatusRunning will change the tx status to txStatusRunning
func (tx *Tx) setStatusRunning() {
	status := txStatusRunning
	tx.status.Store(status)
}

// isRunning will check if the tx status is txStatusRunning
func (tx *Tx) isRunning() bool {
	status := tx.status.Load().(int)
	return status == txStatusRunning
}

// isCommitting will check if the tx status is txStatusCommitting
func (tx *Tx) isCommitting() bool {
	status := tx.status.Load().(int)
	return status == txStatusCommitting
}

// isClosed will check if the tx status is txStatusClosed
func (tx *Tx) isClosed() bool {
	status := tx.status.Load().(int)
	return status == txStatusClosed
}

func (tx *Tx) buildIdxes(records []*Record) error {

	for _, record := range records {
		bucket, meta := record.Bucket, record.H.Meta

		switch meta.Ds {
		case DataStructureBTree:
			tx.buildTreeIdx(record)
		case DataStructureList:
			tx.buildListIdx(record)
		case DataStructureSet:
			tx.buildSetIdx(record)
		case DataStructureSortedSet:
			tx.buildSortedSetIdx(record)
		case DataStructureNone:
			switch meta.Flag {
			case DataBPTreeBucketDeleteFlag:
				tx.db.deleteBucket(DataStructureBTree, bucket)
			case DataSetBucketDeleteFlag:
				tx.db.deleteBucket(DataStructureSet, bucket)
			case DataSortedSetBucketDeleteFlag:
				tx.db.deleteBucket(DataStructureSortedSet, bucket)
			case DataListBucketDeleteFlag:
				tx.db.deleteBucket(DataStructureList, bucket)
			}
		}
		tx.db.KeyCount++
	}
	return nil
}
