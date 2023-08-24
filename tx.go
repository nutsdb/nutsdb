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
	"os"
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
	id                     uint64
	db                     *DB
	writable               bool
	status                 atomic.Value
	pendingWrites          []*Entry
	ReservedStoreTxIDIdxes map[int64]*BPTree
	size                   int64
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
		db:                     db,
		writable:               writable,
		pendingWrites:          []*Entry{},
		ReservedStoreTxIDIdxes: make(map[int64]*BPTree),
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
		tx.ReservedStoreTxIDIdxes = nil
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

		if entry.Meta.Ds == DataStructureTree {
			tx.db.BPTreeKeyEntryPosMap[string(getNewKey(string(entry.Bucket), entry.Key))] = offset
		}

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

func (tx *Tx) buildTempBucketMetaIdx(bucket string, key []byte, bucketMetaTemp BucketMeta) BucketMeta {
	keySize := uint32(len(key))
	if bucketMetaTemp.start == nil {
		bucketMetaTemp = BucketMeta{start: key, end: key, startSize: keySize, endSize: keySize}
	} else {
		if compare(bucketMetaTemp.start, key) > 0 {
			bucketMetaTemp.start = key
			bucketMetaTemp.startSize = keySize
		}

		if compare(bucketMetaTemp.end, key) < 0 {
			bucketMetaTemp.end = key
			bucketMetaTemp.endSize = keySize
		}
	}

	return bucketMetaTemp
}

func (tx *Tx) buildBucketMetaIdx(bucket string, key []byte, bucketMetaTemp BucketMeta) error {
	bucketMeta, ok := tx.db.bucketMetas[bucket]

	start := bucketMetaTemp.start
	startSize := uint32(len(start))
	end := bucketMetaTemp.end
	endSize := uint32(len(end))
	var updateFlag bool

	if !ok {
		bucketMeta = &BucketMeta{start: start, end: end, startSize: startSize, endSize: endSize}
		updateFlag = true
	} else {
		if compare(bucketMeta.start, bucketMetaTemp.start) > 0 {
			bucketMeta.start = start
			bucketMeta.startSize = startSize
			updateFlag = true
		}

		if compare(bucketMeta.end, bucketMetaTemp.end) < 0 {
			bucketMeta.end = end
			bucketMeta.endSize = endSize
			updateFlag = true
		}
	}

	if updateFlag {
		fd, err := os.OpenFile(getBucketMetaFilePath(bucket, tx.db.opt.Dir), os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return err
		}
		defer fd.Close()

		if _, err = fd.WriteAt(bucketMeta.Encode(), 0); err != nil {
			return err
		}

		if tx.db.opt.SyncEnable {
			if err = fd.Sync(); err != nil {
				return err
			}
		}
		tx.db.bucketMetas[bucket] = bucketMeta
	}

	return nil
}

func (tx *Tx) buildTxIDRootIdx(txID uint64) error {
	txIDStr := strconv2.IntToStr(int(txID))

	meta := NewMetaData().WithFlag(DataSetFlag)
	err := tx.db.ActiveCommittedTxIdsIdx.Insert([]byte(txIDStr), nil, NewHint().WithMeta(meta), CountFlagDisabled)
	if err != nil {
		return err
	}
	if len(tx.ReservedStoreTxIDIdxes) > 0 {
		for fID, txIDIdx := range tx.ReservedStoreTxIDIdxes {
			filePath := getBPTTxIDPath(fID, tx.db.opt.Dir)

			err := txIDIdx.Insert([]byte(txIDStr), nil, NewHint().WithMeta(meta), CountFlagDisabled)
			if err != nil {
				return err
			}
			txIDIdx.Filepath = filePath

			err = txIDIdx.WriteNodes(tx.db.opt.RWMode, tx.db.opt.SyncEnable, 2)
			if err != nil {
				return err
			}

			filePath = getBPTRootTxIDPath(fID, tx.db.opt.Dir)
			txIDRootIdx := NewTree()
			rootAddress := strconv2.Int64ToStr(txIDIdx.root.Address)

			err = txIDRootIdx.Insert([]byte(rootAddress), nil, NewHint().WithMeta(meta), CountFlagDisabled)
			if err != nil {
				return err
			}
			txIDRootIdx.Filepath = filePath

			err = txIDRootIdx.WriteNodes(tx.db.opt.RWMode, tx.db.opt.SyncEnable, 2)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (tx *Tx) buildTreeIdx(record *Record) {
	bucket, key, meta, offset := record.Bucket, record.H.Key, record.H.Meta, record.H.DataPos

	if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		newKey := getNewKey(bucket, key)
		hint := NewHint().WithFileId(tx.db.ActiveFile.fileID).WithKey(newKey).WithMeta(meta).WithDataPos(offset)
		_ = tx.db.ActiveBPTreeIdx.Insert(newKey, nil, hint, CountFlagDisabled)
	} else {
		if _, ok := tx.db.BTreeIdx[bucket]; !ok {
			tx.db.BTreeIdx[bucket] = NewBTree()
		}

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
			tx.db.BTreeIdx[bucket].Insert(key, value, hint)
		} else if meta.Flag == DataDeleteFlag {
			tx.db.tm.del(bucket, string(key))
			tx.db.BTreeIdx[bucket].Delete(key)
		}
	}
}

func (tx *Tx) buildSetIdx(record *Record) {
	bucket, key, value, meta := record.Bucket, record.H.Key, record.V, record.H.Meta

	tx.db.resetRecordByMode(record)

	if _, ok := tx.db.SetIdx[bucket]; !ok {
		tx.db.SetIdx[bucket] = NewSet()
	}

	if meta.Flag == DataDeleteFlag {
		_ = tx.db.SetIdx[bucket].SRem(string(key), value)
	}

	if meta.Flag == DataSetFlag {
		_ = tx.db.SetIdx[bucket].SAdd(string(key), [][]byte{value}, []*Record{record})
	}
}

func (tx *Tx) buildSortedSetIdx(record *Record) {
	bucket, key, value, meta := record.Bucket, record.H.Key, record.V, record.H.Meta

	tx.db.resetRecordByMode(record)

	if _, ok := tx.db.SortedSetIdx[bucket]; !ok {
		tx.db.SortedSetIdx[bucket] = NewSortedSet(tx.db)
	}

	switch meta.Flag {
	case DataZAddFlag:
		keyAndScore := strings.Split(string(key), SeparatorForZSetKey)
		key := keyAndScore[0]
		score, _ := strconv2.StrToFloat64(keyAndScore[1])
		_ = tx.db.SortedSetIdx[bucket].ZAdd(key, SCORE(score), value, record)
	case DataZRemFlag:
		_, _ = tx.db.SortedSetIdx[bucket].ZRem(string(key), value)
	case DataZRemRangeByRankFlag:
		startAndEnd := strings.Split(string(value), SeparatorForZSetKey)
		start, _ := strconv2.StrToInt(startAndEnd[0])
		end, _ := strconv2.StrToInt(startAndEnd[1])
		_ = tx.db.SortedSetIdx[bucket].ZRemRangeByRank(string(key), start, end)
	case DataZPopMaxFlag:
		_, _, _ = tx.db.SortedSetIdx[bucket].ZPopMax(string(key))
	case DataZPopMinFlag:
		_, _, _ = tx.db.SortedSetIdx[bucket].ZPopMin(string(key))
	}
}

func (tx *Tx) buildListIdx(record *Record) {
	bucket, key, value, meta := record.Bucket, record.H.Key, record.V, record.H.Meta

	tx.db.resetRecordByMode(record)

	l := tx.db.Index.getList(bucket)

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
	fID := tx.db.MaxFileID
	tx.db.MaxFileID++

	if !tx.db.opt.SyncEnable && tx.db.opt.RWMode == MMap {
		if err := tx.db.ActiveFile.rwManager.Sync(); err != nil {
			return err
		}
	}

	if err := tx.db.ActiveFile.rwManager.Release(); err != nil {
		return err
	}

	if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		tx.db.ActiveBPTreeIdx.Filepath = getBPTPath(fID, tx.db.opt.Dir)
		tx.db.ActiveBPTreeIdx.enabledKeyPosMap = true
		tx.db.ActiveBPTreeIdx.SetKeyPosMap(tx.db.BPTreeKeyEntryPosMap)

		err = tx.db.ActiveBPTreeIdx.WriteNodes(tx.db.opt.RWMode, tx.db.opt.SyncEnable, 1)
		if err != nil {
			return err
		}

		BPTreeRootIdx := &BPTreeRootIdx{
			rootOff:   uint64(tx.db.ActiveBPTreeIdx.root.Address),
			fID:       uint64(fID),
			startSize: uint32(len(tx.db.ActiveBPTreeIdx.FirstKey)),
			endSize:   uint32(len(tx.db.ActiveBPTreeIdx.LastKey)),
			start:     tx.db.ActiveBPTreeIdx.FirstKey,
			end:       tx.db.ActiveBPTreeIdx.LastKey,
		}

		_, err := BPTreeRootIdx.Persistence(getBPTRootPath(fID, tx.db.opt.Dir),
			0, tx.db.opt.SyncEnable)
		if err != nil {
			return err
		}

		tx.db.BPTreeRootIdxes = append(tx.db.BPTreeRootIdxes, BPTreeRootIdx)

		// clear and reset BPTreeKeyEntryPosMap
		tx.db.BPTreeKeyEntryPosMap = nil
		tx.db.BPTreeKeyEntryPosMap = make(map[string]int64)

		// clear and reset ActiveBPTreeIdx
		tx.db.ActiveBPTreeIdx = nil
		tx.db.ActiveBPTreeIdx = NewTree()

		tx.ReservedStoreTxIDIdxes[fID] = tx.db.ActiveCommittedTxIdsIdx

		// clear and reset ActiveCommittedTxIdsIdx
		tx.db.ActiveCommittedTxIdsIdx = nil
		tx.db.ActiveCommittedTxIdsIdx = NewTree()
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

func (tx *Tx) PutWithTimestamp(bucket string, key, value []byte, ttl uint32, timestamp uint64) error {
	return tx.put(bucket, key, value, ttl, DataSetFlag, timestamp, DataStructureTree)
}

// Put sets the value for a key in the bucket.
// a wrapper of the function put.
func (tx *Tx) Put(bucket string, key, value []byte, ttl uint32) error {
	return tx.put(bucket, key, value, ttl, DataSetFlag, uint64(time.Now().UnixMilli()), DataStructureTree)
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
	var bucketMetaTemp BucketMeta

	for _, record := range records {
		bucket, key, meta := record.Bucket, record.H.Key, record.H.Meta
		txID := meta.TxID

		switch meta.Ds {
		case DataStructureTree:
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
				tx.db.deleteBucket(DataStructureTree, bucket)
			case DataSetBucketDeleteFlag:
				tx.db.deleteBucket(DataStructureSet, bucket)
			case DataSortedSetBucketDeleteFlag:
				tx.db.deleteBucket(DataStructureSortedSet, bucket)
			case DataListBucketDeleteFlag:
				tx.db.deleteBucket(DataStructureList, bucket)
			}
		}
		tx.db.KeyCount++

		if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
			bucketMetaTemp = tx.buildTempBucketMetaIdx(bucket, key, bucketMetaTemp)
			if meta.Status == Committed {
				if err := tx.buildTxIDRootIdx(txID); err != nil {
					return err
				}

				if err := tx.buildBucketMetaIdx(bucket, key, bucketMetaTemp); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
