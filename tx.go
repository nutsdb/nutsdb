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
	"fmt"
	"log"
	"strings"
	"sync/atomic"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/utils"
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
	id                uint64
	db                *DB
	writable          bool
	status            atomic.Value
	pendingWrites     *pendingEntryList
	size              int64
	pendingBucketList pendingBucketList
}

type txnCb struct {
	commit func() error
	user   func(error)
	err    error
}

func (tx *Tx) submitEntry(ds uint16, bucket string, e *core.Entry) {
	tx.pendingWrites.submitEntry(ds, bucket, e)
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
	tx = &Tx{
		db:                db,
		writable:          writable,
		pendingWrites:     newPendingEntriesList(),
		pendingBucketList: make(map[core.Ds]map[core.BucketName]*core.Bucket),
	}

	tx.id = tx.getTxID()

	return
}

func (tx *Tx) CommitWith(cb func(error)) {
	if cb == nil {
		panic("Nil callback provided to CommitWith")
	}

	if tx.pendingWrites.size == 0 {
		// Do not run these callbacks from here, because the CommitWith and the
		// callback might be acquiring the same locks. Instead run the callback
		// from another goroutine.
		go runTxnCallback(&txnCb{user: cb, err: nil})
		return
	}
	// defer tx.setStatusClosed()  //must not add this code because another process is also accessing tx
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
	count := tx.pendingWrites.size
	if int64(count) >= tx.db.getMaxBatchCount() || tx.size >= tx.db.getMaxBatchSize() {
		return ErrTxnTooBig
	}

	return nil
}

// getTxID returns the tx id.
// Uses cached snowflake node to avoid recreating for every transaction.
func (tx *Tx) getTxID() uint64 {
	node := tx.db.getSnowflakeNode()
	return uint64(node.Generate().Int64())
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
// 5. send updated entries to watch manager if watch feature is enabled.
//
// 6. Unlock the database and clear the db field.
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

	var curWriteCount int64
	if tx.db.opt.MaxWriteRecordCount > 0 {
		curWriteCount, err = tx.getNewAddRecordCount()
		if err != nil {
			return err
		}

		// judge all write records is whether more than the MaxWriteRecordCount
		if tx.db.RecordCount+curWriteCount > tx.db.opt.MaxWriteRecordCount {
			return ErrTxnExceedWriteLimit
		}
	}

	tx.setStatusCommitting()
	defer tx.setStatusClosed()

	writesBucketLen := len(tx.pendingBucketList)
	if tx.pendingWrites.size == 0 && writesBucketLen == 0 {
		return nil
	}

	buff := tx.allocCommitBuffer()
	defer tx.db.commitBuffer.Reset()

	var records []*core.Record

	pendingWriteList := tx.pendingWrites.toList()
	lastIndex := len(pendingWriteList) - 1
	for i := 0; i < len(pendingWriteList); i++ {
		entry := pendingWriteList[i]
		entrySize := entry.Size()
		if entrySize > tx.db.opt.SegmentSize {
			return ErrDataSizeExceed
		}

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

		record := tx.db.createRecordByModeWithFidAndOff(tx.db.ActiveFile.fileID, uint64(offset), entry)

		// add to cache
		if tx.db.getHintKeyAndRAMIdxCacheSize() > 0 && tx.db.opt.EntryIdxMode == HintKeyAndRAMIdxMode {
			tx.db.hintKeyAndRAMIdxModeLru.Add(record, entry)
		}

		records = append(records, record)
	}

	if err := tx.SubmitBucket(); err != nil {
		return err
	}

	if err := tx.buildIdxes(records, pendingWriteList); err != nil {
		return err
	}
	tx.db.RecordCount += curWriteCount

	if err := tx.buildBucketInIndex(); err != nil {
		return err
	}

	// send updated entries to watch manager
	if tx.db.watchManager != nil {
		tx.sendUpdatedEntries(pendingWriteList, tx.getDeletedBuckets())
	}

	return nil
}

func (tx *Tx) getNewAddRecordCount() (int64, error) {
	var res int64
	changeCountInEntries, err := tx.getChangeCountInEntriesChanges()
	changeCountInBucket := tx.getChangeCountInBucketChanges()
	res += changeCountInEntries
	res += changeCountInBucket
	return res, err
}

func (tx *Tx) getListEntryNewAddRecordCount(bucketId core.BucketId, entry *core.Entry) (int64, error) {
	if entry.Meta.Flag == DataExpireListFlag {
		return 0, nil
	}

	var res int64
	key := string(entry.Key)
	value := string(entry.Value)
	l := tx.db.Index.List.Get(bucketId)

	switch entry.Meta.Flag {
	case DataLPushFlag, DataRPushFlag:
		res++
	case DataLPopFlag, DataRPopFlag:
		res--
	case DataLRemByIndex:
		indexes, _ := utils.UnmarshalInts([]byte(value))
		res -= int64(len(l.GetValidIndexes(key, indexes)))
	case DataLRemFlag:
		count, newValue := splitIntStringStr(value, SeparatorForListKey)
		removeIndices, err := l.GetRemoveIndexes(key, count, func(r *core.Record) (bool, error) {
			v, err := tx.db.getValueByRecord(r)
			if err != nil {
				return false, err
			}
			return bytes.Equal([]byte(newValue), v), nil
		})
		if err != nil {
			return 0, err
		}
		res -= int64(len(removeIndices))
	case DataLTrimFlag:
		newKey, start := splitStringIntStr(key, SeparatorForListKey)
		end, _ := strconv2.StrToInt(value)

		if l.IsExpire(newKey) {
			return 0, nil
		}

		if _, ok := l.Items[newKey]; !ok {
			return 0, nil
		}

		items, err := l.LRange(newKey, start, end)
		if err != nil {
			return res, err
		}

		list := l.Items[newKey]
		res -= int64(list.Count() - len(items))
	}

	return res, nil
}

func (tx *Tx) getKvEntryNewAddRecordCount(bucketId core.BucketId, entry *core.Entry) (int64, error) {
	var res int64

	switch entry.Meta.Flag {
	case DataDeleteFlag:
		res--
	case DataSetFlag:
		if idx, ok := tx.db.Index.BTree.exist(bucketId); ok {
			_, found := idx.Find(entry.Key)
			if !found {
				res++
			}
		} else {
			res++
		}
	}

	return res, nil
}

func (tx *Tx) getSetEntryNewAddRecordCount(_ core.BucketId, entry *core.Entry) (int64, error) {
	var res int64

	if entry.Meta.Flag == DataDeleteFlag {
		res--
	}

	if entry.Meta.Flag == DataSetFlag {
		res++
	}

	return res, nil
}

func (tx *Tx) getSortedSetEntryNewAddRecordCount(bucketId core.BucketId, entry *core.Entry) (int64, error) {
	var res int64
	key := string(entry.Key)
	value := string(entry.Value)

	switch entry.Meta.Flag {
	case DataZAddFlag:
		if !tx.keyExistsInSortedSet(bucketId, key, value) {
			res++
		}
	case DataZRemFlag:
		res--
	case DataZRemRangeByRankFlag:
		start, end := splitIntIntStr(value, SeparatorForZSetKey)
		delNodes, err := tx.db.Index.SortedSet.Get(bucketId).getZRemRangeByRankNodes(key, start, end)
		if err != nil {
			return res, err
		}
		res -= int64(len(delNodes))
	case DataZPopMaxFlag, DataZPopMinFlag:
		res--
	}

	return res, nil
}

func (tx *Tx) keyExistsInSortedSet(bucketId core.BucketId, key, value string) bool {
	if _, exist := tx.db.Index.SortedSet.exist(bucketId); !exist {
		return false
	}
	newKey := key
	if strings.Contains(key, SeparatorForZSetKey) {
		newKey, _ = splitStringFloat64Str(key, SeparatorForZSetKey)
	}
	exists, _ := tx.db.Index.SortedSet.Idx[bucketId].ZExist(newKey, []byte(value))
	return exists
}

func (tx *Tx) getEntryNewAddRecordCount(entry *core.Entry) (int64, error) {
	var res int64
	var err error

	bucket, err := tx.db.bucketManager.GetBucketById(entry.Meta.BucketId)
	if err != nil {
		return 0, err
	}
	bucketId := bucket.Id

	if entry.Meta.Ds == DataStructureBTree {
		res, err = tx.getKvEntryNewAddRecordCount(bucketId, entry)
	}

	if entry.Meta.Ds == DataStructureList {
		res, err = tx.getListEntryNewAddRecordCount(bucketId, entry)
	}

	if entry.Meta.Ds == DataStructureSet {
		res, err = tx.getSetEntryNewAddRecordCount(bucketId, entry)
	}

	if entry.Meta.Ds == DataStructureSortedSet {
		res, err = tx.getSortedSetEntryNewAddRecordCount(bucketId, entry)
	}

	return res, err
}

func (tx *Tx) allocCommitBuffer() *bytes.Buffer {
	var buff *bytes.Buffer

	if tx.size < tx.db.opt.CommitBufferSize {
		buff = tx.db.commitBuffer
	} else {
		buff = new(bytes.Buffer)
		// avoid grow
		buff.Grow(int(tx.size))
	}

	return buff
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
	tx.db.ActiveFile, err = tx.db.fm.GetDataFile(path, tx.db.opt.SegmentSize)
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
func (tx *Tx) put(bucket string, key, value []byte, ttl uint32, flag uint16, timestamp uint64, ds uint16) (err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	bucketStatus, b := tx.getBucketAndItsStatus(ds, bucket)
	if isBucketNotFoundStatus(bucketStatus) {
		return ErrBucketNotFound
	}

	if !tx.writable {
		return ErrTxNotWritable
	}
	bucketId := b.Id

	meta := core.NewMetaData().WithTimeStamp(timestamp).WithKeySize(uint32(len(key))).WithValueSize(uint32(len(value))).WithFlag(flag).
		WithTTL(ttl).WithStatus(UnCommitted).WithDs(ds).WithTxID(tx.id).WithBucketId(bucketId)

	e := core.NewEntry().WithKey(key).WithMeta(meta).WithValue(value)

	err = e.Valid()
	if err != nil {
		return err
	}
	tx.submitEntry(ds, bucket, e)
	tx.size += e.Size()
	return nil
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

func (tx *Tx) buildIdxes(records []*core.Record, entries []*core.Entry) error {
	for i, entry := range entries {
		meta := entry.Meta
		var err error
		switch meta.Ds {
		case DataStructureBTree:
			err = tx.db.buildBTreeIdx(records[i], entry)
		case DataStructureList:
			err = tx.db.buildListIdx(records[i], entry)
		case DataStructureSet:
			err = tx.db.buildSetIdx(records[i], entry)
		case DataStructureSortedSet:
			err = tx.db.buildSortedSetIdx(records[i], entry)
		}

		if err != nil {
			return err
		}

		tx.db.KeyCount++
	}
	return nil
}

func (tx *Tx) putBucket(b *core.Bucket) error {
	if _, exist := tx.pendingBucketList[b.Ds]; !exist {
		tx.pendingBucketList[b.Ds] = map[core.BucketName]*core.Bucket{}
	}
	bucketInDs := tx.pendingBucketList[b.Ds]
	bucketInDs[b.Name] = b
	return nil
}

func (tx *Tx) SubmitBucket() error {
	bucketReqs := make([]*bucketSubmitRequest, 0)
	for ds, mapper := range tx.pendingBucketList {
		for name, bucket := range mapper {
			req := &bucketSubmitRequest{
				ds:     ds,
				name:   name,
				bucket: bucket,
			}
			bucketReqs = append(bucketReqs, req)
		}
	}
	return tx.db.bucketManager.SubmitPendingBucketChange(bucketReqs)
}

// buildBucketInIndex build indexes on creation and deletion of buckets
func (tx *Tx) buildBucketInIndex() error {
	for _, mapper := range tx.pendingBucketList {
		for _, bucket := range mapper {
			switch bucket.Meta.Op {
			case core.BucketInsertOperation:
				switch bucket.Ds {
				case DataStructureBTree:
					tx.db.Index.BTree.Get(bucket.Id)
				case DataStructureList:
					tx.db.Index.List.Get(bucket.Id)
				case DataStructureSet:
					tx.db.Index.Set.Get(bucket.Id)
				case DataStructureSortedSet:
					tx.db.Index.SortedSet.Get(bucket.Id)
				default:
					return ErrDataStructureNotSupported
				}
			case core.BucketDeleteOperation:
				switch bucket.Ds {
				case DataStructureBTree:
					tx.db.Index.BTree.delete(bucket.Id)
				case DataStructureList:
					tx.db.Index.List.delete(bucket.Id)
				case DataStructureSet:
					tx.db.Index.Set.delete(bucket.Id)
				case DataStructureSortedSet:
					tx.db.Index.SortedSet.delete(bucket.Id)
				default:
					return ErrDataStructureNotSupported
				}
			}
		}
	}
	return nil
}

func (tx *Tx) getChangeCountInEntriesChanges() (int64, error) {
	var res int64
	for _, entriesInDS := range tx.pendingWrites.entriesInBTree {
		for _, entry := range entriesInDS {
			curRecordCnt, err := tx.getEntryNewAddRecordCount(entry)
			if err != nil {
				return res, nil
			}
			res += curRecordCnt
		}
	}
	for _, entriesInDS := range tx.pendingWrites.entries {
		for _, entries := range entriesInDS {
			for _, entry := range entries {
				curRecordCnt, err := tx.getEntryNewAddRecordCount(entry)
				if err != nil {
					return res, err
				}
				res += curRecordCnt
			}
		}
	}
	return res, nil
}

func (tx *Tx) getChangeCountInBucketChanges() int64 {
	var res int64
	f := func(bucket *core.Bucket) error {
		bucketId := bucket.Id
		if bucket.Meta.Op == core.BucketDeleteOperation {
			switch bucket.Ds {
			case DataStructureBTree:
				if bTree, ok := tx.db.Index.BTree.Idx[bucketId]; ok {
					res -= int64(bTree.Count())
				}
			case DataStructureSet:
				if set, ok := tx.db.Index.Set.Idx[bucketId]; ok {
					for key := range set.M {
						res -= int64(set.SCard(key))
					}
				}
			case DataStructureSortedSet:
				if sortedSet, ok := tx.db.Index.SortedSet.Idx[bucketId]; ok {
					for key := range sortedSet.M {
						curLen, _ := sortedSet.ZCard(key)
						res -= int64(curLen)
					}
				}
			case DataStructureList:
				if list, ok := tx.db.Index.List.Idx[bucketId]; ok {
					for key := range list.Items {
						curLen, _ := list.Size(key)
						res -= int64(curLen)
					}
				}
			default:
				panic(fmt.Sprintf("there is an unexpected data structure that is unimplemented in our database.:%d", bucket.Ds))
			}
		}
		return nil
	}
	_ = tx.pendingBucketList.rangeBucket(f)
	return res
}

// getBucketAndItsStatus, get bucket and it is status in pendingBucketList,
// if bucket is already in bucket manager but not in pendingList, will return BucketStatusExistAlready.
func (tx *Tx) getBucketAndItsStatus(ds core.Ds, name core.BucketName) (BucketStatus, *core.Bucket) {
	if len(tx.pendingBucketList) > 0 {
		if bucketInDs, exist := tx.pendingBucketList[ds]; exist {
			if bucket, exist := bucketInDs[name]; exist {
				switch bucket.Meta.Op {
				case core.BucketInsertOperation:
					return BucketStatusNew, bucket
				case core.BucketDeleteOperation:
					return BucketStatusDeleted, bucket
				case core.BucketUpdateOperation:
					return BucketStatusUpdated, bucket
				}
			}
		}
	}
	if bucket, err := tx.db.bucketManager.GetBucket(ds, name); err == nil {
		return BucketStatusExistAlready, bucket
	}
	return BucketStatusUnknown, nil
}

// findEntryStatus finds the latest status for the certain Entry in Tx
func (tx *Tx) findEntryAndItsStatus(ds core.Ds, bucket core.BucketName, key string) (EntryStatus, *core.Entry) {
	if tx.pendingWrites.size == 0 {
		return NotFoundEntry, nil
	}
	pendingWriteEntries := tx.pendingWrites.entriesInBTree
	if pendingWriteEntries == nil {
		return NotFoundEntry, nil
	}
	if pendingWriteEntries[bucket] == nil {
		return NotFoundEntry, nil
	}
	entries := pendingWriteEntries[bucket]
	if entry, exist := entries[key]; exist {
		switch entry.Meta.Flag {
		case DataDeleteFlag:
			return EntryDeleted, nil
		default:
			return EntryUpdated, entry
		}
	}
	return NotFoundEntry, nil
}

/*
 * send updated entries to watch manager for monitoring
 * and specifying the buckets to be deleted
 * @param pendingWriteList: the list of entries to be sent
 * @param deletedBuckets: the buckets to be deleted
 *
 *
 * @return: nil if success, error if any
 */
func (tx *Tx) sendUpdatedEntries(pendingWriteList []*core.Entry, deletedBuckets map[core.BucketName]bool) {
	err := tx.db.watchManager.sendUpdatedEntries(pendingWriteList, deletedBuckets, func(bucketId core.BucketId) (core.BucketName, error) {
		bucket, err := tx.db.bucketManager.GetBucketById(bucketId)
		if err != nil {
			return "", err
		}

		return bucket.Name, nil
	})

	if err != nil {
		log.Println("send updated entries error: ", err)
	}
}

/*
* send buckets to watch manager for specifying the bucket to be deleted

* @param pendingWriteList: the list of entries to be sent
* @return: nil if success, error if any
 */
func (tx *Tx) getDeletedBuckets() (deletedBuckets map[core.BucketName]bool) {
	if len(tx.pendingBucketList) == 0 {
		return nil
	}

	deletedBuckets = make(map[core.BucketName]bool)
	for _, mapper := range tx.pendingBucketList {
		for name, bucket := range mapper {
			isAllDsDeleted := len(tx.db.bucketManager.BucketIDMarker[name]) == 0
			if _, ok := deletedBuckets[name]; !ok && bucket.Meta.Op == core.BucketDeleteOperation && isAllDsDeleted {
				deletedBuckets[name] = true
			}

		}
	}

	return deletedBuckets
}
