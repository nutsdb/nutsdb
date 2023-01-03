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
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/nutsdb/nutsdb/ds/list"
	"github.com/nutsdb/nutsdb/ds/set"
	"github.com/nutsdb/nutsdb/ds/zset"
	"github.com/xujiajun/utils/strconv2"
)

var (
	// ErrDataSizeExceed is returned when given key and value size is too big.
	ErrDataSizeExceed = errors.New("data size too big")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx is closed")

	// ErrTxNotWritable is returned when performing a write operation on
	// a read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrKeyEmpty is returned if an empty key is passed on an update function.
	ErrKeyEmpty = errors.New("key cannot be empty")

	// ErrBucketEmpty is returned if bucket is empty.
	ErrBucketEmpty = errors.New("bucket is empty")

	// ErrRangeScan is returned when range scanning not found the result
	ErrRangeScan = errors.New("range scans not found")

	// ErrPrefixScan is returned when prefix scanning not found the result
	ErrPrefixScan = errors.New("prefix scans not found")

	// ErrPrefixSearchScan is returned when prefix and search scanning not found the result
	ErrPrefixSearchScan = errors.New("prefix and search scans not found")

	// ErrNotFoundKey is returned when key not found int the bucket on an view function.
	ErrNotFoundKey = errors.New("key not found in the bucket")
)

var cachePool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// Tx represents a transaction.
type Tx struct {
	id                     uint64
	db                     *DB
	writable               bool
	pendingWrites          []*Entry
	ReservedStoreTxIDIdxes map[int64]*BPTree
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
func (tx *Tx) Commit() error {
	var (
		e              *Entry
		bucketMetaTemp BucketMeta
	)

	if tx.db == nil {
		return ErrDBClosed
	}

	writesLen := len(tx.pendingWrites)

	if writesLen == 0 {
		tx.unlock()
		tx.db = nil
		return nil
	}

	lastIndex := writesLen - 1
	countFlag := CountFlagEnabled
	if tx.db.isMerging {
		countFlag = CountFlagDisabled
	}

	buff := cachePool.Get().(*bytes.Buffer)
	defer func() {
		buff.Reset()
		cachePool.Put(buff)
	}()

	for i := 0; i < writesLen; i++ {
		entry := tx.pendingWrites[i]
		entrySize := entry.Size()
		if entrySize > tx.db.opt.SegmentSize {
			return ErrDataSizeExceed
		}

		if len(entry.Meta.Bucket) > math.MaxUint32 || len(entry.Key) > math.MaxUint32 || len(entry.Value) > math.MaxUint32 {
			return ErrDataSizeExceed
		}

		bucket := string(entry.Meta.Bucket)

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

		if entry.Meta.Ds == DataStructureBPTree {
			tx.db.BPTreeKeyEntryPosMap[string(getNewKey(string(entry.Meta.Bucket), entry.Key))] = offset
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

		if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
			bucketMetaTemp = tx.buildTempBucketMetaIdx(bucket, entry.Key, bucketMetaTemp)
		}

		if i == lastIndex {
			txID := entry.Meta.TxID
			if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
				if err := tx.buildTxIDRootIdx(txID, countFlag); err != nil {
					return err
				}

				if err := tx.buildBucketMetaIdx(bucket, entry.Key, bucketMetaTemp); err != nil {
					return err
				}
			} else {
				tx.db.committedTxIds[txID] = struct{}{}
			}
		}

		e = nil
		if tx.db.opt.EntryIdxMode == HintKeyValAndRAMIdxMode {
			e = entry
		}

		if entry.Meta.Ds == DataStructureBPTree {
			tx.buildBPTreeIdx(bucket, entry, e, offset, countFlag)
		}
		if entry.Meta.Ds == DataStructureNone && entry.Meta.Flag == DataBPTreeBucketDeleteFlag {
			tx.db.deleteBucket(DataStructureBPTree, bucket)
		}
	}

	tx.buildIdxes(writesLen)

	tx.unlock()

	tx.db = nil

	tx.pendingWrites = nil
	tx.ReservedStoreTxIDIdxes = nil

	return nil
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
		fd, err := os.OpenFile(tx.db.getBucketMetaFilePath(bucket), os.O_CREATE|os.O_RDWR, 0644)
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

func (tx *Tx) buildTxIDRootIdx(txID uint64, countFlag bool) error {
	txIDStr := strconv2.IntToStr(int(txID))

	tx.db.ActiveCommittedTxIdsIdx.Insert([]byte(txIDStr), nil, &Hint{Meta: &MetaData{Flag: DataSetFlag}}, countFlag)
	if len(tx.ReservedStoreTxIDIdxes) > 0 {
		for fID, txIDIdx := range tx.ReservedStoreTxIDIdxes {
			filePath := tx.db.getBPTTxIDPath(fID)

			txIDIdx.Insert([]byte(txIDStr), nil, &Hint{Meta: &MetaData{Flag: DataSetFlag}}, countFlag)
			txIDIdx.Filepath = filePath

			err := txIDIdx.WriteNodes(tx.db.opt.RWMode, tx.db.opt.SyncEnable, 2)
			if err != nil {
				return err
			}

			filePath = tx.db.getBPTRootTxIDPath(fID)
			txIDRootIdx := NewTree()
			rootAddress := strconv2.Int64ToStr(txIDIdx.root.Address)

			txIDRootIdx.Insert([]byte(rootAddress), nil, &Hint{Meta: &MetaData{Flag: DataSetFlag}}, countFlag)
			txIDRootIdx.Filepath = filePath

			err = txIDRootIdx.WriteNodes(tx.db.opt.RWMode, tx.db.opt.SyncEnable, 2)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (tx *Tx) buildIdxes(writesLen int) {
	for i := 0; i < writesLen; i++ {
		entry := tx.pendingWrites[i]

		bucket := string(entry.Meta.Bucket)

		if entry.Meta.Ds == DataStructureSet {
			tx.buildSetIdx(bucket, entry)
		}

		if entry.Meta.Ds == DataStructureSortedSet {
			tx.buildSortedSetIdx(bucket, entry)
		}

		if entry.Meta.Ds == DataStructureList {
			tx.buildListIdx(bucket, entry)
		}

		if entry.Meta.Ds == DataStructureNone {
			if entry.Meta.Flag == DataSetBucketDeleteFlag {
				tx.db.deleteBucket(DataStructureSet, bucket)
			}
			if entry.Meta.Flag == DataSortedSetBucketDeleteFlag {
				tx.db.deleteBucket(DataStructureSortedSet, bucket)
			}
			if entry.Meta.Flag == DataListBucketDeleteFlag {
				tx.db.deleteBucket(DataStructureList, bucket)
			}
		}

		tx.db.KeyCount++
	}
}

func (tx *Tx) buildBPTreeIdx(bucket string, entry, e *Entry, offset int64, countFlag bool) {
	if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		newKey := getNewKey(bucket, entry.Key)
		_ = tx.db.ActiveBPTreeIdx.Insert(newKey, e, &Hint{
			FileID:  tx.db.ActiveFile.fileID,
			Key:     newKey,
			Meta:    entry.Meta,
			DataPos: uint64(offset),
		}, countFlag)
	} else {
		if _, ok := tx.db.BPTreeIdx[bucket]; !ok {
			tx.db.BPTreeIdx[bucket] = NewTree()
		}

		if tx.db.BPTreeIdx[bucket] == nil {
			tx.db.BPTreeIdx[bucket] = NewTree()
		}
		_ = tx.db.BPTreeIdx[bucket].Insert(entry.Key, e, &Hint{
			FileID:  tx.db.ActiveFile.fileID,
			Key:     entry.Key,
			Meta:    entry.Meta,
			DataPos: uint64(offset),
		}, countFlag)
	}
}

func (tx *Tx) buildSetIdx(bucket string, entry *Entry) {
	if _, ok := tx.db.SetIdx[bucket]; !ok {
		tx.db.SetIdx[bucket] = set.New()
	}

	if entry.Meta.Flag == DataDeleteFlag {
		_ = tx.db.SetIdx[bucket].SRem(string(entry.Key), entry.Value)
	}

	if entry.Meta.Flag == DataSetFlag {
		_ = tx.db.SetIdx[bucket].SAdd(string(entry.Key), entry.Value)
	}
}

func (tx *Tx) buildSortedSetIdx(bucket string, entry *Entry) {
	if _, ok := tx.db.SortedSetIdx[bucket]; !ok {
		tx.db.SortedSetIdx[bucket] = zset.New()
	}

	switch entry.Meta.Flag {
	case DataZAddFlag:
		keyAndScore := strings.Split(string(entry.Key), SeparatorForZSetKey)
		key := keyAndScore[0]
		score, _ := strconv2.StrToFloat64(keyAndScore[1])
		_ = tx.db.SortedSetIdx[bucket].Put(key, zset.SCORE(score), entry.Value)
	case DataZRemFlag:
		_ = tx.db.SortedSetIdx[bucket].Remove(string(entry.Key))
	case DataZRemRangeByRankFlag:
		start, _ := strconv2.StrToInt(string(entry.Key))
		end, _ := strconv2.StrToInt(string(entry.Value))
		_ = tx.db.SortedSetIdx[bucket].GetByRankRange(start, end, true)
	case DataZPopMaxFlag:
		_ = tx.db.SortedSetIdx[bucket].PopMax()
	case DataZPopMinFlag:
		_ = tx.db.SortedSetIdx[bucket].PopMin()
	}
}

func (tx *Tx) buildListIdx(bucket string, entry *Entry) {
	if _, ok := tx.db.ListIdx[bucket]; !ok {
		tx.db.ListIdx[bucket] = list.New()
	}
	key, value := entry.Key, entry.Value
	if IsExpired(entry.Meta.TTL, entry.Meta.Timestamp) {
		return
	}
	switch entry.Meta.Flag {
	case DataExpireListFlag:
		t, _ := strconv2.StrToInt64(string(value))
		ttl := uint32(t)
		tx.db.ListIdx[bucket].TTL[string(key)] = ttl
		tx.db.ListIdx[bucket].TimeStamp[string(key)] = entry.Meta.Timestamp
	case DataLPushFlag:
		_, _ = tx.db.ListIdx[bucket].LPush(string(key), value)
	case DataRPushFlag:
		_, _ = tx.db.ListIdx[bucket].RPush(string(key), value)
	case DataLRemFlag:
		countAndValue := strings.Split(string(value), SeparatorForListKey)
		count, _ := strconv2.StrToInt(countAndValue[0])
		newValue := countAndValue[1]

		_, _ = tx.db.ListIdx[bucket].LRem(string(key), count, []byte(newValue))
	case DataLPopFlag:
		_, _ = tx.db.ListIdx[bucket].LPop(string(key))
	case DataRPopFlag:
		_, _ = tx.db.ListIdx[bucket].RPop(string(key))
	case DataLSetFlag:
		keyAndIndex := strings.Split(string(key), SeparatorForListKey)
		newKey := keyAndIndex[0]
		index, _ := strconv2.StrToInt(keyAndIndex[1])
		_ = tx.db.ListIdx[bucket].LSet(newKey, index, value)
	case DataLTrimFlag:
		keyAndStartIndex := strings.Split(string(key), SeparatorForListKey)
		newKey := keyAndStartIndex[0]
		start, _ := strconv2.StrToInt(keyAndStartIndex[1])
		end, _ := strconv2.StrToInt(string(value))
		_ = tx.db.ListIdx[bucket].Ltrim(newKey, start, end)
	case DataLRemByIndex:
		indexes, _ := UnmarshalInts(value)
		_, _ = tx.db.ListIdx[bucket].LRemByIndex(string(key), indexes)
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
		tx.db.ActiveBPTreeIdx.Filepath = tx.db.getBPTPath(fID)
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

		_, err := BPTreeRootIdx.Persistence(tx.db.getBPTRootPath(fID),
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
	path := tx.db.getDataPath(tx.db.MaxFileID)
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
		return ErrDBClosed
	}

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

func (tx *Tx) PutWithTimestamp(bucket string, key, value []byte, ttl uint32, timestamp uint64) error {
	return tx.put(bucket, key, value, ttl, DataSetFlag, timestamp, DataStructureBPTree)
}

// Put sets the value for a key in the bucket.
// a wrapper of the function put.
func (tx *Tx) Put(bucket string, key, value []byte, ttl uint32) error {
	return tx.put(bucket, key, value, ttl, DataSetFlag, uint64(time.Now().Unix()), DataStructureBPTree)
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

	if len(key) == 0 {
		return ErrKeyEmpty
	}

	tx.pendingWrites = append(tx.pendingWrites, &Entry{
		Key:   key,
		Value: value,
		Meta: &MetaData{
			KeySize:    uint32(len(key)),
			ValueSize:  uint32(len(value)),
			Timestamp:  timestamp,
			Flag:       flag,
			TTL:        ttl,
			Bucket:     []byte(bucket),
			BucketSize: uint32(len(bucket)),
			Status:     UnCommitted,
			Ds:         ds,
			TxID:       tx.id,
		},
	})

	return nil
}
