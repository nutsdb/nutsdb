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
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"github.com/xujiajun/utils/filesystem"
	"github.com/xujiajun/utils/strconv2"
)

// scanNoLimit represents the data scan no limit flag
const scanNoLimit int = -1
const kvWriteChCapacity = 1000
const flockName = "nutsdb-flock"

type (
	// DB represents a collection of buckets that persist on disk.
	DB struct {
		opt                     Options // the database options
		Index                   *index
		ActiveFile              *dataFile
		MaxFileID               int64
		mu                      sync.RWMutex
		KeyCount                int // total key number ,include expired, deleted, repeated.
		closed                  bool
		isMerging               bool
		fm                      *fileManager
		flock                   *flock.Flock
		commitBuffer            *bytes.Buffer
		mergeStartCh            chan struct{}
		mergeEndCh              chan error
		mergeWorkCloseCh        chan struct{}
		writeCh                 chan *request
		tm                      *ttlManager
		RecordCount             int64 // current valid record count, exclude deleted, repeated
		bm                      *bucketManager
		hintKeyAndRAMIdxModeLru *lRUCache // lru cache for HintKeyAndRAMIdxMode
	}
)

// open returns a newly initialized DB object.
func open(opt Options) (*DB, error) {
	db := &DB{
		MaxFileID:               0,
		opt:                     opt,
		KeyCount:                0,
		closed:                  false,
		Index:                   newIndex(),
		fm:                      newFileManager(opt.RWMode, opt.MaxFdNumsInCache, opt.CleanFdsCacheThreshold, opt.SegmentSize),
		mergeStartCh:            make(chan struct{}),
		mergeEndCh:              make(chan error),
		mergeWorkCloseCh:        make(chan struct{}),
		writeCh:                 make(chan *request, kvWriteChCapacity),
		tm:                      newTTLManager(opt.ExpiredDeleteType),
		hintKeyAndRAMIdxModeLru: newLruCache(opt.HintKeyAndRAMIdxCacheSize),
	}

	db.commitBuffer = createNewBufferWithSize(int(db.opt.CommitBufferSize))

	if ok := filesystem.PathIsExist(db.opt.Dir); !ok {
		if err := os.MkdirAll(db.opt.Dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	fileLock := flock.New(filepath.Join(opt.Dir, flockName))
	if ok, err := fileLock.TryLock(); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrDirLocked
	}

	db.flock = fileLock

	if bm, err := newBucketManager(opt.Dir); err == nil {
		db.bm = bm
	} else {
		return nil, err
	}

	if err := db.rebuildBucketManager(); err != nil {
		return nil, fmt.Errorf("db.rebuildBucketManager err:%s", err)
	}

	if err := db.buildIndexes(); err != nil {
		return nil, fmt.Errorf("db.buildIndexes error: %s", err)
	}

	go db.mergeWorker()
	go db.doWrites()
	go db.tm.run()

	return db, nil
}

// Open returns a newly initialized DB object with Option.
func Open(options Options, ops ...Option) (*DB, error) {
	opts := &options
	for _, do := range ops {
		do(opts)
	}
	return open(*opts)
}

// Update executes a function within a managed read/write transaction.
func (db *DB) Update(fn func(tx *Tx) error) error {
	if fn == nil {
		return ErrFn
	}

	return db.managed(true, fn)
}

// View executes a function within a managed read-only transaction.
func (db *DB) View(fn func(tx *Tx) error) error {
	if fn == nil {
		return ErrFn
	}

	return db.managed(false, fn)
}

// Backup copies the database to file directory at the given dir.
func (db *DB) Backup(dir string) error {
	return db.View(func(tx *Tx) error {
		return filesystem.CopyDir(db.opt.Dir, dir)
	})
}

// BackupTarGZ Backup copy the database to writer.
func (db *DB) BackupTarGZ(w io.Writer) error {
	return db.View(func(tx *Tx) error {
		return tarGZCompress(w, db.opt.Dir)
	})
}

// Close releases all db resources.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDBClosed
	}

	db.closed = true

	err := db.release()
	if err != nil {
		return err
	}

	return nil
}

// release set all obj in the db instance to nil
func (db *DB) release() error {
	GCEnable := db.opt.GCWhenClose

	err := db.ActiveFile.rwManager.Release()
	if err != nil {
		return err
	}

	db.Index = nil

	db.ActiveFile = nil

	err = db.fm.close()

	if err != nil {
		return err
	}

	db.mergeWorkCloseCh <- struct{}{}

	if !db.flock.Locked() {
		return ErrDirUnlocked
	}

	err = db.flock.Unlock()
	if err != nil {
		return err
	}

	db.fm = nil

	db.tm.close()

	if GCEnable {
		runtime.GC()
	}

	return nil
}

func (db *DB) getValueByRecord(record *record) ([]byte, error) {
	if record == nil {
		return nil, ErrRecordIsNil
	}

	if record.Value != nil {
		return record.Value, nil
	}

	// firstly we find data in cache
	if db.hintKeyAndRAMIdxModeLru.get(record) != nil {
		return db.hintKeyAndRAMIdxModeLru.get(record).([]byte), nil
	}

	dirPath := getDataPath(record.FileID, db.opt.Dir)
	df, err := db.fm.getDataFile(dirPath, db.opt.SegmentSize)
	if err != nil {
		return nil, err
	}
	defer func(rwManager RWManager) {
		err := rwManager.Release()
		if err != nil {
			return
		}
	}(df.rwManager)

	payloadSize := int64(len(record.Key)) + int64(record.ValueSize)
	item, err := df.ReadEntry(int(record.DataPos), payloadSize)
	if err != nil {
		return nil, fmt.Errorf("read err. pos %d, key %s, err %s", record.DataPos, record.Key, err)
	}

	// saved in cache
	db.hintKeyAndRAMIdxModeLru.add(string(item.Value), item)
	return item.Value, nil
}

func (db *DB) commitTransaction(tx *Tx) error {
	var err error
	defer func() {
		var panicked bool
		if r := recover(); r != nil {
			// resume normal execution
			panicked = true
		}
		if panicked || err != nil {
			// log.Fatal("panicked=", panicked, ", err=", err)
			if errRollback := tx.Rollback(); errRollback != nil {
				err = errRollback
			}
		}
	}()

	// commit current tx
	tx.lock()
	tx.setStatusRunning()
	err = tx.Commit()
	if err != nil {
		// log.Fatal("txCommit fail,err=", err)
		return err
	}

	return err
}

func (db *DB) writeRequests(reqs []*request) error {
	var err error
	if len(reqs) == 0 {
		return nil
	}

	done := func(err error) {
		for _, r := range reqs {
			r.Err = err
			r.Wg.Done()
		}
	}

	for _, req := range reqs {
		tx := req.tx
		cerr := db.commitTransaction(tx)
		if cerr != nil {
			err = cerr
		}
	}

	done(err)
	return err
}

// MaxBatchCount returns max possible entries in batch
func (db *DB) getMaxBatchCount() int64 {
	return db.opt.MaxBatchCount
}

// MaxBatchSize returns max possible batch size
func (db *DB) getMaxBatchSize() int64 {
	return db.opt.MaxBatchSize
}

func (db *DB) getMaxWriteRecordCount() int64 {
	return db.opt.MaxWriteRecordCount
}

func (db *DB) getHintKeyAndRAMIdxCacheSize() int {
	return db.opt.HintKeyAndRAMIdxCacheSize
}

func (db *DB) doWrites() {
	pendingCh := make(chan struct{}, 1)
	writeRequests := func(reqs []*request) {
		if err := db.writeRequests(reqs); err != nil {
			log.Fatal("writeRequests fail, err=", err)
		}
		<-pendingCh
	}

	reqs := make([]*request, 0, 10)
	var r *request
	var ok bool
	for {
		r, ok = <-db.writeCh
		if !ok {
			goto closedCase
		}

		for {
			reqs = append(reqs, r)

			if len(reqs) >= 3*kvWriteChCapacity {
				pendingCh <- struct{}{} // blocking.
				goto writeCase
			}

			select {
			// Either push to pending, or continue to pick from writeCh.
			case r, ok = <-db.writeCh:
				if !ok {
					goto closedCase
				}
			case pendingCh <- struct{}{}:
				goto writeCase
			}
		}

	closedCase:
		// All the pending request are drained.
		// Don't close the writeCh, because it has be used in several places.
		for {
			select {
			case r = <-db.writeCh:
				reqs = append(reqs, r)
			default:
				pendingCh <- struct{}{} // Push to pending before doing write.
				writeRequests(reqs)
				return
			}
		}

	writeCase:
		go writeRequests(reqs)
		reqs = make([]*request, 0, 10)
	}
}

// setActiveFile sets the ActiveFile (dataFile object).
func (db *DB) setActiveFile() (err error) {
	activeFilePath := getDataPath(db.MaxFileID, db.opt.Dir)
	db.ActiveFile, err = db.fm.getDataFile(activeFilePath, db.opt.SegmentSize)
	if err != nil {
		return
	}

	db.ActiveFile.fileID = db.MaxFileID

	return nil
}

// getMaxFileIDAndFileIds returns max fileId and fileIds.
func (db *DB) getMaxFileIDAndFileIDs() (maxFileID int64, dataFileIds []int) {
	files, _ := os.ReadDir(db.opt.Dir)

	if len(files) == 0 {
		return 0, nil
	}

	for _, file := range files {
		filename := file.Name()
		fileSuffix := path.Ext(path.Base(filename))
		if fileSuffix != DataSuffix {
			continue
		}

		filename = strings.TrimSuffix(filename, DataSuffix)
		id, _ := strconv2.StrToInt(filename)
		dataFileIds = append(dataFileIds, id)
	}

	if len(dataFileIds) == 0 {
		return 0, nil
	}

	sort.Ints(dataFileIds)
	maxFileID = int64(dataFileIds[len(dataFileIds)-1])

	return
}

func (db *DB) parseDataFiles(dataFileIds []int) (err error) {
	var (
		off      int64
		f        *fileRecovery
		fID      int64
		dataInTx dataInTx
	)

	parseDataInTx := func() error {
		for _, entry := range dataInTx.es {
			// if this bucket is not existed in bucket manager right now
			// its because it already deleted in the feature WAL log.
			// so we can just ignore here.
			bucketId := entry.Meta.BucketId
			if _, err := db.bm.getBucketById(bucketId); errors.Is(err, ErrBucketNotExist) {
				continue
			}

			record := db.createRecordByModeWithFidAndOff(entry.fid, uint64(entry.off), &entry.entry)

			if err = db.buildIdxes(record, &entry.entry); err != nil {
				return err
			}

			db.KeyCount++

		}
		return nil
	}

	readEntriesFromFile := func() error {
		for {
			entry, err := f.readEntry(off)
			if err != nil {
				// whatever which logic branch it will choose, we will release the fd.
				_ = f.release()
				if errors.Is(err, io.EOF) || errors.Is(err, ErrIndexOutOfBound) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrEntryZero) || errors.Is(err, ErrHeaderSizeOutOfBounds) {
					break
				}
				if off >= db.opt.SegmentSize {
					break
				}

				return err
			}

			if entry == nil {
				break
			}

			entryWhenRecovery := &entryWhenRecovery{
				entry: *entry,
				fid:   fID,
				off:   off,
			}
			if dataInTx.txId == 0 {
				dataInTx.appendEntry(entryWhenRecovery)
				dataInTx.txId = entry.Meta.TxID
				dataInTx.startOff = off
			} else if dataInTx.isSameTx(entryWhenRecovery) {
				dataInTx.appendEntry(entryWhenRecovery)
			}

			if entry.Meta.Status == Committed {
				err := parseDataInTx()
				if err != nil {
					return err
				}
				dataInTx.reset()
				dataInTx.startOff = off
			}

			if !dataInTx.isSameTx(entryWhenRecovery) {
				dataInTx.reset()
				dataInTx.startOff = off
			}

			off += entry.size()
		}

		if fID == db.MaxFileID {
			db.ActiveFile.ActualSize = off
			db.ActiveFile.writeOff = off
		}

		return nil
	}

	for _, dataID := range dataFileIds {
		off = 0
		fID = int64(dataID)
		dataPath := getDataPath(fID, db.opt.Dir)
		f, err = newFileRecovery(dataPath, db.opt.BufferSizeOfRecovery)
		if err != nil {
			return err
		}
		err := readEntriesFromFile()
		if err != nil {
			return err
		}
	}

	// compute the valid record count and save it in db.RecordCount
	db.RecordCount, err = db.getRecordCount()
	return
}

func (db *DB) getRecordCount() (int64, error) {
	var res int64

	// Iterate through the bTree indices
	for _, btree := range db.Index.bTree.idx {
		res += int64(btree.count())
	}

	// Iterate through the list indices
	for _, listItem := range db.Index.list.idx {
		for key := range listItem.Items {
			curLen, err := listItem.size(key)
			if err != nil {
				return res, err
			}
			res += int64(curLen)
		}
	}

	// Iterate through the set indices
	for _, setItem := range db.Index.set.idx {
		for key := range setItem.M {
			res += int64(setItem.sCard(key))
		}
	}

	// Iterate through the sortedSet indices
	for _, zsetItem := range db.Index.sortedSet.idx {
		for key := range zsetItem.M {
			curLen, err := zsetItem.zCard(key)
			if err != nil {
				return res, err
			}
			res += int64(curLen)
		}
	}

	return res, nil
}

func (db *DB) buildBTreeIdx(record *record, entry *entry) error {
	key, meta := entry.Key, entry.Meta

	bucket, err := db.bm.getBucketById(meta.BucketId)
	if err != nil {
		return err
	}
	bucketId := bucket.Id

	bTree := db.Index.bTree.getWithDefault(bucketId)

	if record.isExpired() || meta.Flag == DataDeleteFlag {
		db.tm.del(bucketId, string(key))
		bTree.delete(key)
	} else {
		if meta.TTL != Persistent {
			db.tm.add(bucketId, string(key), db.expireTime(meta.Timestamp, meta.TTL), db.buildExpireCallback(bucket.Name, key))
		} else {
			db.tm.del(bucketId, string(key))
		}
		bTree.insert(record)
	}
	return nil
}

func (db *DB) expireTime(timestamp uint64, ttl uint32) time.Duration {
	now := time.UnixMilli(time.Now().UnixMilli())
	expireTime := time.UnixMilli(int64(timestamp))
	expireTime = expireTime.Add(time.Duration(int64(ttl)) * time.Second)
	return expireTime.Sub(now)
}

func (db *DB) buildIdxes(record *record, entry *entry) error {
	meta := entry.Meta
	switch meta.Ds {
	case DataStructureBTree:
		return db.buildBTreeIdx(record, entry)
	case DataStructureList:
		if err := db.buildListIdx(record, entry); err != nil {
			return err
		}
	case DataStructureSet:
		if err := db.buildSetIdx(record, entry); err != nil {
			return err
		}
	case DataStructureSortedSet:
		if err := db.buildSortedSetIdx(record, entry); err != nil {
			return err
		}
	default:
		panic(fmt.Sprintf("there is an unexpected data structure that is unimplemented in our database.:%d", meta.Ds))
	}
	return nil
}

func (db *DB) deleteBucket(ds uint16, bucket bucketId) {
	if ds == DataStructureSet {
		db.Index.set.delete(bucket)
	}
	if ds == DataStructureSortedSet {
		db.Index.sortedSet.delete(bucket)
	}
	if ds == DataStructureBTree {
		db.Index.bTree.delete(bucket)
	}
	if ds == DataStructureList {
		db.Index.list.delete(bucket)
	}
}

// buildSetIdx builds set index when opening the DB.
func (db *DB) buildSetIdx(rec *record, entry *entry) error {
	key, val, meta := entry.Key, entry.Value, entry.Meta

	bucket, err := db.bm.getBucketById(entry.Meta.BucketId)
	if err != nil {
		return err
	}
	bucketId := bucket.Id

	s := db.Index.set.getWithDefault(bucketId)

	switch meta.Flag {
	case DataSetFlag:
		if err := s.sAdd(string(key), [][]byte{val}, []*record{rec}); err != nil {
			return fmt.Errorf("when build setIdx sAdd index err: %s", err)
		}
	case DataDeleteFlag:
		if err := s.sRem(string(key), val); err != nil {
			return fmt.Errorf("when build setIdx sRem index err: %s", err)
		}
	}

	return nil
}

// buildSortedSetIdx builds sorted set index when opening the DB.
func (db *DB) buildSortedSetIdx(rec *record, entry *entry) error {
	key, val, meta := entry.Key, entry.Value, entry.Meta

	bucket, err := db.bm.getBucketById(entry.Meta.BucketId)
	if err != nil {
		return err
	}
	bucketId := bucket.Id

	ss := db.Index.sortedSet.getWithDefault(bucketId, db)

	switch meta.Flag {
	case DataZAddFlag:
		keyAndScore := strings.Split(string(key), SeparatorForZSetKey)
		if len(keyAndScore) == 2 {
			key := keyAndScore[0]
			score, _ := strconv2.StrToFloat64(keyAndScore[1])
			err = ss.zAdd(key, SCORE(score), val, rec)
		}
	case DataZRemFlag:
		_, err = ss.zRem(string(key), val)
	case DataZRemRangeByRankFlag:
		start, end := splitIntIntStr(string(val), SeparatorForZSetKey)
		err = ss.zRemRangeByRank(string(key), start, end)
	case DataZPopMaxFlag:
		_, _, err = ss.zPopMax(string(key))
	case DataZPopMinFlag:
		_, _, err = ss.zPopMin(string(key))
	}

	// We don't need to panic if sorted set is not found.
	if err != nil && !errors.Is(err, ErrSortedSetNotFound) {
		return fmt.Errorf("when build sortedSetIdx err: %s", err)
	}

	return nil
}

// buildListIdx builds list index when opening the DB.
func (db *DB) buildListIdx(rec *record, entry *entry) error {
	key, val, meta := entry.Key, entry.Value, entry.Meta

	bucket, err := db.bm.getBucketById(entry.Meta.BucketId)
	if err != nil {
		return err
	}
	bucketId := bucket.Id

	l := db.Index.list.getWithDefault(bucketId)

	if isExpired(meta.TTL, meta.Timestamp) {
		return nil
	}

	switch meta.Flag {
	case DataExpireListFlag:
		t, _ := strconv2.StrToInt64(string(val))
		ttl := uint32(t)
		l.TTL[string(key)] = ttl
		l.TimeStamp[string(key)] = meta.Timestamp
	case DataLPushFlag:
		err = l.lPush(string(key), rec)
	case DataRPushFlag:
		err = l.rPush(string(key), rec)
	case DataLRemFlag:
		err = db.buildListLRemIdx(val, l, key)
	case DataLPopFlag:
		_, err = l.lPop(string(key))
	case DataRPopFlag:
		_, err = l.rPop(string(key))
	case DataLTrimFlag:
		newKey, start := splitStringIntStr(string(key), SeparatorForListKey)
		end, _ := strconv2.StrToInt(string(val))
		err = l.lTrim(newKey, start, end)
	case DataLRemByIndex:
		indexes, _ := unmarshalInts(val)
		err = l.lRemByIndex(string(key), indexes)
	}

	if err != nil {
		return fmt.Errorf("when build listIdx err: %s", err)
	}

	return nil
}

func (db *DB) buildListLRemIdx(value []byte, l *list, key []byte) error {
	count, newValue := splitIntStringStr(string(value), SeparatorForListKey)

	return l.lRem(string(key), count, func(r *record) (bool, error) {
		v, err := db.getValueByRecord(r)
		if err != nil {
			return false, err
		}
		return bytes.Equal([]byte(newValue), v), nil
	})
}

// buildIndexes builds indexes when db initialize resource.
func (db *DB) buildIndexes() (err error) {
	var (
		maxFileID   int64
		dataFileIds []int
	)

	maxFileID, dataFileIds = db.getMaxFileIDAndFileIDs()

	// init db.ActiveFile
	db.MaxFileID = maxFileID

	// set ActiveFile
	if err = db.setActiveFile(); err != nil {
		return
	}

	if dataFileIds == nil && maxFileID == 0 {
		return
	}

	// build hint index
	return db.parseDataFiles(dataFileIds)
}

func (db *DB) createRecordByModeWithFidAndOff(fid int64, off uint64, entry *entry) *record {
	record := newRecord()

	record.withKey(entry.Key).
		withTimestamp(entry.Meta.Timestamp).
		withTTL(entry.Meta.TTL).
		withTxID(entry.Meta.TxID)

	if db.opt.EntryIdxMode == HintKeyValAndRAMIdxMode {
		record.withValue(entry.Value)
	}

	if db.opt.EntryIdxMode == HintKeyAndRAMIdxMode {
		record.withFileId(fid).
			withDataPos(off).
			withValueSize(uint32(len(entry.Value)))
	}

	return record
}

// managed calls a block of code that is fully contained in a transaction.
func (db *DB) managed(writable bool, fn func(tx *Tx) error) (err error) {
	var tx *Tx

	tx, err = db.Begin(writable)
	if err != nil {
		return err
	}
	//defer func() {
	//	if r := recover(); r != nil {
	//		err = fmt.Errorf("panic when executing tx, err is %+v", r)
	//	}
	//}()

	if err = fn(tx); err == nil {
		err = tx.Commit()
	} else {
		if db.opt.ErrorHandler != nil {
			db.opt.ErrorHandler.HandleError(err)
		}

		if errRollback := tx.Rollback(); errRollback != nil {
			err = fmt.Errorf("%v. Rollback err: %v", err, errRollback)
		}
	}

	return err
}

func (db *DB) sendToWriteCh(tx *Tx) (*request, error) {
	req := requestPool.Get().(*request)
	req.reset()
	req.Wg.Add(1)
	req.tx = tx
	req.incrRef()     // for db write
	db.writeCh <- req // Handled in doWrites.
	return req, nil
}

func (db *DB) checkListExpired() {
	db.Index.list.rangeIdx(func(l *list) {
		for key := range l.TTL {
			l.isExpire(key)
		}
	})
}

// IsClose return the value that represents the status of DB
func (db *DB) IsClose() bool {
	return db.closed
}

func (db *DB) buildExpireCallback(bucket string, key []byte) func() {
	return func() {
		err := db.Update(func(tx *Tx) error {
			b, err := tx.db.bm.getBucket(DataStructureBTree, bucket)
			if err != nil {
				return err
			}
			bucketId := b.Id
			if db.tm.exist(bucketId, string(key)) {
				return tx.Delete(bucket, key)
			}
			return nil
		})
		if err != nil {
			log.Printf("occur error when expired deletion, error: %v", err.Error())
		}
	}
}

func (db *DB) rebuildBucketManager() error {
	bucketFilePath := db.opt.Dir + "/" + bucketStoreFileName
	f, err := newFileRecovery(bucketFilePath, db.opt.BufferSizeOfRecovery)
	if err != nil {
		return nil
	}
	bucketRequest := make([]*bucketSubmitRequest, 0)

	for {
		bucket, err := f.readBucket()
		if err != nil {
			// whatever which logic branch it will choose, we will release the fd.
			_ = f.release()
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			} else {
				return err
			}
		}
		bucketRequest = append(bucketRequest, &bucketSubmitRequest{
			ds:     bucket.Ds,
			name:   bucketName(bucket.Name),
			bucket: bucket,
		})
	}

	if len(bucketRequest) > 0 {
		err = db.bm.submitPendingBucketChange(bucketRequest)
		if err != nil {
			return err
		}
	}
	return nil
}
