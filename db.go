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
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/gofrs/flock"
	"github.com/nutsdb/nutsdb/internal/data"
	"github.com/nutsdb/nutsdb/internal/fileio"
	"github.com/nutsdb/nutsdb/internal/utils"
	"github.com/xujiajun/utils/filesystem"
	"github.com/xujiajun/utils/strconv2"
)

// ScanNoLimit represents the data scan no limit flag
const ScanNoLimit int = -1
const KvWriteChCapacity = 1000
const FLockName = "nutsdb-flock"

type (
	// SnowflakeManager manages snowflake node initialization and caching
	SnowflakeManager struct {
		node    *snowflake.Node
		once    sync.Once
		nodeNum int64
	}

	// DB represents a collection of buckets that persist on disk.
	DB struct {
		opt                     Options // the database options
		Index                   *index
		ActiveFile              *DataFile
		MaxFileID               int64
		mu                      sync.RWMutex
		KeyCount                int // total key number ,include expired, deleted, repeated.
		closed                  bool
		isMerging               bool
		fm                      *FileManager
		flock                   *flock.Flock
		commitBuffer            *bytes.Buffer
		mergeStartCh            chan struct{}
		mergeEndCh              chan error
		mergeWorkCloseCh        chan struct{}
		writeCh                 chan *request
		tm                      *ttlManager
		RecordCount             int64 // current valid record count, exclude deleted, repeated
		bm                      *BucketManager
		hintKeyAndRAMIdxModeLru *utils.LRUCache // lru cache for HintKeyAndRAMIdxMode
		sm                      *SnowflakeManager
		wm                      *watchManager
	}
)

// NewSnowflakeManager creates a new SnowflakeManager with the given node number
func NewSnowflakeManager(nodeNum int64) *SnowflakeManager {
	return &SnowflakeManager{
		nodeNum: nodeNum,
	}
}

// GetNode returns the snowflake node, initializing it once.
// If initialization fails, it will fatal the program.
func (sm *SnowflakeManager) GetNode() *snowflake.Node {
	sm.once.Do(func() {
		var err error
		sm.node, err = snowflake.NewNode(sm.nodeNum)
		if err != nil {
			log.Fatalf("Failed to initialize snowflake node with nodeNum=%d: %v", sm.nodeNum, err)
		}
	})
	return sm.node
}

// open returns a newly initialized DB object.
func open(opt Options) (*DB, error) {
	db := &DB{
		MaxFileID:               0,
		opt:                     opt,
		KeyCount:                0,
		closed:                  false,
		Index:                   newIndexWithOptions(opt),
		fm:                      NewFileManager(opt.RWMode, opt.MaxFdNumsInCache, opt.CleanFdsCacheThreshold, opt.SegmentSize),
		mergeStartCh:            make(chan struct{}),
		mergeEndCh:              make(chan error),
		mergeWorkCloseCh:        make(chan struct{}),
		writeCh:                 make(chan *request, KvWriteChCapacity),
		tm:                      newTTLManager(opt.ExpiredDeleteType),
		hintKeyAndRAMIdxModeLru: utils.NewLruCache(opt.HintKeyAndRAMIdxCacheSize),
		sm:                      NewSnowflakeManager(opt.NodeNum),
	}

	db.commitBuffer = createNewBufferWithSize(int(db.opt.CommitBufferSize))

	if ok := filesystem.PathIsExist(db.opt.Dir); !ok {
		if err := os.MkdirAll(db.opt.Dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	fileLock := flock.New(filepath.Join(opt.Dir, FLockName))
	if ok, err := fileLock.TryLock(); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrDirLocked
	}

	db.flock = fileLock

	if bm, err := NewBucketManager(opt.Dir); err == nil {
		db.bm = bm
	} else {
		return nil, err
	}

	if err := db.rebuildBucketManager(); err != nil {
		return nil, fmt.Errorf("db.rebuildBucketManager err:%s", err)
	}

	if err := db.recoverMergeManifest(); err != nil {
		return nil, fmt.Errorf("recover merge manifest: %w", err)
	}

	if err := db.buildIndexes(); err != nil {
		return nil, fmt.Errorf("db.buildIndexes error: %s", err)
	}

	if db.opt.EnableWatch {
		db.wm = NewWatchManager()
		go db.wm.startDistributor()
	} else {
		db.wm = nil
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
		return utils.TarGZCompress(w, db.opt.Dir)
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

	err = db.fm.Close()

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

	db.commitBuffer = nil

	db.tm.close()

	if db.wm != nil {
		if err := db.wm.close(); err != nil {
			log.Printf("watch manager closed already")
		}
	}

	if GCEnable {
		runtime.GC()
	}

	return nil
}

func (db *DB) getValueByRecord(record *data.Record) ([]byte, error) {
	if record == nil {
		return nil, ErrRecordIsNil
	}

	if record.Value != nil {
		return record.Value, nil
	}

	// firstly we find data in cache
	if db.getHintKeyAndRAMIdxCacheSize() > 0 {
		if value := db.hintKeyAndRAMIdxModeLru.Get(record); value != nil {
			return value.(*Entry).Value, nil
		}
	}

	dirPath := getDataPath(record.FileID, db.opt.Dir)
	df, err := db.fm.GetDataFileReadOnly(dirPath, db.opt.SegmentSize)
	if err != nil {
		return nil, err
	}
	defer func(rwManager fileio.RWManager) {
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
	if db.getHintKeyAndRAMIdxCacheSize() > 0 {
		db.hintKeyAndRAMIdxModeLru.Add(record, item)
	}

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

// getSnowflakeNode returns a cached snowflake node, creating it once if needed.
func (db *DB) getSnowflakeNode() *snowflake.Node {
	return db.sm.GetNode()
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

			if len(reqs) >= 3*KvWriteChCapacity {
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

// setActiveFile sets the ActiveFile (DataFile object).
func (db *DB) setActiveFile() (err error) {
	activeFilePath := getDataPath(db.MaxFileID, db.opt.Dir)
	db.ActiveFile, err = db.fm.GetDataFile(activeFilePath, db.opt.SegmentSize)
	if err != nil {
		return
	}

	db.ActiveFile.fileID = db.MaxFileID

	return nil
}

// getMaxFileIDAndFileIds returns max fileId and fileIds.
func (db *DB) getMaxFileIDAndFileIDs() (maxFileID int64, dataFileIds []int64) {
	userIDs, mergeIDs, err := enumerateDataFileIDs(db.opt.Dir)
	if err != nil {
		return 0, nil
	}

	if len(userIDs) > 0 {
		maxFileID = userIDs[len(userIDs)-1]
	}

	dataFileIds = make([]int64, 0, len(userIDs)+len(mergeIDs))
	dataFileIds = append(dataFileIds, userIDs...)
	dataFileIds = append(dataFileIds, mergeIDs...)

	if len(dataFileIds) > 1 {
		sort.Slice(dataFileIds, func(i, j int) bool { return dataFileIds[i] < dataFileIds[j] })
	}

	if len(dataFileIds) == 0 {
		return maxFileID, nil
	}

	return maxFileID, dataFileIds
}

func (db *DB) parseDataFiles(dataFileIds []int64) (err error) {
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
			if _, err := db.bm.GetBucketById(bucketId); errors.Is(err, ErrBucketNotExist) {
				continue
			}

			record := db.createRecordByModeWithFidAndOff(entry.fid, uint64(entry.off), &entry.Entry)

			if err = db.buildIdxes(record, &entry.Entry); err != nil {
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

			entryWhenRecovery := &EntryWhenRecovery{
				Entry: *entry,
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

			off += entry.Size()
		}

		if fID == db.MaxFileID {
			db.ActiveFile.ActualSize = off
			db.ActiveFile.writeOff = off
		}

		return nil
	}

	for _, dataID := range dataFileIds {
		off = 0
		fID = dataID

		// Try to load hint file first if enabled
		if db.opt.EnableHintFile {
			hintLoaded, _ := db.loadHintFile(fID)

			if hintLoaded {
				// Hint file loaded successfully, skip scanning data file
				continue
			}
		}

		// Fall back to scanning data file
		dataPath := getDataPath(fID, db.opt.Dir)
		f, err = newFileRecovery(dataPath, db.opt.BufferSizeOfRecovery)
		if err != nil {
			return err
		}
		err = readEntriesFromFile()
		if err != nil {
			return err
		}
	}

	// compute the valid record count and save it in db.RecordCount
	db.RecordCount, err = db.getRecordCount()
	return
}

// loadHintFile loads a single hint file and rebuilds indexes
func (db *DB) loadHintFile(fid int64) (bool, error) {
	hintPath := getHintPath(fid, db.opt.Dir)

	// Check if hint file exists
	if _, err := os.Stat(hintPath); os.IsNotExist(err) {
		return false, nil // Hint file doesn't exist, need to scan data file
	}

	reader := &HintFileReader{}
	if err := reader.Open(hintPath); err != nil {
		return false, nil
	}
	defer func() {
		if err := reader.Close(); err != nil {
			// Log error but don't fail the operation
			log.Printf("Warning: failed to close hint file reader: %v", err)
		}
	}()

	// Read all hint entries and build indexes
	for {
		hintEntry, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break // End of file
			}
			return false, nil
		}

		if hintEntry == nil {
			continue
		}

		// Check if bucket exists
		bucketId := hintEntry.BucketId
		if _, err := db.bm.GetBucketById(bucketId); errors.Is(err, ErrBucketNotExist) {
			continue // Skip if bucket doesn't exist
		}

		// Create a record from hint entry
		record := data.NewRecord()
		record.WithKey(hintEntry.Key).
			WithFileId(hintEntry.FileID).
			WithDataPos(hintEntry.DataPos).
			WithValueSize(hintEntry.ValueSize).
			WithTimestamp(hintEntry.Timestamp).
			WithTTL(hintEntry.TTL).
			WithTxID(0) // TxID is not stored in hint file

		// Create an entry from hint entry
		entry := NewEntry()
		entry.WithKey(hintEntry.Key)

		// Create metadata
		meta := NewMetaData()
		meta.WithBucketId(hintEntry.BucketId).
			WithKeySize(hintEntry.KeySize).
			WithValueSize(hintEntry.ValueSize).
			WithTimeStamp(hintEntry.Timestamp).
			WithTTL(hintEntry.TTL).
			WithFlag(hintEntry.Flag).
			WithStatus(hintEntry.Status).
			WithDs(hintEntry.Ds).
			WithTxID(0) // TxID is not stored in hint file

		entry.WithMeta(meta)

		// In HintKeyValAndRAMIdxMode, we need to load the value from data file
		if db.opt.EntryIdxMode == HintKeyValAndRAMIdxMode {
			value, err := db.getValueByRecord(record)
			if err != nil {
				// If we can't load the value, we can't use this entry in HintKeyValAndRAMIdxMode
				// Skip this entry and continue
				continue
			}
			entry.WithValue(value)
			record.WithValue(value)
		} else if db.opt.EntryIdxMode == HintKeyAndRAMIdxMode {
			// In HintKeyAndRAMIdxMode, for Set data structure, we also need to load the value
			// because Set uses value hash as the key in its internal map structure
			if hintEntry.Ds == DataStructureSet || hintEntry.Ds == DataStructureSortedSet {
				value, err := db.getValueByRecord(record)
				if err != nil {
					continue
				}
				entry.WithValue(value)
				// Don't set record.WithValue for HintKeyAndRAMIdxMode, as we only need it for index building
			}
		}

		// Build indexes
		if err := db.buildIdxes(record, entry); err != nil {
			continue
		}

		db.KeyCount++
	}

	return true, nil
}

func (db *DB) getRecordCount() (int64, error) {
	var res int64

	// Iterate through the BTree indices
	for _, btree := range db.Index.bTree.idx {
		res += int64(btree.Count())
	}

	// Iterate through the List indices
	for _, listItem := range db.Index.list.idx {
		for key := range listItem.Items {
			curLen, err := listItem.Size(key)
			if err != nil {
				return res, err
			}
			res += int64(curLen)
		}
	}

	// Iterate through the Set indices
	for _, setItem := range db.Index.set.idx {
		for key := range setItem.M {
			res += int64(setItem.SCard(key))
		}
	}

	// Iterate through the SortedSet indices
	for _, zsetItem := range db.Index.sortedSet.idx {
		for key := range zsetItem.M {
			curLen, err := zsetItem.ZCard(key)
			if err != nil {
				return res, err
			}
			res += int64(curLen)
		}
	}

	return res, nil
}

func (db *DB) buildBTreeIdx(record *data.Record, entry *Entry) error {
	key, meta := entry.Key, entry.Meta

	bucket, err := db.bm.GetBucketById(meta.BucketId)
	if err != nil {
		return err
	}
	bucketId := bucket.Id

	bTree := db.Index.bTree.getWithDefault(bucketId)

	if record.IsExpired() || meta.Flag == DataDeleteFlag {
		db.tm.del(bucketId, string(key))
		bTree.Delete(key)
	} else {
		if meta.TTL != Persistent {
			db.tm.add(bucketId, string(key), expireTime(meta.Timestamp, meta.TTL), db.buildExpireCallback(bucket.Name, key))
		} else {
			db.tm.del(bucketId, string(key))
		}
		bTree.InsertRecord(record.Key, record)
	}
	return nil
}

func (db *DB) buildIdxes(record *data.Record, entry *Entry) error {
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

// buildSetIdx builds set index when opening the DB.
func (db *DB) buildSetIdx(record *data.Record, entry *Entry) error {
	key, val, meta := entry.Key, entry.Value, entry.Meta

	bucket, err := db.bm.GetBucketById(entry.Meta.BucketId)
	if err != nil {
		return err
	}
	bucketId := bucket.Id

	s := db.Index.set.getWithDefault(bucketId)

	switch meta.Flag {
	case DataSetFlag:
		if err := s.SAdd(string(key), [][]byte{val}, []*data.Record{record}); err != nil {
			return fmt.Errorf("when build SetIdx SAdd index err: %s", err)
		}
	case DataDeleteFlag:
		if err := s.SRem(string(key), val); err != nil {
			return fmt.Errorf("when build SetIdx SRem index err: %s", err)
		}
	}

	return nil
}

// buildSortedSetIdx builds sorted set index when opening the DB.
func (db *DB) buildSortedSetIdx(record *data.Record, entry *Entry) error {
	key, val, meta := entry.Key, entry.Value, entry.Meta

	bucket, err := db.bm.GetBucketById(entry.Meta.BucketId)
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
			err = ss.ZAdd(key, SCORE(score), val, record)
		}
	case DataZRemFlag:
		_, err = ss.ZRem(string(key), val)
	case DataZRemRangeByRankFlag:
		start, end := splitIntIntStr(string(val), SeparatorForZSetKey)
		err = ss.ZRemRangeByRank(string(key), start, end)
	case DataZPopMaxFlag:
		_, _, err = ss.ZPopMax(string(key))
	case DataZPopMinFlag:
		_, _, err = ss.ZPopMin(string(key))
	}

	// We don't need to panic if sorted set is not found.
	if err != nil && !errors.Is(err, ErrSortedSetNotFound) {
		return fmt.Errorf("when build sortedSetIdx err: %s", err)
	}

	return nil
}

// buildListIdx builds List index when opening the DB.
func (db *DB) buildListIdx(record *data.Record, entry *Entry) error {
	key, val, meta := entry.Key, entry.Value, entry.Meta

	bucket, err := db.bm.GetBucketById(entry.Meta.BucketId)
	if err != nil {
		return err
	}
	bucketId := bucket.Id

	l := db.Index.list.getWithDefault(bucketId)

	if data.IsExpired(meta.TTL, meta.Timestamp) {
		return nil
	}

	switch meta.Flag {
	case DataExpireListFlag:
		t, _ := strconv2.StrToInt64(string(val))
		ttl := uint32(t)
		l.TTL[string(key)] = ttl
		l.TimeStamp[string(key)] = meta.Timestamp
	case DataLPushFlag:
		err = l.LPush(string(key), record)
	case DataRPushFlag:
		err = l.RPush(string(key), record)
	case DataLRemFlag:
		err = db.buildListLRemIdx(val, l, key)
	case DataLPopFlag:
		_, err = l.LPop(string(key))
	case DataRPopFlag:
		_, err = l.RPop(string(key))
	case DataLTrimFlag:
		newKey, start := splitStringIntStr(string(key), SeparatorForListKey)
		end, _ := strconv2.StrToInt(string(val))
		err = l.LTrim(newKey, start, end)
	case DataLRemByIndex:
		indexes, _ := utils.UnmarshalInts(val)
		err = l.LRemByIndex(string(key), indexes)
	}

	if err != nil {
		return fmt.Errorf("when build listIdx err: %s", err)
	}

	return nil
}

func (db *DB) buildListLRemIdx(value []byte, l *data.List, key []byte) error {
	count, newValue := splitIntStringStr(string(value), SeparatorForListKey)

	return l.LRem(string(key), count, func(r *data.Record) (bool, error) {
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
		dataFileIds []int64
	)

	maxFileID, dataFileIds = db.getMaxFileIDAndFileIDs()

	// init db.ActiveFile
	db.MaxFileID = maxFileID

	// set ActiveFile
	if err = db.setActiveFile(); err != nil {
		return
	}

	if len(dataFileIds) == 0 {
		return
	}

	// build hint index
	return db.parseDataFiles(dataFileIds)
}

func (db *DB) createRecordByModeWithFidAndOff(fid int64, off uint64, entry *Entry) *data.Record {
	record := data.NewRecord()

	record.WithKey(entry.Key).
		WithTimestamp(entry.Meta.Timestamp).
		WithTTL(entry.Meta.TTL).
		WithTxID(entry.Meta.TxID)

	if db.opt.EntryIdxMode == HintKeyValAndRAMIdxMode {
		record.WithValue(entry.Value)
	}

	if db.opt.EntryIdxMode == HintKeyAndRAMIdxMode {
		record.WithFileId(fid).
			WithDataPos(off).
			WithValueSize(uint32(len(entry.Value)))
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
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic when executing tx, err is %+v", r)
		}
	}()

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
	req.IncrRef()     // for db write
	db.writeCh <- req // Handled in doWrites.
	return req, nil
}

func (db *DB) checkListExpired() {
	db.Index.list.rangeIdx(func(l *data.List) {
		for key := range l.TTL {
			l.IsExpire(key)
		}
	})
}

// IsClose return the value that represents the status of DB
func (db *DB) IsClose() bool {
	return db.closed
}

func (db *DB) buildExpireCallback(bucket string, key []byte) func() {
	return func() {
		_ = db.Update(func(tx *Tx) error {
			b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
			if err != nil {
				return err
			}
			bucketId := b.Id
			if db.tm.exist(bucketId, string(key)) {
				return tx.Delete(bucket, key)
			}
			return nil
		})
	}
}

func (db *DB) rebuildBucketManager() error {
	bucketFilePath := db.opt.Dir + "/" + BucketStoreFileName
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
			name:   BucketName(bucket.Name),
			bucket: bucket,
		})
	}

	if len(bucketRequest) > 0 {
		err = db.bm.SubmitPendingBucketChange(bucketRequest)
		if err != nil {
			return err
		}
	}
	return nil
}

/**
 * Watch watches the key and bucket and calls the callback function for each message received.
 * The callback will be called once for each individual message in the batch.
 *
 * @param bucket - the bucket name to watch
 * @param key - the key in the bucket to watch
 * @param cb - the callback function to call for each message received
 * @param opts - the options for the watch
 *   - CallbackTimeout - the timeout for the callback, default is 1 second
 *
 * @return error - the error if the watch is stopped
 */
func (db *DB) Watch(bucket string, key []byte, cb func(message *Message) error, opts ...WatchOptions) error {
	watchOpts := NewWatchOptions()

	if len(opts) > 0 {
		watchOpts = &opts[0]
	}

	if db.wm == nil {
		return ErrWatchFeatureDisabled
	}

	subscriber, err := db.wm.subscribe(bucket, string(key))
	if err != nil {
		return err
	}

	maxBatchSize := 128
	batch := make([]*Message, 0, maxBatchSize)

	// Use a ticker to process the batch every 100 milliseconds
	// Avoid CPU busy spinning
	ticker := time.NewTicker(100 * time.Millisecond)
	keyWatch := string(key)

	processBatch := func(batch []*Message) error {
		if len(batch) == 0 {
			return nil
		}

		for _, msg := range batch {
			errChan := make(chan error, 1)
			go func(msg *Message) {
				errChan <- cb(msg)
			}(msg)

			select {
			case <-time.After(watchOpts.CallbackTimeout):
				return ErrWatchingCallbackTimeout
			case err := <-errChan:
				if err != nil {
					return err
				}
			}
		}
		return nil
	}

	defer func() {
		ticker.Stop()

		if subscriber != nil && subscriber.active.Load() {
			if err := db.wm.unsubscribe(bucket, keyWatch, subscriber.id); err != nil {
				// ignore the error
			}
		}
	}()

	for {
		select {
		case <-db.wm.done():
			// drain the batch
			if err := processBatch(batch); err != nil {
				return err
			}
			return nil
		case message, ok := <-subscriber.receiveChan:
			if !ok {
				if err := processBatch(batch); err != nil {
					return err
				}

				return nil
			}

			batch = append(batch, message)

			if len(batch) >= maxBatchSize {
				if err := processBatch(batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		case <-ticker.C:
			for len(batch) > 0 {
				if err := processBatch(batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		}
	}
}
