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
	"io/ioutil"
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

const (
	// DataDeleteFlag represents the data delete flag
	DataDeleteFlag uint16 = iota

	// DataSetFlag represents the data set flag
	DataSetFlag

	// DataLPushFlag represents the data LPush flag
	DataLPushFlag

	// DataRPushFlag represents the data RPush flag
	DataRPushFlag

	// DataLRemFlag represents the data LRem flag
	DataLRemFlag

	// DataLPopFlag represents the data LPop flag
	DataLPopFlag

	// DataRPopFlag represents the data RPop flag
	DataRPopFlag

	// DataLSetFlag represents the data LSet flag
	DataLSetFlag

	// DataLTrimFlag represents the data LTrim flag
	DataLTrimFlag

	// DataZAddFlag represents the data ZAdd flag
	DataZAddFlag

	// DataZRemFlag represents the data ZRem flag
	DataZRemFlag

	// DataZRemRangeByRankFlag represents the data ZRemRangeByRank flag
	DataZRemRangeByRankFlag

	// DataZPopMaxFlag represents the data ZPopMax flag
	DataZPopMaxFlag

	// DataZPopMinFlag represents the data aZPopMin flag
	DataZPopMinFlag

	// DataSetBucketDeleteFlag represents the delete Set bucket flag
	DataSetBucketDeleteFlag

	// DataSortedSetBucketDeleteFlag represents the delete Sorted Set bucket flag
	DataSortedSetBucketDeleteFlag

	// DataBPTreeBucketDeleteFlag represents the delete BPTree bucket flag
	DataBPTreeBucketDeleteFlag

	// DataListBucketDeleteFlag represents the delete List bucket flag
	DataListBucketDeleteFlag

	// LRemByIndex represents the data LRemByIndex flag
	DataLRemByIndex

	// DataListBucketDeleteFlag represents that set ttl for the list
	DataExpireListFlag
)

const (
	// UnCommitted represents the tx unCommitted status
	UnCommitted uint16 = 0

	// Committed represents the tx committed status
	Committed uint16 = 1

	// Persistent represents the data persistent flag
	Persistent uint32 = 0

	// ScanNoLimit represents the data scan no limit flag
	ScanNoLimit int = -1

	KvWriteChCapacity = 1000
)

const (
	// DataStructureSet represents the data structure set flag
	DataStructureSet uint16 = iota

	// DataStructureSortedSet represents the data structure sorted set flag
	DataStructureSortedSet

	// DataStructureTree represents the data structure b+ tree or b tree flag
	DataStructureTree

	// DataStructureList represents the data structure list flag
	DataStructureList

	// DataStructureNone represents not the data structure
	DataStructureNone
)

const FLockName = "nutsdb-flock"

type (
	// DB represents a collection of buckets that persist on disk.
	DB struct {
		opt                     Options // the database options
		BTreeIdx                BTreeIdx
		BPTreeRootIdxes         []*BPTreeRootIdx
		BPTreeKeyEntryPosMap    map[string]int64 // key = bucket+key  val = EntryPos
		bucketMetas             BucketMetasIdx
		SetIdx                  SetIdx
		SortedSetIdx            SortedSetIdx
		Index                   *index
		ActiveFile              *DataFile
		ActiveBPTreeIdx         *BPTree
		ActiveCommittedTxIdsIdx *BPTree
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
	}

	// BucketMetasIdx represents the index of the bucket's meta-information
	BucketMetasIdx map[string]*BucketMeta
)

// open returns a newly initialized DB object.
func open(opt Options) (*DB, error) {
	db := &DB{
		//BPTreeIdx:               make(BPTreeIdx),
		BTreeIdx:                make(BTreeIdx),
		SetIdx:                  make(SetIdx),
		SortedSetIdx:            make(SortedSetIdx),
		ActiveBPTreeIdx:         NewTree(),
		MaxFileID:               0,
		opt:                     opt,
		KeyCount:                0,
		closed:                  false,
		BPTreeKeyEntryPosMap:    make(map[string]int64),
		bucketMetas:             make(map[string]*BucketMeta),
		ActiveCommittedTxIdsIdx: NewTree(),
		Index:                   NewIndex(),
		fm:                      newFileManager(opt.RWMode, opt.MaxFdNumsInCache, opt.CleanFdsCacheThreshold),
		mergeStartCh:            make(chan struct{}),
		mergeEndCh:              make(chan error),
		mergeWorkCloseCh:        make(chan struct{}),
		writeCh:                 make(chan *request, KvWriteChCapacity),
		tm:                      newTTLManager(opt.ExpiredDeleteType),
	}

	commitBuffer := new(bytes.Buffer)
	commitBuffer.Grow(int(db.opt.CommitBufferSize))
	db.commitBuffer = commitBuffer

	if ok := filesystem.PathIsExist(db.opt.Dir); !ok {
		if err := os.MkdirAll(db.opt.Dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	flock := flock.New(filepath.Join(opt.Dir, FLockName))
	if ok, err := flock.TryLock(); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrDirLocked
	}

	db.flock = flock

	if err := db.checkEntryIdxMode(); err != nil {
		return nil, err
	}

	if opt.EntryIdxMode == HintBPTSparseIdxMode {
		for _, subDir := range []string{
			path.Join(db.opt.Dir, bptDir, "root"),
			path.Join(db.opt.Dir, bptDir, "txid"),
			path.Join(db.opt.Dir, "meta/bucket"),
		} {
			if err := createDirIfNotExist(subDir); err != nil {
				return nil, err
			}
		}
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

func (db *DB) checkEntryIdxMode() error {
	hasDataFlag := false
	hasBptDirFlag := false

	files, err := ioutil.ReadDir(db.opt.Dir)
	if err != nil {
		return err
	}

	for _, f := range files {
		id := f.Name()

		fileSuffix := path.Ext(path.Base(id))
		if fileSuffix == DataSuffix {
			hasDataFlag = true
			if hasBptDirFlag {
				break
			}
		}

		if id == bptDir {
			hasBptDirFlag = true
		}
	}

	if db.opt.EntryIdxMode != HintBPTSparseIdxMode && hasDataFlag && hasBptDirFlag {
		return errors.New("not support HintBPTSparseIdxMode switch to the other EntryIdxMode")
	}

	if db.opt.EntryIdxMode == HintBPTSparseIdxMode && !hasBptDirFlag && hasDataFlag {
		return errors.New("not support the other EntryIdxMode switch to HintBPTSparseIdxMode")
	}

	return nil
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

	db.BTreeIdx = nil

	db.BPTreeKeyEntryPosMap = nil

	db.bucketMetas = nil

	db.SetIdx = nil

	db.SortedSetIdx = nil

	db.Index = nil

	db.ActiveFile = nil

	db.ActiveBPTreeIdx = nil

	db.ActiveCommittedTxIdsIdx = nil

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

	db = nil

	if GCEnable {
		runtime.GC()
	}

	return nil
}

func (db *DB) getValueByRecord(r *Record) ([]byte, error) {
	if r == nil {
		return nil, errors.New("the record is nil")
	}

	if r.V != nil {
		return r.V, nil
	}

	e, err := db.getEntryByHint(r.H)
	if err != nil {
		return nil, err
	}

	return e.Value, nil
}

func (db *DB) getEntryByHint(h *Hint) (*Entry, error) {
	dirPath := getDataPath(h.FileID, db.opt.Dir)
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

	payloadSize := h.Meta.PayloadSize()
	item, err := df.ReadRecord(int(h.DataPos), payloadSize)
	if err != nil {
		return nil, fmt.Errorf("read err. pos %d, key %s, err %s", h.DataPos, string(h.Key), err)
	}

	return item, nil
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
			//log.Fatal("panicked=", panicked, ", err=", err)
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
		//log.Fatal("txCommit fail,err=", err)
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
				pendingCh <- struct{}{} // Push to pending before doing a write.
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
	filepath := getDataPath(db.MaxFileID, db.opt.Dir)
	db.ActiveFile, err = db.fm.getDataFile(filepath, db.opt.SegmentSize)
	if err != nil {
		return
	}

	db.ActiveFile.fileID = db.MaxFileID

	return nil
}

// getMaxFileIDAndFileIds returns max fileId and fileIds.
func (db *DB) getMaxFileIDAndFileIDs() (maxFileID int64, dataFileIds []int) {
	files, _ := ioutil.ReadDir(db.opt.Dir)
	if len(files) == 0 {
		return 0, nil
	}

	for _, f := range files {
		id := f.Name()
		fileSuffix := path.Ext(path.Base(id))
		if fileSuffix != DataSuffix {
			continue
		}

		id = strings.TrimSuffix(id, DataSuffix)
		idVal, _ := strconv2.StrToInt(id)
		dataFileIds = append(dataFileIds, idVal)
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

			if entry.Meta.Status == Committed {
				meta := NewMetaData().WithFlag(DataSetFlag)
				h := NewHint().WithMeta(meta)
				err := db.ActiveCommittedTxIdsIdx.Insert(entry.GetTxIDBytes(), nil, h, CountFlagEnabled)
				if err != nil {
					return fmt.Errorf("can not ingest the hint obj to ActiveCommittedTxIdsIdx, err: %s", err.Error())
				}
			}

			h := NewHint().WithKey(entry.Key).WithFileId(entry.fid).WithMeta(entry.Meta).WithDataPos(uint64(entry.off))
			r := NewRecord().WithBucket(entry.GetBucketString()).WithValue(entry.Value).WithHint(h)

			if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
				db.BPTreeKeyEntryPosMap[string(getNewKey(string(entry.Bucket), entry.Key))] = off
			}

			if r.H.Meta.Ds == DataStructureTree {
				r.H.Meta.Status = Committed

				// only if in HintKeyValAndRAMIdxMode, set the value of record
				db.resetRecordByMode(r)

				if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
					if err = db.buildActiveBPTreeIdx(r); err != nil {
						return err
					}
				} else {
					db.buildBTreeIdx(r)
				}
			} else {
				if r.H.Meta.Ds == DataStructureNone {
					db.buildNotDSIdxes(r)
				} else {
					if err = db.buildOtherIdxes(r); err != nil {
						return err
					}
				}
			}

			db.KeyCount++

		}
		return nil
	}

	var readEntriesFromFile = func() error {
		for {
			entry, err := f.readEntry()
			if err != nil {
				// whatever which logic branch it will choose, we will release the fd.
				_ = f.release()
				if errors.Is(err, io.EOF) || errors.Is(err, ErrIndexOutOfBound) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrEntryZero) {
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

	if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		dataFileIds = dataFileIds[len(dataFileIds)-1:]
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

	if HintBPTSparseIdxMode == db.opt.EntryIdxMode {
		if err = db.buildBPTreeRootIdxes(dataFileIds); err != nil {
			return err
		}
	}

	return
}

func (db *DB) buildBPTreeRootIdxes(dataFileIds []int) error {
	var off int64

	dataFileIdsSize := len(dataFileIds)

	if dataFileIdsSize == 1 {
		return nil
	}

	for i := 0; i < len(dataFileIds[0:dataFileIdsSize-1]); i++ {
		off = 0
		path := getBPTRootPath(int64(dataFileIds[i]), db.opt.Dir)
		fd, err := os.OpenFile(filepath.Clean(path), os.O_RDWR, os.ModePerm)
		if err != nil {
			return err
		}

		for {
			bs, err := ReadBPTreeRootIdxAt(fd, off)

			if err != nil {
				if err == io.EOF || err == nil && bs == nil {
					break
				}
				return err
			}

			if err == nil && bs != nil {
				db.BPTreeRootIdxes = append(db.BPTreeRootIdxes, bs)
				off += bs.Size()
			}

		}

		fd.Close()
	}

	return nil
}

func (db *DB) buildBTreeIdx(r *Record) {
	if r.IsExpired() {
		return
	}

	bucket, key, meta := r.Bucket, r.H.Key, r.H.Meta

	if _, ok := db.BTreeIdx[bucket]; !ok {
		db.BTreeIdx[bucket] = NewBTree()
	}

	if meta.Flag == DataDeleteFlag {
		db.tm.del(bucket, string(key))
		db.BTreeIdx[bucket].Delete(key)
	} else {
		if meta.TTL != Persistent {
			now := time.UnixMilli(time.Now().UnixMilli())
			expireTime := time.UnixMilli(int64(meta.Timestamp))
			expireTime = expireTime.Add(time.Duration(int64(meta.TTL)) * time.Second)
			expire := expireTime.Sub(now)

			callback := func() {
				err := db.Update(func(tx *Tx) error {
					if tx.db.tm.exist(bucket, string(key)) {
						return tx.Delete(bucket, key)
					}
					return nil
				})
				if err != nil {
					log.Printf("occur error when expired deletion, error: %v", err.Error())
				}
			}

			db.tm.add(bucket, string(key), expire, callback)
		} else {
			db.tm.del(bucket, string(key))
		}

		db.BTreeIdx[bucket].Insert(key, r.V, r.H)
	}
}

func (db *DB) buildActiveBPTreeIdx(r *Record) error {
	newKey := getNewKey(r.Bucket, r.H.Key)
	if err := db.ActiveBPTreeIdx.Insert(newKey, r.V, r.H, CountFlagEnabled); err != nil {
		return fmt.Errorf("when build BPTreeIdx insert index err: %s", err)
	}

	return nil
}

func (db *DB) buildBucketMetaIdx() error {
	if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		files, err := ioutil.ReadDir(getBucketMetaPath(db.opt.Dir))
		if err != nil {
			return err
		}

		if len(files) != 0 {
			for _, f := range files {
				name := f.Name()
				fileSuffix := path.Ext(path.Base(name))
				if fileSuffix != BucketMetaSuffix {
					continue
				}

				name = strings.TrimSuffix(name, BucketMetaSuffix)

				bucketMeta, err := ReadBucketMeta(getBucketMetaFilePath(name, db.opt.Dir))
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}

				db.bucketMetas[name] = bucketMeta
			}
		}
	}

	return nil
}

func (db *DB) buildOtherIdxes(r *Record) error {
	switch r.H.Meta.Ds {
	case DataStructureList:
		if err := db.buildListIdx(r); err != nil {
			return err
		}
	case DataStructureSet:
		if err := db.buildSetIdx(r); err != nil {
			return err
		}
	case DataStructureSortedSet:
		if err := db.buildSortedSetIdx(r); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) buildNotDSIdxes(r *Record) {
	if r.H.Meta.Flag == DataSetBucketDeleteFlag {
		db.deleteBucket(DataStructureSet, r.Bucket)
	}
	if r.H.Meta.Flag == DataSortedSetBucketDeleteFlag {
		db.deleteBucket(DataStructureSortedSet, r.Bucket)
	}
	if r.H.Meta.Flag == DataBPTreeBucketDeleteFlag {
		db.deleteBucket(DataStructureTree, r.Bucket)
	}
	if r.H.Meta.Flag == DataListBucketDeleteFlag {
		db.deleteBucket(DataStructureList, r.Bucket)
	}
}

func (db *DB) deleteBucket(ds uint16, bucket string) {
	if ds == DataStructureSet {
		delete(db.SetIdx, bucket)
	}
	if ds == DataStructureSortedSet {
		delete(db.SortedSetIdx, bucket)
	}
	if ds == DataStructureTree {
		delete(db.BTreeIdx, bucket)
	}
	if ds == DataStructureList {
		db.Index.deleteList(bucket)
	}
}

// buildSetIdx builds set index when opening the DB.
func (db *DB) buildSetIdx(r *Record) error {
	bucket, key, val, meta := r.Bucket, r.H.Key, r.V, r.H.Meta
	db.resetRecordByMode(r)

	if _, ok := db.SetIdx[bucket]; !ok {
		db.SetIdx[bucket] = NewSet()
	}

	switch meta.Flag {
	case DataSetFlag:
		if err := db.SetIdx[bucket].SAdd(string(key), [][]byte{val}, []*Record{r}); err != nil {
			return fmt.Errorf("when build SetIdx SAdd index err: %s", err)
		}
	case DataDeleteFlag:
		if err := db.SetIdx[bucket].SRem(string(key), val); err != nil {
			return fmt.Errorf("when build SetIdx SRem index err: %s", err)
		}
	}

	return nil
}

// buildSortedSetIdx builds sorted set index when opening the DB.
func (db *DB) buildSortedSetIdx(r *Record) error {
	bucket, key, val, meta := r.Bucket, r.H.Key, r.V, r.H.Meta
	db.resetRecordByMode(r)

	if _, ok := db.SortedSetIdx[bucket]; !ok {
		db.SortedSetIdx[bucket] = NewSortedSet(db)
	}

	var err error

	switch meta.Flag {
	case DataZAddFlag:
		keyAndScore := strings.Split(string(key), SeparatorForZSetKey)
		if len(keyAndScore) == 2 {
			key := keyAndScore[0]
			score, _ := strconv2.StrToFloat64(keyAndScore[1])
			err = db.SortedSetIdx[bucket].ZAdd(key, SCORE(score), val, r)
		}
	case DataZRemFlag:
		_, err = db.SortedSetIdx[bucket].ZRem(string(key), val)
	case DataZRemRangeByRankFlag:
		startAndEnd := strings.Split(string(val), SeparatorForZSetKey)
		start, _ := strconv2.StrToInt(startAndEnd[0])
		end, _ := strconv2.StrToInt(startAndEnd[1])
		err = db.SortedSetIdx[bucket].ZRemRangeByRank(string(key), start, end)
	case DataZPopMaxFlag:
		_, _, err = db.SortedSetIdx[bucket].ZPopMax(string(key))
	case DataZPopMinFlag:
		_, _, err = db.SortedSetIdx[bucket].ZPopMin(string(key))
	}

	if err != nil {
		return fmt.Errorf("when build sortedSetIdx err: %s", err)
	}

	return nil
}

// buildListIdx builds List index when opening the DB.
func (db *DB) buildListIdx(r *Record) error {
	bucket, key, val, meta := r.Bucket, r.H.Key, r.V, r.H.Meta
	db.resetRecordByMode(r)

	l := db.Index.getList(bucket)

	if IsExpired(meta.TTL, meta.Timestamp) {
		return nil
	}

	var err error

	switch meta.Flag {
	case DataExpireListFlag:
		t, _ := strconv2.StrToInt64(string(val))
		ttl := uint32(t)
		l.TTL[string(key)] = ttl
		l.TimeStamp[string(key)] = meta.Timestamp
	case DataLPushFlag:
		err = l.LPush(string(key), r)
	case DataRPushFlag:
		err = l.RPush(string(key), r)
	case DataLRemFlag:
		countAndValueIndex := strings.Split(string(val), SeparatorForListKey)
		count, _ := strconv2.StrToInt(countAndValueIndex[0])
		value := []byte(countAndValueIndex[1])

		err = l.LRem(string(key), count, func(r *Record) (bool, error) {
			v, err := db.getValueByRecord(r)
			if err != nil {
				return false, err
			}
			return bytes.Equal(value, v), nil
		})
	case DataLPopFlag:
		_, err = l.LPop(string(key))
	case DataRPopFlag:
		_, err = l.RPop(string(key))
	case DataLSetFlag:
		keyAndIndex := strings.Split(string(key), SeparatorForListKey)
		newKey := keyAndIndex[0]
		index, _ := strconv2.StrToInt(keyAndIndex[1])
		err = l.LSet(newKey, index, r)
	case DataLTrimFlag:
		keyAndStartIndex := strings.Split(string(key), SeparatorForListKey)
		newKey := keyAndStartIndex[0]
		start, _ := strconv2.StrToInt(keyAndStartIndex[1])
		end, _ := strconv2.StrToInt(string(val))
		err = l.LTrim(newKey, start, end)
	case DataLRemByIndex:
		var indexes []int
		indexes, err = UnmarshalInts(val)
		if err != nil {
			break
		}
		err = l.LRemByIndex(string(key), indexes)
	}

	if err != nil {
		return fmt.Errorf("when build listIdx err: %s", err)
	}

	return nil
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

	if err = db.buildBucketMetaIdx(); err != nil {
		return
	}

	// build hint index
	return db.parseDataFiles(dataFileIds)
}

func (db *DB) resetRecordByMode(record *Record) {
	if db.opt.EntryIdxMode != HintKeyValAndRAMIdxMode {
		record.V = nil
	}
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

		errRollback := tx.Rollback()
		err = fmt.Errorf("%v. Rollback err: %v", err, errRollback)
	}

	return err
}

const bptDir = "bpt"

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
	db.Index.rangeList(func(l *List) {
		for key := range l.TTL {
			l.IsExpire(key)
		}
	})
}

// IsClose return the value that represents the status of DB
func (db *DB) IsClose() bool {
	return db.closed
}
