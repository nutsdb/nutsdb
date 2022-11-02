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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/xujiajun/nutsdb/ds/list"
	"github.com/xujiajun/nutsdb/ds/set"
	"github.com/xujiajun/nutsdb/ds/zset"
	"github.com/xujiajun/utils/filesystem"
	"github.com/xujiajun/utils/strconv2"
)

var (
	// ErrDBClosed is returned when db is closed.
	ErrDBClosed = errors.New("db is closed")

	// ErrBucket is returned when bucket is not in the HintIdx.
	ErrBucket = errors.New("err bucket")

	// ErrEntryIdxModeOpt is returned when set db EntryIdxMode option is wrong.
	ErrEntryIdxModeOpt = errors.New("err EntryIdxMode option set")

	// ErrFn is returned when fn is nil.
	ErrFn = errors.New("err fn")

	// ErrBucketNotFound is returned when looking for bucket that does not exist
	ErrBucketNotFound = errors.New("bucket not found")

	// ErrNotSupportHintBPTSparseIdxMode is returned not support mode `HintBPTSparseIdxMode`
	ErrNotSupportHintBPTSparseIdxMode = errors.New("not support mode `HintBPTSparseIdxMode`")
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
)

const (
	// DataStructureSet represents the data structure set flag
	DataStructureSet uint16 = iota

	// DataStructureSortedSet represents the data structure sorted set flag
	DataStructureSortedSet

	// DataStructureBPTree represents the data structure b+ tree flag
	DataStructureBPTree

	// DataStructureList represents the data structure list flag
	DataStructureList

	// DataStructureNone represents not the data structure
	DataStructureNone
)

type (
	// DB represents a collection of buckets that persist on disk.
	DB struct {
		opt                     Options   // the database options
		BPTreeIdx               BPTreeIdx // Hint Index
		BPTreeRootIdxes         []*BPTreeRootIdx
		BPTreeKeyEntryPosMap    map[string]int64 // key = bucket+key  val = EntryPos
		bucketMetas             BucketMetasIdx
		SetIdx                  SetIdx
		SortedSetIdx            SortedSetIdx
		ListIdx                 ListIdx
		ActiveFile              *DataFile
		ActiveBPTreeIdx         *BPTree
		ActiveCommittedTxIdsIdx *BPTree
		committedTxIds          map[uint64]struct{}
		MaxFileID               int64
		mu                      sync.RWMutex
		KeyCount                int // total key number ,include expired, deleted, repeated.
		closed                  bool
		isMerging               bool
		fm                      *fileManager
	}

	// BPTreeIdx represents the B+ tree index
	BPTreeIdx map[string]*BPTree

	// SetIdx represents the set index
	SetIdx map[string]*set.Set

	// SortedSetIdx represents the sorted set index
	SortedSetIdx map[string]*zset.SortedSet

	// ListIdx represents the list index
	ListIdx map[string]*list.List

	// Entries represents entries
	Entries []*Entry

	// BucketMetasIdx represents the index of the bucket's meta-information
	BucketMetasIdx map[string]*BucketMeta
)

// open returns a newly initialized DB object.
func open(opt Options) (*DB, error) {
	db := &DB{
		BPTreeIdx:               make(BPTreeIdx),
		SetIdx:                  make(SetIdx),
		SortedSetIdx:            make(SortedSetIdx),
		ListIdx:                 make(ListIdx),
		ActiveBPTreeIdx:         NewTree(),
		MaxFileID:               0,
		opt:                     opt,
		KeyCount:                0,
		closed:                  false,
		committedTxIds:          make(map[uint64]struct{}),
		BPTreeKeyEntryPosMap:    make(map[string]int64),
		bucketMetas:             make(map[string]*BucketMeta),
		ActiveCommittedTxIdsIdx: NewTree(),
		fm:                      newFileManager(opt.RWMode, opt.MaxFdNumsInCache, opt.CleanFdsCacheThreshold),
	}

	if ok := filesystem.PathIsExist(db.opt.Dir); !ok {
		if err := os.MkdirAll(db.opt.Dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	if err := db.checkEntryIdxMode(); err != nil {
		return nil, err
	}

	if opt.EntryIdxMode == HintBPTSparseIdxMode {
		bptRootIdxDir := db.opt.Dir + "/" + bptDir + "/root"
		if ok := filesystem.PathIsExist(bptRootIdxDir); !ok {
			if err := os.MkdirAll(bptRootIdxDir, os.ModePerm); err != nil {
				return nil, err
			}
		}

		bptTxIDIdxDir := db.opt.Dir + "/" + bptDir + "/txid"
		if ok := filesystem.PathIsExist(bptTxIDIdxDir); !ok {
			if err := os.MkdirAll(bptTxIDIdxDir, os.ModePerm); err != nil {
				return nil, err
			}
		}

		bucketMetaDir := db.opt.Dir + "/meta/bucket"
		if ok := filesystem.PathIsExist(bucketMetaDir); !ok {
			if err := os.MkdirAll(bucketMetaDir, os.ModePerm); err != nil {
				return nil, err
			}
		}
	}

	if err := db.buildIndexes(); err != nil {
		return nil, fmt.Errorf("db.buildIndexes error: %s", err)
	}

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

// Merge removes dirty data and reduce data redundancy,following these steps:
//
// 1. Filter delete or expired entry.
//
// 2. Write entry to activeFile if the key not exist，if exist miss this write operation.
//
// 3. Filter the entry which is committed.
//
// 4. At last remove the merged files.
//
// Caveat: Merge is Called means starting multiple write transactions, and it
// will affect the other write request. so execute it at the appropriate time.
func (db *DB) Merge() error {
	var (
		off                 int64
		pendingMergeFIds    []int
		pendingMergeEntries []*Entry
	)

	if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		return ErrNotSupportHintBPTSparseIdxMode
	}

	db.isMerging = true

	_, pendingMergeFIds = db.getMaxFileIDAndFileIDs()

	if len(pendingMergeFIds) < 2 {
		db.isMerging = false
		return errors.New("the number of files waiting to be merged is at least 2")
	}

	for _, pendingMergeFId := range pendingMergeFIds {
		off = 0
		f, err := db.fm.getDataFile(db.getDataPath(int64(pendingMergeFId)), db.opt.SegmentSize)
		if err != nil {
			db.isMerging = false
			return err
		}

		pendingMergeEntries = []*Entry{}

		for {
			if entry, err := f.ReadAt(int(off)); err == nil {
				if entry == nil {
					break
				}

				var skipEntry bool

				if db.isFilterEntry(entry) {
					skipEntry = true
				}

				// check if we have a new entry with same key and bucket
				if r, _ := db.getRecordFromKey(entry.Meta.Bucket, entry.Key); r != nil && !skipEntry {
					if r.H.FileID > int64(pendingMergeFId) {
						skipEntry = true
					} else if r.H.FileID == int64(pendingMergeFId) && r.H.DataPos > uint64(off) {
						skipEntry = true
					}
				}

				if skipEntry {
					off += entry.Size()
					if off >= db.opt.SegmentSize {
						break
					}
					continue
				}

				pendingMergeEntries = db.getPendingMergeEntries(entry, pendingMergeEntries)

				off += entry.Size()
				if off >= db.opt.SegmentSize {
					break
				}

			} else {
				if err == io.EOF {
					break
				}
				if err == ErrIndexOutOfBound {
					break
				}
				_ = f.rwManager.Release()
				return fmt.Errorf("when merge operation build hintIndex readAt err: %s", err)
			}
		}

		if err := db.reWriteData(pendingMergeEntries); err != nil {
			_ = f.rwManager.Release()
			return err
		}

		path := db.getDataPath(int64(pendingMergeFId))
		_ = f.rwManager.Release()
		err = f.rwManager.Close()
		if err != nil {
			return err
		}
		if err := os.Remove(path); err != nil {
			db.isMerging = false
			return fmt.Errorf("when merge err: %s", err)
		}
	}

	return nil
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

	err := db.ActiveFile.rwManager.Release()
	if err != nil {
		return err
	}

	db.ActiveFile = nil

	db.BPTreeIdx = nil

	err = db.fm.close()

	if err != nil {
		return err
	}

	return nil
}

// setActiveFile sets the ActiveFile (DataFile object).
func (db *DB) setActiveFile() (err error) {
	filepath := db.getDataPath(db.MaxFileID)
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

	maxFileID = 0

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

// getActiveFileWriteOff returns the write-offset of activeFile.
func (db *DB) getActiveFileWriteOff() (off int64, err error) {
	off = 0
	for {
		if item, err := db.ActiveFile.ReadAt(int(off)); err == nil {
			if item == nil {
				break
			}

			off += item.Size()
			// set ActiveFileActualSize
			db.ActiveFile.ActualSize = off

		} else {
			if err == io.EOF {
				break
			}
			if err == ErrIndexOutOfBound {
				break
			}

			return -1, fmt.Errorf("when build activeDataIndex readAt err: %s", err)
		}
	}

	return
}

func (db *DB) parseDataFiles(dataFileIds []int) (unconfirmedRecords []*Record, committedTxIds map[uint64]struct{}, err error) {
	var (
		off int64
		e   *Entry
	)

	committedTxIds = make(map[uint64]struct{})

	if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		dataFileIds = dataFileIds[len(dataFileIds)-1:]
	}

	for _, dataID := range dataFileIds {
		off = 0
		fID := int64(dataID)
		path := db.getDataPath(fID)
		f, err := newFileRecovery(path, db.opt.BufferSizeOfRecovery)
		if err != nil {
			return nil, nil, err
		}

		for {
			if entry, err := f.readEntry(); err == nil {
				if entry == nil {
					break
				}

				e = nil
				if db.opt.EntryIdxMode == HintKeyValAndRAMIdxMode {
					e = &Entry{
						Key:   entry.Key,
						Value: entry.Value,
						Meta:  entry.Meta,
					}
				}

				if entry.Meta.Status == Committed {
					committedTxIds[entry.Meta.TxID] = struct{}{}
					db.ActiveCommittedTxIdsIdx.Insert([]byte(strconv2.Int64ToStr(int64(entry.Meta.TxID))), nil,
						&Hint{Meta: &MetaData{Flag: DataSetFlag}}, CountFlagEnabled)
				}

				unconfirmedRecords = append(unconfirmedRecords, &Record{
					H: &Hint{
						Key:     entry.Key,
						FileID:  fID,
						Meta:    entry.Meta,
						DataPos: uint64(off),
					},
					E: e,
				})

				if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
					db.BPTreeKeyEntryPosMap[string(getNewKey(string(entry.Meta.Bucket), entry.Key))] = off
				}

				off += entry.Size()

			} else {
				if err == io.EOF {
					break
				}
				if err == ErrIndexOutOfBound {
					break
				}
				if off >= db.opt.SegmentSize {
					break
				}
				if err != nil {
					return nil, nil, err
				}
				return nil, nil, fmt.Errorf("when build hintIndex readAt err: %s", err)
			}
		}

		if err != nil {
			return nil, nil, err
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
		path := db.getBPTRootPath(int64(dataFileIds[i]))
		fd, err := os.OpenFile(filepath.Clean(path), os.O_RDWR, os.ModePerm)
		if err != nil {
			return err
		}

		for {
			bs, err := ReadBPTreeRootIdxAt(fd, off)
			if err == io.EOF || err == nil && bs == nil {
				break
			}
			if err != nil {
				return err
			}

			if err == nil && bs != nil {
				db.BPTreeRootIdxes = append(db.BPTreeRootIdxes, bs)
				off += bs.Size()
			}

		}

		fd.Close()
	}

	db.committedTxIds = nil

	return nil
}

func (db *DB) buildBPTreeIdx(bucket string, r *Record) error {
	if _, ok := db.BPTreeIdx[bucket]; !ok {
		db.BPTreeIdx[bucket] = NewTree()
	}

	if err := db.BPTreeIdx[bucket].Insert(r.H.Key, r.E, r.H, CountFlagEnabled); err != nil {
		return fmt.Errorf("when build BPTreeIdx insert index err: %s", err)
	}

	return nil
}

func (db *DB) buildActiveBPTreeIdx(r *Record) error {
	newKey := getNewKey(string(r.H.Meta.Bucket), r.H.Key)
	if err := db.ActiveBPTreeIdx.Insert(newKey, r.E, r.H, CountFlagEnabled); err != nil {
		return fmt.Errorf("when build BPTreeIdx insert index err: %s", err)
	}

	return nil
}

func (db *DB) buildBucketMetaIdx() error {
	if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		files, err := ioutil.ReadDir(db.getBucketMetaPath())
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

				bucketMeta, err := ReadBucketMeta(db.getBucketMetaFilePath(name))
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

func (db *DB) buildOtherIdxes(bucket string, r *Record) error {
	if r.H.Meta.Ds == DataStructureSet {
		if err := db.buildSetIdx(bucket, r); err != nil {
			return err
		}
	}

	if r.H.Meta.Ds == DataStructureSortedSet {
		if err := db.buildSortedSetIdx(bucket, r); err != nil {
			return err
		}
	}

	if r.H.Meta.Ds == DataStructureList {
		if err := db.buildListIdx(bucket, r); err != nil {
			return err
		}
	}

	return nil
}

// buildHintIdx builds the Hint Indexes.
func (db *DB) buildHintIdx(dataFileIds []int) error {
	unconfirmedRecords, committedTxIds, err := db.parseDataFiles(dataFileIds)
	db.committedTxIds = committedTxIds

	if err != nil {
		return err
	}

	if len(unconfirmedRecords) == 0 {
		return nil
	}

	for _, r := range unconfirmedRecords {
		if _, ok := db.committedTxIds[r.H.Meta.TxID]; ok {
			bucket := string(r.H.Meta.Bucket)

			if r.H.Meta.Ds == DataStructureBPTree {
				r.H.Meta.Status = Committed

				if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
					if err = db.buildActiveBPTreeIdx(r); err != nil {
						return err
					}
				} else {
					if err = db.buildBPTreeIdx(bucket, r); err != nil {
						return err
					}
				}
			}

			if err = db.buildOtherIdxes(bucket, r); err != nil {
				return err
			}

			if r.H.Meta.Ds == DataStructureNone {
				db.buildNotDSIdxes(bucket, r)
			}

			db.KeyCount++
		}
	}

	if HintBPTSparseIdxMode == db.opt.EntryIdxMode {
		if err = db.buildBPTreeRootIdxes(dataFileIds); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) buildNotDSIdxes(bucket string, r *Record) {
	if r.H.Meta.Flag == DataSetBucketDeleteFlag {
		db.deleteBucket(DataStructureSet, bucket)
	}
	if r.H.Meta.Flag == DataSortedSetBucketDeleteFlag {
		db.deleteBucket(DataStructureSortedSet, bucket)
	}
	if r.H.Meta.Flag == DataBPTreeBucketDeleteFlag {
		db.deleteBucket(DataStructureBPTree, bucket)
	}
	if r.H.Meta.Flag == DataListBucketDeleteFlag {
		db.deleteBucket(DataStructureList, bucket)
	}
}

func (db *DB) deleteBucket(ds uint16, bucket string) {
	if ds == DataStructureSet {
		delete(db.SetIdx, bucket)
	}
	if ds == DataStructureSortedSet {
		delete(db.SortedSetIdx, bucket)
	}
	if ds == DataStructureBPTree {
		delete(db.BPTreeIdx, bucket)
	}
	if ds == DataStructureList {
		delete(db.ListIdx, bucket)
	}
}

// buildSetIdx builds set index when opening the DB.
func (db *DB) buildSetIdx(bucket string, r *Record) error {
	if _, ok := db.SetIdx[bucket]; !ok {
		db.SetIdx[bucket] = set.New()
	}

	if r.E == nil {
		return ErrEntryIdxModeOpt
	}

	if r.H.Meta.Flag == DataSetFlag {
		if err := db.SetIdx[bucket].SAdd(string(r.E.Key), r.E.Value); err != nil {
			return fmt.Errorf("when build SetIdx SAdd index err: %s", err)
		}
	}

	if r.H.Meta.Flag == DataDeleteFlag {
		if err := db.SetIdx[bucket].SRem(string(r.E.Key), r.E.Value); err != nil {
			return fmt.Errorf("when build SetIdx SRem index err: %s", err)
		}
	}

	return nil
}

// buildSortedSetIdx builds sorted set index when opening the DB.
func (db *DB) buildSortedSetIdx(bucket string, r *Record) error {
	if _, ok := db.SortedSetIdx[bucket]; !ok {
		db.SortedSetIdx[bucket] = zset.New()
	}

	if r.H.Meta.Flag == DataZAddFlag {
		keyAndScore := strings.Split(string(r.E.Key), SeparatorForZSetKey)
		if len(keyAndScore) == 2 {
			key := keyAndScore[0]
			score, _ := strconv2.StrToFloat64(keyAndScore[1])
			if r.E == nil {
				return ErrEntryIdxModeOpt
			}
			_ = db.SortedSetIdx[bucket].Put(key, zset.SCORE(score), r.E.Value)
		}
	}
	if r.H.Meta.Flag == DataZRemFlag {
		_ = db.SortedSetIdx[bucket].Remove(string(r.E.Key))
	}
	if r.H.Meta.Flag == DataZRemRangeByRankFlag {
		start, _ := strconv2.StrToInt(string(r.E.Key))
		end, _ := strconv2.StrToInt(string(r.E.Value))
		_ = db.SortedSetIdx[bucket].GetByRankRange(start, end, true)
	}
	if r.H.Meta.Flag == DataZPopMaxFlag {
		_ = db.SortedSetIdx[bucket].PopMax()
	}
	if r.H.Meta.Flag == DataZPopMinFlag {
		_ = db.SortedSetIdx[bucket].PopMin()
	}

	return nil
}

// buildListIdx builds List index when opening the DB.
func (db *DB) buildListIdx(bucket string, r *Record) error {
	if _, ok := db.ListIdx[bucket]; !ok {
		db.ListIdx[bucket] = list.New()
	}

	if r.E == nil {
		return ErrEntryIdxModeOpt
	}

	switch r.H.Meta.Flag {
	case DataLPushFlag:
		_, _ = db.ListIdx[bucket].LPush(string(r.E.Key), r.E.Value)
	case DataRPushFlag:
		_, _ = db.ListIdx[bucket].RPush(string(r.E.Key), r.E.Value)
	case DataLRemFlag:
		countAndValueIndex := strings.Split(string(r.E.Value), SeparatorForListKey)
		count, _ := strconv2.StrToInt(countAndValueIndex[0])
		value := []byte(countAndValueIndex[1])

		if _, err := db.ListIdx[bucket].LRem(string(r.E.Key), count, value); err != nil {
			return ErrWhenBuildListIdx(err)
		}
	case DataLPopFlag:
		if _, err := db.ListIdx[bucket].LPop(string(r.E.Key)); err != nil {
			return ErrWhenBuildListIdx(err)
		}
	case DataRPopFlag:
		if _, err := db.ListIdx[bucket].RPop(string(r.E.Key)); err != nil {
			return ErrWhenBuildListIdx(err)
		}
	case DataLSetFlag:
		keyAndIndex := strings.Split(string(r.E.Key), SeparatorForListKey)
		newKey := keyAndIndex[0]
		index, _ := strconv2.StrToInt(keyAndIndex[1])
		if err := db.ListIdx[bucket].LSet(newKey, index, r.E.Value); err != nil {
			return ErrWhenBuildListIdx(err)
		}
	case DataLTrimFlag:
		keyAndStartIndex := strings.Split(string(r.E.Key), SeparatorForListKey)
		newKey := keyAndStartIndex[0]
		start, _ := strconv2.StrToInt(keyAndStartIndex[1])
		end, _ := strconv2.StrToInt(string(r.E.Value))
		if err := db.ListIdx[bucket].Ltrim(newKey, start, end); err != nil {
			return ErrWhenBuildListIdx(err)
		}
	case DataLRemByIndex:
		indexes, err := UnmarshalInts(r.E.Value)
		if err != nil {
			return err
		}
		if _, err := db.ListIdx[bucket].LRemByIndex(string(r.E.Key), indexes); err != nil {
			return ErrWhenBuildListIdx(err)
		}
	}

	return nil
}

// ErrWhenBuildListIdx returns err when build listIdx
func ErrWhenBuildListIdx(err error) error {
	return fmt.Errorf("when build listIdx LRem err: %s", err)
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

	if db.ActiveFile.writeOff, err = db.getActiveFileWriteOff(); err != nil {
		return
	}

	if err = db.buildBucketMetaIdx(); err != nil {
		return
	}

	// build hint index
	return db.buildHintIdx(dataFileIds)
}

// managed calls a block of code that is fully contained in a transaction.
func (db *DB) managed(writable bool, fn func(tx *Tx) error) (err error) {
	var tx *Tx

	tx, err = db.Begin(writable)
	if err != nil {
		return err
	}
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

	if err = fn(tx); err == nil {
		err = tx.Commit()
	}
	return err
}

const bptDir = "bpt"

// getDataPath returns the data path at given fid.
func (db *DB) getDataPath(fID int64) string {
	separator := string(filepath.Separator)
	return db.opt.Dir + separator + strconv2.Int64ToStr(fID) + DataSuffix
}

func (db *DB) getMetaPath() string {
	separator := string(filepath.Separator)
	return db.opt.Dir + separator + "meta"
}

func (db *DB) getBucketMetaPath() string {
	separator := string(filepath.Separator)
	return db.getMetaPath() + separator + "bucket"
}

func (db *DB) getBucketMetaFilePath(name string) string {
	separator := string(filepath.Separator)
	return db.getBucketMetaPath() + separator + name + BucketMetaSuffix
}

func (db *DB) getBPTDir() string {
	separator := string(filepath.Separator)
	return db.opt.Dir + separator + bptDir
}

func (db *DB) getBPTPath(fID int64) string {
	separator := string(filepath.Separator)
	return db.getBPTDir() + separator + strconv2.Int64ToStr(fID) + BPTIndexSuffix
}

func (db *DB) getBPTRootPath(fID int64) string {
	separator := string(filepath.Separator)
	return db.getBPTDir() + separator + "root" + separator + strconv2.Int64ToStr(fID) + BPTRootIndexSuffix
}

func (db *DB) getBPTTxIDPath(fID int64) string {
	separator := string(filepath.Separator)
	return db.getBPTDir() + separator + "txid" + separator + strconv2.Int64ToStr(fID) + BPTTxIDIndexSuffix
}

func (db *DB) getBPTRootTxIDPath(fID int64) string {
	separator := string(filepath.Separator)
	return db.getBPTDir() + separator + "txid" + separator + strconv2.Int64ToStr(fID) + BPTRootTxIDIndexSuffix
}

func (db *DB) getPendingMergeEntries(entry *Entry, pendingMergeEntries []*Entry) []*Entry {
	if entry.Meta.Ds == DataStructureBPTree {
		bptIdx, exist := db.BPTreeIdx[string(entry.Meta.Bucket)]
		if exist {
			r, err := bptIdx.Find(entry.Key)
			if err == nil && r.H.Meta.Flag == DataSetFlag {
				pendingMergeEntries = append(pendingMergeEntries, entry)
			}
		}
	}

	if entry.Meta.Ds == DataStructureSet {
		setIdx, exist := db.SetIdx[string(entry.Meta.Bucket)]
		if exist {
			if setIdx.SIsMember(string(entry.Key), entry.Value) {
				pendingMergeEntries = append(pendingMergeEntries, entry)
			}
		}
	}

	if entry.Meta.Ds == DataStructureSortedSet {
		keyAndScore := strings.Split(string(entry.Key), SeparatorForZSetKey)
		if len(keyAndScore) == 2 {
			key := keyAndScore[0]
			sortedSetIdx, exist := db.SortedSetIdx[string(entry.Meta.Bucket)]
			if exist {
				n := sortedSetIdx.GetByKey(key)
				if n != nil {
					pendingMergeEntries = append(pendingMergeEntries, entry)
				}
			}
		}
	}

	if entry.Meta.Ds == DataStructureList {
		listIdx, exist := db.ListIdx[string(entry.Meta.Bucket)]
		if exist {
			items, _ := listIdx.LRange(string(entry.Key), 0, -1)
			ok := false
			if entry.Meta.Flag == DataRPushFlag || entry.Meta.Flag == DataLPushFlag {
				for _, item := range items {
					if string(entry.Value) == string(item) {
						ok = true
						break
					}
				}
				if ok {
					pendingMergeEntries = append(pendingMergeEntries, entry)
				}
			}
		}
	}

	return pendingMergeEntries
}

func (db *DB) reWriteData(pendingMergeEntries []*Entry) error {
	if len(pendingMergeEntries) == 0 {
		return nil
	}
	tx, err := db.Begin(true)
	if err != nil {
		db.isMerging = false
		return err
	}

	dataFile, err := db.fm.getDataFile(db.getDataPath(db.MaxFileID+1), db.opt.SegmentSize)
	if err != nil {
		db.isMerging = false
		return err
	}
	db.ActiveFile = dataFile
	db.MaxFileID++

	for _, e := range pendingMergeEntries {
		err := tx.put(string(e.Meta.Bucket), e.Key, e.Value, e.Meta.TTL, e.Meta.Flag, e.Meta.Timestamp, e.Meta.Ds)
		if err != nil {
			tx.Rollback()
			db.isMerging = false
			return err
		}
	}
	tx.Commit()
	return nil
}

func (db *DB) isFilterEntry(entry *Entry) bool {
	if entry.Meta.Flag == DataDeleteFlag || entry.Meta.Flag == DataRPopFlag ||
		entry.Meta.Flag == DataLPopFlag || entry.Meta.Flag == DataLRemFlag ||
		entry.Meta.Flag == DataLTrimFlag || entry.Meta.Flag == DataZRemFlag ||
		entry.Meta.Flag == DataZRemRangeByRankFlag || entry.Meta.Flag == DataZPopMaxFlag ||
		entry.Meta.Flag == DataZPopMinFlag || entry.Meta.Flag == DataLRemByIndex ||
		IsExpired(entry.Meta.TTL, entry.Meta.Timestamp) {
		return true
	}

	return false
}

// getRecordFromKey fetches Record for given key and bucket
// this is a helper function used in Merge so it does not work if index mode is HintBPTSparseIdxMode
func (db *DB) getRecordFromKey(bucket, key []byte) (record *Record, err error) {
	idxMode := db.opt.EntryIdxMode
	if !(idxMode == HintKeyValAndRAMIdxMode || idxMode == HintKeyAndRAMIdxMode) {
		return nil, errors.New("not implemented")
	}
	idx, ok := db.BPTreeIdx[string(bucket)]
	if !ok {
		return nil, ErrBucketNotFound
	}
	return idx.Find(key)
}
