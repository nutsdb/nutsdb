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
	"io"
	"io/ioutil"
	"os"
	"path"
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
)

const (
	//flag
	DataDeleteFlag uint16 = iota
	DataSetFlag
	DataLPushFlag
	DataRPushFlag
	DataLRemFlag
	DataLPopFlag
	DataRPopFlag
	DataLSetFlag
	DataLTrimFlag
	DataZAddFlag
	DataZRemFlag
	DataZRemRangeByRankFlag
	DataZPopMaxFlag
	DataZPopMinFlag
)

const (
	//status
	UnCommitted uint16 = 0
	Committed   uint16 = 1

	Persistent  uint32 = 0
	ScanNoLimit int    = -1
)

const (
	DataStructureSet uint16 = iota
	DataStructureSortedSet
	DataStructureBPTree
	DataStructureList
)

type (
	// DB represents a collection of buckets that persist on disk.
	DB struct {
		opt          Options   // the database options
		BPTreeIdx    BPTreeIdx // Hint Index
		SetIdx       SetIdx
		SortedSetIdx SortedSetIdx
		ListIdx      ListIdx
		ActiveFile   *DataFile
		MaxFileId    int64
		mu           sync.RWMutex
		KeyCount     int // total key number ,include expired, deleted, repeated.
		closed       bool
		isMerging    bool
	}

	// BPTreeIdx is the B+ tree index
	BPTreeIdx    map[string]*BPTree
	SetIdx       map[string]*set.Set
	SortedSetIdx map[string]*zset.SortedSet
	ListIdx      map[string]*list.List

	Entries map[string]*Entry
)

// Open returns a newly initialized DB object.
func Open(opt Options) (*DB, error) {
	db := &DB{
		BPTreeIdx:    make(BPTreeIdx),
		SetIdx:       make(SetIdx),
		SortedSetIdx: make(SortedSetIdx),
		ListIdx:      make(ListIdx),
		MaxFileId:    0,
		opt:          opt,
		KeyCount:     0,
		closed:       false,
	}

	if ok := filesystem.PathIsExist(db.opt.Dir); !ok {
		if err := os.Mkdir(db.opt.Dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	if err := db.buildIndexes(); err != nil {
		return nil, fmt.Errorf("db.buildIndexes error: %s", err)
	}

	return db, nil
}

// Update executes a function within a managed read/write transaction.
func (db *DB) Update(fn func(tx *Tx) error) error {
	return db.managed(true, fn)
}

// View executes a function within a managed read-only transaction.
func (db *DB) View(fn func(tx *Tx) error) error {
	return db.managed(false, fn)
}

// managed calls a block of code that is fully contained in a transaction.
func (db *DB) managed(writable bool, fn func(tx *Tx) error) (err error) {
	var tx *Tx
	tx, err = db.Begin(writable)
	if err != nil {
		return
	}

	if err = fn(tx); err != nil {
		err = tx.Rollback()
		return
	}

	if err = tx.Commit(); err != nil {
		err = tx.Rollback()
		return
	}

	return
}

// getDataPath returns the data path at given fid.
func (db *DB) getDataPath(fId int64) string {
	return db.opt.Dir + "/" + strconv2.Int64ToStr(fId) + DataSuffix
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
// will effect the other write request. so execute it at the appropriate time.
func (db *DB) Merge() error {
	var (
		off                           int64
		dataFileIds, pendingMergeFIds []int
		pendingMergeEntries           []*Entry
	)

	db.isMerging = true

	_, dataFileIds = db.getMaxFileIdAndFileIds()
	pendingMergeFIds = dataFileIds

	for _, pendingMergeFId := range pendingMergeFIds {
		off = 0
		fId := int64(pendingMergeFId)
		f, err := NewDataFile(db.getDataPath(fId), db.opt.SegmentSize)
		if err != nil {
			db.isMerging = false
			return err
		}
		readEntriesNum := 0
		pendingMergeEntries = []*Entry{}

		for {
			if entry, err := f.ReadAt(int(off)); err == nil {
				if entry == nil {
					break
				}
				readEntriesNum++
				if entry.Meta.Flag == DataDeleteFlag || entry.Meta.Flag == DataRPopFlag ||
					entry.Meta.Flag == DataLPopFlag || entry.Meta.Flag == DataLRemFlag ||
					entry.Meta.Flag == DataLTrimFlag || entry.Meta.Flag == DataZRemFlag ||
					entry.Meta.Flag == DataZRemRangeByRankFlag || entry.Meta.Flag == DataZPopMaxFlag ||
					entry.Meta.Flag == DataZPopMinFlag || IsExpired(entry.Meta.TTL, entry.Meta.timestamp) {
					off += entry.Size()
					if off >= db.opt.SegmentSize {
						break
					}
					continue
				}

				if entry.Meta.ds == DataStructureBPTree {
					if r, err := db.BPTreeIdx[string(entry.Meta.bucket)].Find(entry.Key); err == nil {
						if r.H.meta.Flag == DataSetFlag {
							pendingMergeEntries = append(pendingMergeEntries, entry)
						}
					}
				}

				if entry.Meta.ds == DataStructureSet {
					if db.SetIdx[string(entry.Meta.bucket)].SIsMember(string(entry.Key), entry.Value) {
						pendingMergeEntries = append(pendingMergeEntries, entry)
					}
				}

				if entry.Meta.ds == DataStructureSortedSet {
					keyAndScore := strings.Split(string(entry.Key), SeparatorForZSetKey)
					if len(keyAndScore) == 2 {
						key := keyAndScore[0]
						n := db.SortedSetIdx[string(entry.Meta.bucket)].GetByKey(key)
						if n != nil {
							pendingMergeEntries = append(pendingMergeEntries, entry)
						}
					}
				}

				if entry.Meta.ds == DataStructureList {
					items, _ := db.ListIdx[string(entry.Meta.bucket)].LRange(string(entry.Key), 0, -1)
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

				off += entry.Size()
				if off >= db.opt.SegmentSize {
					break
				}

			} else {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("when merge operation build hintIndex readAt err: %s", err)
			}
		}

		//rewrite to active file
		tx, err := db.Begin(true)
		if err != nil {
			db.isMerging = false
			return err
		}

		for _, e := range pendingMergeEntries {
			err := tx.put(string(e.Meta.bucket), e.Key, e.Value, e.Meta.TTL, e.Meta.Flag, e.Meta.timestamp, e.Meta.ds)
			if err != nil {
				tx.Rollback()
				db.isMerging = false
				return err
			}
		}
		tx.Commit()
		//remove old file
		if err := os.Remove(db.getDataPath(int64(pendingMergeFId))); err != nil {
			db.isMerging = false
			return fmt.Errorf("when merge err: %s", err)
		}
	}

	return nil
}

// Backup copies the database to file directory at the given dir.
func (db *DB) Backup(dir string) error {
	err := db.View(func(tx *Tx) error {
		return filesystem.CopyDir(db.opt.Dir, dir)
	})
	if err != nil {
		return err
	}

	return nil
}

// Close releases all db resources.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDBClosed
	}

	db.closed = true

	db.ActiveFile = nil

	db.BPTreeIdx = nil

	return nil
}

// setActiveFile sets the ActiveFile (DataFile object).
func (db *DB) setActiveFile() (err error) {
	filepath := db.getDataPath(db.MaxFileId)
	db.ActiveFile, err = NewDataFile(filepath, db.opt.SegmentSize)
	if err != nil {
		return
	}

	db.ActiveFile.fileId = db.MaxFileId

	return nil
}

// getMaxFileIdAndFileIds returns max fileId and fileIds.
func (db *DB) getMaxFileIdAndFileIds() (maxFileId int64, dataFileIds []int) {
	files, _ := ioutil.ReadDir(db.opt.Dir)
	if len(files) == 0 {
		return 0, nil
	}

	maxFileId = 0

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

	sort.Sort(sort.IntSlice(dataFileIds))
	maxFileId = int64(dataFileIds[len(dataFileIds)-1])

	return
}

// getActiveFileWriteOff returns the write offset of activeFile.
func (db *DB) getActiveFileWriteOff() (off int64, err error) {
	off = 0
	for {
		if item, err := db.ActiveFile.ReadAt(int(off)); err == nil {
			if item == nil {
				break
			}

			off += item.Size()
			//set ActiveFileActualSize
			db.ActiveFile.ActualSize = off

		} else {
			if err == io.EOF {
				break
			}

			return -1, fmt.Errorf("when build activeDataIndex readAt err: %s", err)
		}
	}

	return
}

// buildHintIdx builds the Hint Indexes.
func (db *DB) buildHintIdx(dataFileIds []int) error {
	var (
		off                int64
		unconfirmedRecords []*Record
		committedTxIds     map[uint64]struct{}
		e                  *Entry
	)

	committedTxIds = make(map[uint64]struct{})

	for _, dataId := range dataFileIds {
		off = 0
		fId := int64(dataId)
		f, err := NewDataFile(db.getDataPath(fId), db.opt.SegmentSize)

		if err != nil {
			return err
		}

		for {
			if entry, err := f.ReadAt(int(off)); err == nil {
				if entry == nil {
					break
				}

				e = nil
				if db.opt.EntryIdxMode == HintAndRAMIdxMode {
					e = &Entry{
						Key:   entry.Key,
						Value: entry.Value,
						Meta:  entry.Meta,
					}
				}

				if entry.Meta.status == Committed {
					committedTxIds[entry.Meta.txId] = struct{}{}
				}

				unconfirmedRecords = append(unconfirmedRecords, &Record{
					H: &Hint{
						key:     entry.Key,
						fileId:  fId,
						meta:    entry.Meta,
						dataPos: uint64(off),
					},
					E: e,
				})

				off += entry.Size()

			} else {
				if err == io.EOF {
					break
				}

				if off >= db.opt.SegmentSize {
					fmt.Println("off,db.opt.SegmentSize", off, db.opt.SegmentSize)
					break
				}

				//return fmt.Errorf("when build hintIndex readAt err: %s", err)
			}
		}
	}

	if len(unconfirmedRecords) > 0 {
		for _, r := range unconfirmedRecords {
			if _, ok := committedTxIds[r.H.meta.txId]; ok {
				bucket := string(r.H.meta.bucket)

				if r.H.meta.ds == DataStructureBPTree {
					if _, ok := db.BPTreeIdx[bucket]; !ok {
						db.BPTreeIdx[bucket] = NewTree()
					}
					r.H.meta.status = Committed

					if err := db.BPTreeIdx[bucket].Insert(r.H.key, r.E, r.H, CountFlagEnabled); err != nil {
						return fmt.Errorf("when build BPTreeIdx insert index err: %s", err)
					}
				}

				if r.H.meta.ds == DataStructureSet {
					if _, ok := db.SetIdx[bucket]; !ok {
						db.SetIdx[bucket] = set.New()
					}

					if r.E == nil {
						return ErrEntryIdxModeOpt
					}

					if r.H.meta.Flag == DataSetFlag {
						if err := db.SetIdx[bucket].SAdd(string(r.E.Key), r.E.Value); err != nil {
							return fmt.Errorf("when build SetIdx SAdd index err: %s", err)
						}
					}

					if r.H.meta.Flag == DataDeleteFlag {
						if err := db.SetIdx[bucket].SRem(string(r.E.Key), r.E.Value); err != nil {
							return fmt.Errorf("when build SetIdx SRem index err: %s", err)
						}
					}
				}

				if r.H.meta.ds == DataStructureSortedSet {
					if _, ok := db.SortedSetIdx[bucket]; !ok {
						db.SortedSetIdx[bucket] = zset.New()
					}

					if r.H.meta.Flag == DataZAddFlag {
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
					if r.H.meta.Flag == DataZRemFlag {
						_ = db.SortedSetIdx[bucket].Remove(string(r.E.Key))
					}
					if r.H.meta.Flag == DataZRemRangeByRankFlag {
						start, _ := strconv2.StrToInt(string(r.E.Key))
						end, _ := strconv2.StrToInt(string(r.E.Value))
						_ = db.SortedSetIdx[bucket].GetByRankRange(start, end, true)
					}
					if r.H.meta.Flag == DataZPopMaxFlag {
						_ = db.SortedSetIdx[bucket].PopMax()
					}
					if r.H.meta.Flag == DataZPopMinFlag {
						_ = db.SortedSetIdx[bucket].PopMin()
					}
				}

				if r.H.meta.ds == DataStructureList {
					if _, ok := db.ListIdx[bucket]; !ok {
						db.ListIdx[bucket] = list.New()
					}

					if r.E == nil {
						return ErrEntryIdxModeOpt
					}

					switch r.H.meta.Flag {
					case DataLPushFlag:
						_, _ = db.ListIdx[bucket].LPush(string(r.E.Key), r.E.Value)
					case DataRPushFlag:
						_, _ = db.ListIdx[bucket].RPush(string(r.E.Key), r.E.Value)
					case DataLRemFlag:
						count, _ := strconv2.StrToInt(string(r.E.Value))
						if _, err := db.ListIdx[bucket].LRem(string(r.E.Key), count); err != nil {
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
					}
				}

				db.KeyCount++
			}
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
		maxFileId   int64
		dataFileIds []int
	)

	maxFileId, dataFileIds = db.getMaxFileIdAndFileIds()

	//init db.ActiveFile
	db.MaxFileId = maxFileId

	//set ActiveFile
	if err = db.setActiveFile(); err != nil {
		return
	}

	if dataFileIds == nil && maxFileId == 0 {
		return
	}

	if db.ActiveFile.writeOff, err = db.getActiveFileWriteOff(); err != nil {
		return
	}

	// build hint index
	return db.buildHintIdx(dataFileIds)
}
