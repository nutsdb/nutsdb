// Copyright 2023 The nutsdb Author. All rights reserved.
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
	"os"
	"strings"
)

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
		off              int64
		pendingMergeFIds []int
	)

	if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		return ErrNotSupportHintBPTSparseIdxMode
	}

	// to prevent the initiation of multiple merges simultaneously.
	db.mu.Lock()

	if db.isMerging {
		db.mu.Unlock()
		return ErrIsMerging
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	_, pendingMergeFIds = db.getMaxFileIDAndFileIDs()

	db.mu.Unlock()

	if len(pendingMergeFIds) < 2 {
		return errors.New("the number of files waiting to be merged is at least 2")
	}

	for _, pendingMergeFId := range pendingMergeFIds {
		off = 0
		path := getDataPath(int64(pendingMergeFId), db.opt.Dir)
		fr, err := newFileRecovery(path, db.opt.BufferSizeOfRecovery)
		if err != nil {
			return err
		}

		for {
			if entry, err := fr.readEntry(); err == nil {
				if entry == nil {
					break
				}

				var skipEntry bool

				if entry.isFilter() {
					skipEntry = true
				}

				// check if we have a new entry with same key and bucket
				if r, _ := db.getRecordFromKey(entry.Bucket, entry.Key); r != nil && !skipEntry {
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

				if ok := db.isPendingMergeEntry(entry); ok {
					if err := db.reWriteData(entry); err != nil {
						_ = fr.release()
						return err
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
				if err == ErrIndexOutOfBound {
					break
				}
				if err == io.ErrUnexpectedEOF {
					break
				}
				return fmt.Errorf("when merge operation build hintIndex readAt err: %s", err)
			}
		}

		err = fr.release()
		if err != nil {
			return err
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("when merge err: %s", err)
		}
	}

	return nil
}

func (db *DB) mergeWorker() {

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

func (db *DB) isPendingMergeEntry(entry *Entry) bool {
	if entry.Meta.Ds == DataStructureBPTree {
		bptIdx, exist := db.BPTreeIdx[string(entry.Bucket)]
		if exist {
			r, err := bptIdx.Find(entry.Key)
			if err == nil && r.H.Meta.Flag == DataSetFlag {
				return true
			}
		}
	}

	if entry.Meta.Ds == DataStructureSet {
		setIdx, exist := db.SetIdx[string(entry.Bucket)]
		if exist {
			if setIdx.SIsMember(string(entry.Key), entry.Value) {
				return true
			}
		}
	}

	if entry.Meta.Ds == DataStructureSortedSet {
		keyAndScore := strings.Split(string(entry.Key), SeparatorForZSetKey)
		if len(keyAndScore) == 2 {
			key := keyAndScore[0]
			sortedSetIdx, exist := db.SortedSetIdx[string(entry.Bucket)]
			if exist {
				n := sortedSetIdx.GetByKey(key)
				if n != nil {
					return true
				}
			}
		}
	}

	if entry.Meta.Ds == DataStructureList {
		//check the key of list is expired or not
		//if expired, it will clear the items of index
		//so that nutsdb can clear entry of expiring list in the function isPendingMergeEntry
		db.checkListExpired()
		listIdx := db.Index.getList(string(entry.Bucket))

		if listIdx != nil {
			items, _ := listIdx.LRange(string(entry.Key), 0, -1)
			ok := false
			if entry.Meta.Flag == DataRPushFlag || entry.Meta.Flag == DataLPushFlag {
				for _, item := range items {
					v, _ := db.getValueByRecord(item)
					if string(entry.Value) == string(v) {
						ok = true
						break
					}
				}
				return ok
			}
		}
	}

	return false
}

func (db *DB) reWriteData(pendingMergeEntry *Entry) error {
	tx, err := db.Begin(true)
	if err != nil {
		db.isMerging = false
		return err
	}

	dataFile, err := db.fm.getDataFile(getDataPath(db.MaxFileID+1, db.opt.Dir), db.opt.SegmentSize)
	if err != nil {
		db.isMerging = false
		return err
	}
	db.ActiveFile = dataFile
	db.MaxFileID++

	err = tx.put(string(pendingMergeEntry.Bucket), pendingMergeEntry.Key, pendingMergeEntry.Value, pendingMergeEntry.Meta.TTL,
		pendingMergeEntry.Meta.Flag, pendingMergeEntry.Meta.Timestamp, pendingMergeEntry.Meta.Ds)
	if err != nil {
		tx.Rollback()
		db.isMerging = false
		return err
	}
	tx.Commit()
	return nil
}
