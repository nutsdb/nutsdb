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

var (
	ErrDontNeedMerge = errors.New("the number of files waiting to be merged is at least 2")
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
// Caveat: merge is Called means starting multiple write transactions, and it
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
	if len(pendingMergeFIds) < 2 {
		db.mu.Unlock()
		return ErrDontNeedMerge
	}

	db.MaxFileID++

	if !db.opt.SyncEnable && db.opt.RWMode == MMap {
		if err := db.ActiveFile.rwManager.Sync(); err != nil {
			db.mu.Unlock()
			return err
		}
	}

	if err := db.ActiveFile.rwManager.Release(); err != nil {
		db.mu.Unlock()
		return err
	}

	var err error
	path := getDataPath(db.MaxFileID, db.opt.Dir)
	db.ActiveFile, err = db.fm.getDataFile(path, db.opt.SegmentSize)
	if err != nil {
		db.mu.Unlock()
		return err
	}

	db.ActiveFile.fileID = db.MaxFileID

	db.mu.Unlock()

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

				if entry.isFilter() {
					off += entry.Size()
					if off >= db.opt.SegmentSize {
						break
					}
					continue
				}

				// Due to the lack of concurrency safety in the index,
				// there is a possibility that a race condition might occur when the merge goroutine reads the index,
				// while a transaction is being committed, causing modifications to the index.
				// To address this issue, we need to use a transaction to perform this operation.
				err := db.Update(func(tx *Tx) error {
					// check if we have a new entry with same key and bucket
					if r, ok := db.getRecordFromKey(entry.Bucket, entry.Key); ok {
						if r.IsExpired() {
							// When merging, only need to delete the expired data directly from the index without leaving records
							tx.db.BTreeIdx[string(entry.Bucket)].Delete(entry.Key)
							return nil
						}
						if r.H.Meta.TxID <= entry.Meta.TxID {
							if ok := db.isPendingMergeEntry(entry); ok {
								return tx.put(
									string(entry.Bucket),
									entry.Key,
									entry.Value,
									entry.Meta.TTL,
									entry.Meta.Flag,
									entry.Meta.Timestamp,
									entry.Meta.Ds,
								)
							}
						}
					}
					return nil
				})

				if err != nil {
					_ = fr.release()
					return err
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

// getRecordFromKey fetches Record for given key and bucket
// this is a helper function used in merge so it does not work if index mode is HintBPTSparseIdxMode
func (db *DB) getRecordFromKey(bucket, key []byte) (*Record, bool) {
	idxMode := db.opt.EntryIdxMode
	if idxMode == HintBPTSparseIdxMode {
		return nil, false
	}
	idx, ok := db.BTreeIdx[string(bucket)]
	if !ok {
		return nil, false
	}
	return idx.Find(key)
}

func (db *DB) isPendingMergeEntry(entry *Entry) bool {
	if entry.Meta.Ds == DataStructureTree {
		bptIdx, exist := db.BTreeIdx[string(entry.Bucket)]
		if exist {
			r, ok := bptIdx.Find(entry.Key)
			if ok && r.H.Meta.Flag == DataSetFlag {
				return true
			}
		}
	}

	if entry.Meta.Ds == DataStructureSet {
		setIdx, exist := db.SetIdx[string(entry.Bucket)]
		if exist {
			isMember, err := setIdx.SIsMember(string(entry.Key), entry.Value)
			if err != nil {
				return false
			}
			if isMember {
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

		if listIdx := db.Index.getList(string(entry.Bucket)); listIdx != nil {
			items, _ := listIdx.LRange(string(entry.Key), 0, -1)
			if entry.Meta.Flag == DataRPushFlag || entry.Meta.Flag == DataLPushFlag {
				for _, item := range items {
					v, _ := db.getValueByRecord(item)
					if string(entry.Value) == string(v) {
						return true
					}
				}
			}
		}
	}

	return false
}
