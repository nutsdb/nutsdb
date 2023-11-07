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
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/xujiajun/utils/strconv2"
)

var ErrDontNeedMerge = errors.New("the number of files waiting to be merged is at least 2")

func (db *DB) Merge() error {
	db.mergeStartCh <- struct{}{}
	return <-db.mergeEndCh
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
// Caveat: merge is Called means starting multiple write transactions, and it
// will affect the other write request. so execute it at the appropriate time.
func (db *DB) merge() error {
	var (
		off              int64
		pendingMergeFIds []int
	)

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

	mergingPath := make([]string, len(pendingMergeFIds))

	for i, pendingMergeFId := range pendingMergeFIds {
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
					if ok := db.isPendingMergeEntry(entry); ok {
						if entry.Meta.Flag == DataLPushFlag {
							return tx.LPushRaw(string(entry.Bucket), entry.Key, entry.Value)
						}

						if entry.Meta.Flag == DataRPushFlag {
							return tx.RPushRaw(string(entry.Bucket), entry.Key, entry.Value)
						}

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
				if errors.Is(err, io.EOF) || errors.Is(err, ErrIndexOutOfBound) || errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				return fmt.Errorf("when merge operation build hintIndex readAt err: %s", err)
			}
		}

		err = fr.release()
		if err != nil {
			return err
		}
		mergingPath[i] = path
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	for i := 0; i < len(mergingPath); i++ {
		if err := os.Remove(mergingPath[i]); err != nil {
			return fmt.Errorf("when merge err: %s", err)
		}
	}

	return nil
}

func (db *DB) mergeWorker() {
	var ticker *time.Ticker

	if db.opt.MergeInterval != 0 {
		ticker = time.NewTicker(db.opt.MergeInterval)
	} else {
		ticker = time.NewTicker(math.MaxInt)
		ticker.Stop()
	}

	for {
		select {
		case <-db.mergeStartCh:
			db.mergeEndCh <- db.merge()
			// if automatic merging is enabled, then after a manual merge
			// the t needs to be reset.
			if db.opt.MergeInterval != 0 {
				ticker.Reset(db.opt.MergeInterval)
			}
		case <-ticker.C:
			_ = db.merge()
		case <-db.mergeWorkCloseCh:
			return
		}
	}
}

func (db *DB) isPendingMergeEntry(entry *Entry) bool {
	if entry.Meta.Ds == DataStructureBTree {
		return db.isPendingBtreeEntry(entry)
	}

	if entry.Meta.Ds == DataStructureSet {
		return db.isPendingSetEntry(entry)
	}

	if entry.Meta.Ds == DataStructureSortedSet {
		return db.isPendingZSetEntry(entry)
	}

	if entry.Meta.Ds == DataStructureList {
		return db.isPendingListEntry(entry)
	}

	return false
}

func (db *DB) isPendingBtreeEntry(entry *Entry) bool {
	idx, exist := db.Index.bTree.exist(string(entry.Bucket))
	if !exist {
		return false
	}

	r, ok := idx.Find(entry.Key)
	if !ok || r.H.Meta.Flag != DataSetFlag {
		return false
	}

	if r.IsExpired() {
		db.tm.del(string(entry.Bucket), string(entry.Key))
		idx.Delete(entry.Key)
		return false
	}

	if r.H.Meta.TxID != entry.Meta.TxID || r.H.Meta.Timestamp != entry.Meta.Timestamp {
		return false
	}

	return true
}

func (db *DB) isPendingSetEntry(entry *Entry) bool {
	setIdx, exist := db.Index.set.exist(string(entry.Bucket))
	if !exist {
		return false
	}

	isMember, err := setIdx.SIsMember(string(entry.Key), entry.Value)
	if err != nil || !isMember {
		return false
	}

	return true
}

func (db *DB) isPendingZSetEntry(entry *Entry) bool {
	key, score := splitStringFloat64Str(string(entry.Key), SeparatorForZSetKey)
	sortedSetIdx, exist := db.Index.sortedSet.exist(string(entry.Bucket))
	if !exist {
		return false
	}
	s, err := sortedSetIdx.ZScore(key, entry.Value)
	if err != nil || s != score {
		return false
	}

	return true
}

func (db *DB) isPendingListEntry(entry *Entry) bool {
	var userKeyStr string
	var curSeq uint64
	var userKey []byte

	if entry.Meta.Flag == DataExpireListFlag {
		userKeyStr = string(entry.Key)
		list, exist := db.Index.list.exist(string(entry.Bucket))
		if !exist {
			return false
		}

		if _, ok := list.Items[userKeyStr]; !ok {
			return false
		}

		t, _ := strconv2.StrToInt64(string(entry.Value))
		ttl := uint32(t)
		if _, ok := list.TTL[userKeyStr]; !ok {
			return false
		}

		if list.TTL[userKeyStr] != ttl || list.TimeStamp[userKeyStr] != entry.Meta.Timestamp {
			return false
		}

		return true
	}

	if entry.Meta.Flag == DataLPushFlag || entry.Meta.Flag == DataRPushFlag {
		userKey, curSeq = decodeListKey(entry.Key)
		userKeyStr = string(userKey)

		list, exist := db.Index.list.exist(string(entry.Bucket))
		if !exist {
			return false
		}

		if _, ok := list.Items[userKeyStr]; !ok {
			return false
		}

		r, ok := list.Items[userKeyStr].Find(ConvertUint64ToBigEndianBytes(curSeq))
		if !ok {
			return false
		}

		if !bytes.Equal(r.H.Key, entry.Key) || r.H.Meta.TxID != entry.Meta.TxID || r.H.Meta.Timestamp != entry.Meta.Timestamp {
			return false
		}

		return true
	}

	return false
}
