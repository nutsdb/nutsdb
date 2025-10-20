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

	"github.com/nutsdb/nutsdb/internal/utils"
	"github.com/xujiajun/utils/strconv2"
)

var ErrDontNeedMerge = errors.New("the number of files waiting to be merged is less than 2")

func (db *DB) Merge() error {
	db.mergeStartCh <- struct{}{}
	return <-db.mergeEndCh
}

func (db *DB) merge() error {
	if db.opt.EnableMergeV2 {
		return db.mergeV2()
	}
	return db.mergeLegacy()
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
func (db *DB) mergeLegacy() error {
	var (
		off              int64
		pendingMergeFIds []int64
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
	db.ActiveFile, err = db.fm.GetDataFile(path, db.opt.SegmentSize)
	if err != nil {
		db.mu.Unlock()
		return err
	}

	db.ActiveFile.fileID = db.MaxFileID
	startFileID := db.MaxFileID

	db.mu.Unlock()

	mergingPath := make([]string, len(pendingMergeFIds))

	// Used to collect all merged entry information for later writing to the HintFile
	var mergedEntries []mergedEntryInfo

	for i, pendingMergeFId := range pendingMergeFIds {
		off = 0
		path := getDataPath(pendingMergeFId, db.opt.Dir)
		fr, err := newFileRecovery(path, db.opt.BufferSizeOfRecovery)
		if err != nil {
			return err
		}

		for {
			if entry, err := fr.readEntry(off); err == nil {
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
						bucket, err := db.bm.GetBucketById(entry.Meta.BucketId)
						if err != nil {
							return err
						}
						bucketName := bucket.Name

						switch entry.Meta.Flag {
						case DataLPushFlag:
							if err := tx.LPushRaw(bucketName, entry.Key, entry.Value); err != nil {
								return err
							}
						case DataRPushFlag:
							if err := tx.RPushRaw(bucketName, entry.Key, entry.Value); err != nil {
								return err
							}
						default:
							if err := tx.put(
								bucketName,
								entry.Key,
								entry.Value,
								entry.Meta.TTL,
								entry.Meta.Flag,
								entry.Meta.Timestamp,
								entry.Meta.Ds,
							); err != nil {
								return err
							}
						}

						// Record merged entry information to get actual position from index later
						mergedEntries = append(mergedEntries, mergedEntryInfo{
							entry:    entry,
							bucketId: bucket.Id,
						})
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
				if errors.Is(err, io.EOF) || errors.Is(err, ErrIndexOutOfBound) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrHeaderSizeOutOfBounds) {
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

	// Now that all data has been written, create HintFile for all newly generated data files
	// Only create HintFile for new files actually generated during the Merge process
	db.mu.Lock()
	endFileID := db.MaxFileID // Record the maximum file ID when merge ends
	db.mu.Unlock()

	// 只有当启用 HintFile 功能时才创建 HintFile
	if db.opt.EnableHintFile {
		if err := db.buildHintFilesAfterMerge(startFileID, endFileID); err != nil {
			return fmt.Errorf("failed to build hint files after merge: %w", err)
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	for i := 0; i < len(mergingPath); i++ {
		if err := os.Remove(mergingPath[i]); err != nil {
			return fmt.Errorf("when merge err: %s", err)
		}

		// Delete the HintFile corresponding to the old data file (if it exists)
		oldHintPath := getHintPath(int64(pendingMergeFIds[i]), db.opt.Dir)
		if _, err := os.Stat(oldHintPath); err == nil {
			if removeErr := os.Remove(oldHintPath); removeErr != nil {
				// Log error but don't interrupt the merge process
				fmt.Printf("warning: failed to remove old hint file %s: %v\n", oldHintPath, removeErr)
			}
		}
	}

	return nil
}

// buildHintFilesAfterMerge creates HintFiles for all newly generated data files after merge
// by traversing the index to find all records pointing to new files.
// Only process files in the range [startFileID, endFileID]
func (db *DB) buildHintFilesAfterMerge(startFileID, endFileID int64) error {
	if startFileID > endFileID {
		return nil
	}

	for fileID := startFileID; fileID <= endFileID; fileID++ {
		dataPath := getDataPath(fileID, db.opt.Dir)
		fr, err := newFileRecovery(dataPath, db.opt.BufferSizeOfRecovery)
		if err != nil {
			return fmt.Errorf("failed to open data file %s: %w", dataPath, err)
		}

		hintPath := getHintPath(fileID, db.opt.Dir)
		hintWriter := &HintFileWriter{}
		if err := hintWriter.Create(hintPath); err != nil {
			_ = fr.release()
			return fmt.Errorf("failed to create hint file %s: %w", hintPath, err)
		}

		cleanup := func(remove bool) {
			_ = hintWriter.Close()
			_ = fr.release()
			if remove {
				_ = os.Remove(hintPath)
			}
		}

		off := int64(0)
		for {
			entry, err := fr.readEntry(off)
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, ErrIndexOutOfBound) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrHeaderSizeOutOfBounds) {
					break
				}
				cleanup(true)
				return fmt.Errorf("failed to read entry from %s at offset %d: %w", dataPath, off, err)
			}

			if entry == nil {
				break
			}

			hintEntry := newHintEntryFromEntry(entry, fileID, uint64(off))

			if err := hintWriter.Write(hintEntry); err != nil {
				cleanup(true)
				return fmt.Errorf("failed to write hint entry to %s: %w", hintPath, err)
			}

			off += entry.Size()
			if off >= fr.size {
				break
			}
		}

		if err := hintWriter.Sync(); err != nil {
			cleanup(true)
			return fmt.Errorf("failed to sync hint file %s: %w", hintPath, err)
		}

		if err := hintWriter.Close(); err != nil {
			_ = fr.release()
			return fmt.Errorf("failed to close hint file %s: %w", hintPath, err)
		}

		if err := fr.release(); err != nil {
			return fmt.Errorf("failed to close data file %s: %w", dataPath, err)
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
	switch {
	case entry.IsBelongsToBTree():
		return db.isPendingBtreeEntry(entry)
	case entry.IsBelongsToList():
		return db.isPendingListEntry(entry)
	case entry.IsBelongsToSet():
		return db.isPendingSetEntry(entry)
	case entry.IsBelongsToSortSet():
		return db.isPendingZSetEntry(entry)
	}
	return false
}

func (db *DB) isPendingBtreeEntry(entry *Entry) bool {
	idx, exist := db.Index.bTree.exist(entry.Meta.BucketId)
	if !exist {
		return false
	}

	r, ok := idx.Find(entry.Key)
	if !ok {
		return false
	}

	if r.IsExpired() {
		db.tm.del(entry.Meta.BucketId, string(entry.Key))
		idx.Delete(entry.Key)
		return false
	}

	if r.TxID != entry.Meta.TxID || r.Timestamp != entry.Meta.Timestamp {
		return false
	}

	return true
}

func (db *DB) isPendingSetEntry(entry *Entry) bool {
	setIdx, exist := db.Index.set.exist(entry.Meta.BucketId)
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
	sortedSetIdx, exist := db.Index.sortedSet.exist(entry.Meta.BucketId)
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
		list, exist := db.Index.list.exist(entry.Meta.BucketId)
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

		list, exist := db.Index.list.exist(entry.Meta.BucketId)
		if !exist {
			return false
		}

		if _, ok := list.Items[userKeyStr]; !ok {
			return false
		}

		r, ok := list.Items[userKeyStr].Find(utils.ConvertUint64ToBigEndianBytes(curSeq))
		if !ok {
			return false
		}

		if !bytes.Equal(r.Key, entry.Key) || r.TxID != entry.Meta.TxID || r.Timestamp != entry.Meta.Timestamp {
			return false
		}

		return true
	}

	return false
}

// mergedEntryInfo 用于在 merge 过程中暂存条目信息
type mergedEntryInfo struct {
	entry    *Entry
	bucketId BucketId
}
