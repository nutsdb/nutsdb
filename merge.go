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
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync/atomic"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/utils"
	"github.com/xujiajun/utils/strconv2"
)

var ErrDontNeedMerge = errors.New("the number of files waiting to be merged is less than 2")

func (db *DB) Merge() error {
	return db.mergeWorker.TriggerMerge()
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

	db.mu.Lock()
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

				if entry.IsFilter() {
					off += entry.Size()
					if off >= db.opt.SegmentSize {
						break
					}
					continue
				}

				// Index reads can race with commits; use a transaction to avoid inconsistencies.
				err := db.Update(func(tx *Tx) error {
					if ok := db.isPendingMergeEntry(entry); ok {
						bucket, err := db.bucketMgr.GetBucketById(entry.Meta.BucketId)
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
				if errors.Is(err, io.EOF) || errors.Is(err, ErrIndexOutOfBound) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, core.ErrHeaderSizeOutOfBounds) {
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
	endFileID := db.MaxFileID
	db.mu.Unlock()

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

		oldHintPath := getHintPath(int64(pendingMergeFIds[i]), db.opt.Dir)
		if _, err := os.Stat(oldHintPath); err == nil {
			if removeErr := os.Remove(oldHintPath); removeErr != nil {
				fmt.Printf("warning: failed to remove old hint file %s: %v\n", oldHintPath, removeErr)
			}
		}
	}

	return nil
}

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
				if errors.Is(err, io.EOF) || errors.Is(err, ErrIndexOutOfBound) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, core.ErrHeaderSizeOutOfBounds) {
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

func (db *DB) isPendingMergeEntry(entry *core.Entry) bool {
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

func (db *DB) isPendingBtreeEntry(entry *core.Entry) bool {
	idx, exist := db.Index.BTree.exist(entry.Meta.BucketId)
	if !exist {
		return false
	}

	r, ok := idx.Find(entry.Key)
	if !ok {
		return false
	}

	if db.ttlService.GetChecker().IsExpired(r.TTL, r.Timestamp) {
		idx.Delete(entry.Key)
		return false
	}

	if r.TxID != entry.Meta.TxID || r.Timestamp != entry.Meta.Timestamp {
		return false
	}

	return true
}

func (db *DB) isPendingSetEntry(entry *core.Entry) bool {
	setIdx, exist := db.Index.Set.exist(entry.Meta.BucketId)
	if !exist {
		return false
	}

	isMember, err := setIdx.SIsMember(string(entry.Key), entry.Value)
	if err != nil || !isMember {
		return false
	}

	return true
}

func (db *DB) isPendingZSetEntry(entry *core.Entry) bool {
	key, score := splitStringFloat64Str(string(entry.Key), SeparatorForZSetKey)
	sortedSetIdx, exist := db.Index.SortedSet.exist(entry.Meta.BucketId)
	if !exist {
		return false
	}
	s, err := sortedSetIdx.ZScore(key, entry.Value)
	if err != nil || s != score {
		return false
	}

	return true
}

func (db *DB) isPendingListEntry(entry *core.Entry) bool {
	var userKeyStr string
	var curSeq uint64
	var userKey []byte

	if entry.Meta.Flag == DataExpireListFlag {
		userKeyStr = string(entry.Key)
		list, exist := db.Index.List.exist(entry.Meta.BucketId)
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

		list, exist := db.Index.List.exist(entry.Meta.BucketId)
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

type mergedEntryInfo struct {
	entry    *core.Entry
	bucketId core.BucketId
}

type mergeWorker struct {
	lifecycle core.ComponentLifecycle

	db            *DB
	statusManager *StatusManager

	mergeStartCh chan struct{}
	mergeEndCh   chan error
	isMerging    atomic.Bool

	ticker *time.Ticker

	config MergeConfig
}

type MergeConfig struct {
	MergeInterval   time.Duration
	EnableAutoMerge bool
}

func DefaultMergeConfig() MergeConfig {
	return MergeConfig{
		MergeInterval:   0,
		EnableAutoMerge: false,
	}
}

func newMergeWorker(db *DB, sm *StatusManager, config MergeConfig) *mergeWorker {
	return &mergeWorker{
		db:            db,
		statusManager: sm,
		mergeStartCh:  make(chan struct{}),
		mergeEndCh:    make(chan error, 1),
		config:        config,
	}
}

func (mw *mergeWorker) Name() string {
	return "MergeWorker"
}

func (mw *mergeWorker) Start(ctx context.Context) error {
	if err := mw.lifecycle.Start(ctx); err != nil {
		return err
	}

	if mw.config.EnableAutoMerge && mw.config.MergeInterval > 0 {
		mw.ticker = time.NewTicker(mw.config.MergeInterval)
	} else {
		mw.ticker = time.NewTicker(math.MaxInt64)
		mw.ticker.Stop()
	}

	mw.statusManager.Add(1)
	mw.lifecycle.Go(mw.run)

	return nil
}

func (mw *mergeWorker) Stop(timeout time.Duration) error {
	if mw.ticker != nil {
		mw.ticker.Stop()
	}

	return mw.lifecycle.Stop(timeout)
}

func (mw *mergeWorker) TriggerMerge() error {
	if mw.statusManager.isClosingOrClosed() {
		return ErrDBClosed
	}

	if mw.isMerging.Load() {
		return ErrIsMerging
	}

	select {
	case mw.mergeStartCh <- struct{}{}:
		return <-mw.mergeEndCh
	case <-mw.lifecycle.Context().Done():
		return ErrDBClosed
	}
}

func (mw *mergeWorker) IsMerging() bool {
	return mw.isMerging.Load()
}

func (mw *mergeWorker) run(ctx context.Context) {
	defer mw.statusManager.Done()

	for {
		select {
		case <-ctx.Done():
			return

		case <-mw.mergeStartCh:
			select {
			case <-ctx.Done():
				return
			default:
			}

			err := mw.performMerge()

			select {
			case mw.mergeEndCh <- err:
			default:
			}

			if mw.config.EnableAutoMerge && mw.config.MergeInterval > 0 {
				mw.ticker.Reset(mw.config.MergeInterval)
			}

		case <-mw.ticker.C:
			select {
			case <-ctx.Done():
				return
			default:
			}

			_ = mw.performMerge()
		}
	}
}

func (mw *mergeWorker) performMerge() error {
	if mw.statusManager.isClosingOrClosed() {
		return ErrDBClosed
	}

	if !mw.isMerging.CompareAndSwap(false, true) {
		return ErrIsMerging
	}
	defer mw.isMerging.Store(false)

	return mw.db.merge()
}

func (mw *mergeWorker) SetMergeInterval(interval time.Duration) {
	mw.config.MergeInterval = interval

	if interval > 0 {
		mw.config.EnableAutoMerge = true
		if mw.ticker != nil {
			mw.ticker.Reset(interval)
		}
	} else {
		mw.config.EnableAutoMerge = false
		if mw.ticker != nil {
			mw.ticker.Stop()
		}
	}
}

func (mw *mergeWorker) GetMergeInterval() time.Duration {
	return mw.config.MergeInterval
}
