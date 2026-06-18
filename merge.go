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
	"math"
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

	if db.ttlService.IsExpired(r.TTL, r.Timestamp) {
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

	// Propagate the worker's lifecycle context so that merge phases can be
	// canceled when the DB is shutting down. This prevents Close() from racing
	// with in-flight merge work (which would otherwise touch db.ActiveFile /
	// db.Index after they have been nil-ed by release()).
	return mw.db.merge(mw.lifecycle.Context())
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
