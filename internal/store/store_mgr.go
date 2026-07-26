// Copyright 2026 The nutsdb Author. All rights reserved.
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

package store

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/fileio"
	"github.com/nutsdb/nutsdb/internal/utils"
)

// storeMgr is the StoreManager implementation that coordinates MemStore and disk I/O.
// Write protocol: disk append first, then mem commit; mem failure reverts mem to the snapshot.
type storeMgr struct {
	opts    DiskStoreOptions
	store   fileio.Store
	hintMgr fileio.HintManager
	mem     MemStore

	mu     sync.RWMutex
	closed bool
}

// OpenStoreManager opens a StoreManager that keeps MemStore and disk in sync
// per STORE_MGR_DESIGN.md (disk-first, revert mem on mem commit failure).
func OpenStoreManager(opts DiskStoreOptions) (StoreManager, error) {
	if opts.Dir == "" {
		return nil, fileio.ErrInvalidOptions
	}
	opts = normalizeDiskStoreOptions(opts)

	st, err := fileio.Open(fileio.Options{
		Dir:             opts.Dir,
		SegmentSize:     opts.SegmentSize,
		WriteBufferSize: opts.WriteBufferSize,
		MaxRecordSize:   opts.MaxRecordSize,
		SyncMode:        opts.SyncMode,
		MaxOpenSegments: opts.MaxOpenSegments,
		ReadBackend:     opts.ReadBackend,
	})
	if err != nil {
		return nil, err
	}
	m := &storeMgr{
		opts:    opts,
		store:   st,
		hintMgr: fileio.NewHintManager(opts.Dir),
		mem:     NewMemStore(),
	}
	if err := m.recover(); err != nil {
		_ = st.Close()
		return nil, err
	}
	return m, nil
}

// openStoreManagerWithMem is used by tests to inject a custom MemStore.
func openStoreManagerWithMem(opts DiskStoreOptions, mem MemStore) (StoreManager, error) {
	if opts.Dir == "" {
		return nil, fileio.ErrInvalidOptions
	}
	opts = normalizeDiskStoreOptions(opts)
	st, err := fileio.Open(fileio.Options{
		Dir:             opts.Dir,
		SegmentSize:     opts.SegmentSize,
		WriteBufferSize: opts.WriteBufferSize,
		MaxRecordSize:   opts.MaxRecordSize,
		SyncMode:        opts.SyncMode,
		MaxOpenSegments: opts.MaxOpenSegments,
		ReadBackend:     opts.ReadBackend,
	})
	if err != nil {
		return nil, err
	}
	m := &storeMgr{
		opts:    opts,
		store:   st,
		hintMgr: fileio.NewHintManager(opts.Dir),
		mem:     mem,
	}
	if err := m.recover(); err != nil {
		_ = st.Close()
		return nil, err
	}
	return m, nil
}

func normalizeDiskStoreOptions(opts DiskStoreOptions) DiskStoreOptions {
	def := DefaultDiskStoreOptions(opts.Dir)
	if opts.SegmentSize == 0 {
		opts.SegmentSize = def.SegmentSize
		opts.WriteBufferSize = def.WriteBufferSize
		opts.MaxRecordSize = def.MaxRecordSize
		opts.MaxOpenSegments = def.MaxOpenSegments
		opts.EnableReadTTL = def.EnableReadTTL
		opts.AutoFillTimestamp = def.AutoFillTimestamp
		opts.SyncMode = def.SyncMode
		opts.ReadBackend = def.ReadBackend
	}
	if opts.WriteBufferSize == 0 {
		opts.WriteBufferSize = def.WriteBufferSize
	}
	if opts.MaxRecordSize == 0 {
		opts.MaxRecordSize = def.MaxRecordSize
	}
	if opts.MaxOpenSegments == 0 {
		opts.MaxOpenSegments = def.MaxOpenSegments
	}
	return opts
}

func (m *storeMgr) recover() error {
	ids, err := m.store.ListFileIDs()
	if err != nil {
		return err
	}
	activeID := m.store.ActiveFileID()
	for _, id := range ids {
		if id == activeID {
			continue
		}
		rd, err := m.hintMgr.OpenReader(id)
		if err != nil {
			utils.GetLogger().Printf("storemgr: hint load fallback id=%d err=%v", id, err)
			if err := m.store.Iterate(id, m.applyRecord); err != nil {
				return err
			}
			continue
		}
		err = rd.Iterate(m.applyHintEntry)
		_ = rd.Close()
		if err != nil {
			utils.GetLogger().Printf("storemgr: hint iterate fallback id=%d err=%v", id, err)
			if err := m.store.Iterate(id, m.applyRecord); err != nil {
				return err
			}
		}
	}
	if activeID != 0 {
		return m.store.Iterate(activeID, m.applyRecord)
	}
	return nil
}

func (m *storeMgr) applyHintEntry(e fileio.HintEntry) error {
	switch e.Type {
	case fileio.RecordDelete:
		m.mem.Delete(e.Key)
	case fileio.RecordPut:
		_ = m.mem.Put(e.Key, e.Loc)
	}
	return nil
}

func (m *storeMgr) applyRecord(loc fileio.Location, typ fileio.RecordType, payload []byte) error {
	key, err := decodeKey(payload, typ)
	if err != nil {
		if errors.Is(err, fileio.ErrHintSkipMeta) {
			return nil
		}
		return err
	}
	switch typ {
	case fileio.RecordDelete:
		m.mem.Delete(key)
	case fileio.RecordPut:
		_ = m.mem.Put(key, loc)
	}
	return nil
}

func (m *storeMgr) revertMemPut(key []byte, oldLoc fileio.Location, hadOld bool) {
	if hadOld {
		_ = m.mem.Put(key, oldLoc)
		return
	}
	m.mem.Delete(key)
}

func (m *storeMgr) Get(ctx context.Context, key []byte) (*core.Record, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, ErrDiskStoreClosed
	}
	if len(key) == 0 {
		return nil, ErrKeyEmpty
	}
	loc, err := m.mem.Get(key)
	if err != nil {
		return nil, err
	}
	payload, typ, err := m.store.Read(loc)
	if err != nil {
		return nil, err
	}
	if typ != fileio.RecordPut {
		return nil, ErrKeyNotFound
	}
	_, rec, err := decodePutPayload(payload)
	if err != nil {
		return nil, err
	}
	if m.opts.EnableReadTTL && isExpired(rec, uint64(time.Now().Unix())) {
		return nil, ErrKeyNotFound
	}
	return rec, nil
}

func (m *storeMgr) Put(ctx context.Context, key []byte, value *core.Record) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrDiskStoreClosed
	}
	return m.putLocked(key, value)
}

func (m *storeMgr) putLocked(key []byte, value *core.Record) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrInvalidRecord
	}
	rec := *value
	if len(rec.Key) == 0 {
		rec.Key = append([]byte(nil), key...)
	} else if !bytes.Equal(rec.Key, key) {
		return ErrKeyMismatch
	}
	if m.opts.AutoFillTimestamp && rec.Timestamp == 0 {
		rec.Timestamp = uint64(time.Now().Unix())
	}
	payload, err := encodePutPayload(key, &rec)
	if err != nil {
		return err
	}

	oldLoc, oldErr := m.mem.Get(key)
	hadOld := oldErr == nil

	prevActive := m.store.ActiveFileID()
	loc, err := m.appendLocked(payload, fileio.RecordPut)
	if err != nil {
		return err // mem unchanged
	}
	if err := m.mem.Put(key, loc); err != nil {
		m.revertMemPut(key, oldLoc, hadOld)
		return err
	}
	m.afterWriteLocked(prevActive)
	return nil
}

func (m *storeMgr) Delete(ctx context.Context, key []byte) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrDiskStoreClosed
	}
	return m.deleteLocked(key)
}

func (m *storeMgr) deleteLocked(key []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	oldLoc, err := m.mem.Get(key)
	hadOld := err == nil
	if !hadOld && !m.opts.DeleteWritesTombstoneIfMissing {
		return nil
	}
	payload, err := encodeDeletePayload(key)
	if err != nil {
		return err
	}
	prevActive := m.store.ActiveFileID()
	if _, err := m.appendLocked(payload, fileio.RecordDelete); err != nil {
		return err // mem unchanged
	}
	m.mem.Delete(key)
	// If a future MemStore.Delete could fail, restore with: m.mem.Put(key, oldLoc) when hadOld.
	_ = oldLoc
	_ = hadOld
	m.afterWriteLocked(prevActive)
	return nil
}

func (m *storeMgr) appendLocked(payload []byte, typ fileio.RecordType) (fileio.Location, error) {
	if m.opts.SyncMode == fileio.SyncEveryWrite {
		return m.store.AppendSync(payload, typ)
	}
	return m.store.Append(payload, typ)
}

func (m *storeMgr) afterWriteLocked(prevActive uint32) {
	_ = m.store.SealIfNeeded()
	cur := m.store.ActiveFileID()
	if prevActive != 0 && cur != 0 && prevActive != cur {
		m.buildHintLocked(prevActive)
	}
}

func (m *storeMgr) buildHintLocked(fileID uint32) {
	if err := m.hintMgr.BuildFromSegment(m.store, fileID, decodeKey); err != nil {
		utils.GetLogger().Printf("storemgr: build hint id=%d err=%v", fileID, err)
	}
}

func (m *storeMgr) Iterate(ctx context.Context, callback func(key []byte, value *core.Record) bool) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}
	type item struct {
		key []byte
		loc fileio.Location
	}
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return ErrDiskStoreClosed
	}
	var items []item
	m.mem.Iterate(func(key []byte, loc fileio.Location) bool {
		items = append(items, item{key: append([]byte(nil), key...), loc: loc})
		return true
	})
	m.mu.RUnlock()

	now := uint64(time.Now().Unix())
	for _, it := range items {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		payload, typ, err := m.store.Read(it.loc)
		if err != nil || typ != fileio.RecordPut {
			continue
		}
		_, rec, err := decodePutPayload(payload)
		if err != nil {
			continue
		}
		if m.opts.EnableReadTTL && isExpired(rec, now) {
			continue
		}
		if !callback(it.key, rec) {
			return nil
		}
	}
	return nil
}

func (m *storeMgr) BatchPut(ctx context.Context, records []struct {
	Key   []byte
	Value *core.Record
}) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrDiskStoreClosed
	}
	for _, r := range records {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		if err := m.putLocked(r.Key, r.Value); err != nil {
			return err
		}
	}
	return m.batchSyncLocked()
}

func (m *storeMgr) BatchDelete(ctx context.Context, keys [][]byte) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrDiskStoreClosed
	}
	for _, key := range keys {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		if err := m.deleteLocked(key); err != nil {
			return err
		}
	}
	return m.batchSyncLocked()
}

func (m *storeMgr) BatchGet(ctx context.Context, keys [][]byte) ([]struct {
	Key   []byte
	Value *core.Record
}, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	out := make([]struct {
		Key   []byte
		Value *core.Record
	}, len(keys))
	for i, key := range keys {
		if err := checkCtx(ctx); err != nil {
			return nil, err
		}
		out[i].Key = key
		rec, err := m.Get(ctx, key)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) || errors.Is(err, ErrKeyEmpty) {
				out[i].Value = nil
				continue
			}
			return nil, err
		}
		out[i].Value = rec
	}
	return out, nil
}

func (m *storeMgr) batchSyncLocked() error {
	switch m.opts.SyncMode {
	case fileio.SyncNoSync:
		return nil
	default:
		return m.store.Sync()
	}
}

func (m *storeMgr) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	_ = m.store.Sync()
	prev := m.store.ActiveFileID()
	_ = m.store.SealIfNeeded()
	cur := m.store.ActiveFileID()
	if prev != 0 && cur != prev {
		m.buildHintLocked(prev)
	}
	return m.store.Close()
}
