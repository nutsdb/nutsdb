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
	"os"
	"sort"
	"sync"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/fileio"
)

type lsmStoreMgr struct {
	opts   LSMOptions
	vlog   fileio.Store
	wal    *walStore
	vs     *VersionSet
	sstDir string

	mu     sync.Mutex
	mem    *MemTable
	imms   []*MemTable
	seq    uint64
	closed bool

	// cache of open SST readers by file number
	readers map[uint64]*sstReader
}

func openLSMStoreManager(opts LSMOptions) (StoreManager, error) {
	opts = normalizeLSMOptions(opts)
	if err := os.MkdirAll(opts.Dir, dirPerm); err != nil {
		return nil, err
	}
	sstDir := defaultSSTDir(opts.Dir)
	walDir := defaultWALDir(opts.Dir)
	if opts.ValueLog.Dir == "" {
		opts.ValueLog.Dir = defaultVLogDir(opts.Dir)
	}

	vlog, err := fileio.Open(opts.ValueLog)
	if err != nil {
		return nil, err
	}
	walBase := fileio.DefaultOptions(walDir)
	walBase.SegmentSize = opts.ValueLog.SegmentSize
	if walBase.SegmentSize == 0 {
		walBase.SegmentSize = fileio.DefaultSegmentSize
	}
	wal, err := openWAL(walDir, opts.WALSyncMode, walBase)
	if err != nil {
		_ = vlog.Close()
		return nil, err
	}
	vs, err := recoverVersionSet(opts.Dir, sstDir)
	if err != nil {
		_ = wal.Close()
		_ = vlog.Close()
		return nil, err
	}

	m := &lsmStoreMgr{
		opts:    opts,
		vlog:    vlog,
		wal:     wal,
		vs:      vs,
		sstDir:  sstDir,
		mem:     newMemTable(),
		readers: make(map[uint64]*sstReader),
		seq:     vs.Current().LastSequence,
	}

	maxSeq, err := wal.Replay(func(op byte, key []byte, ref ValueRef, seq uint64) error {
		if seq <= m.vs.Current().LogNumber {
			return nil
		}
		if seq > m.seq {
			m.seq = seq
		}
		return m.mem.Put(key, ref, seq)
	})
	if err != nil {
		_ = m.Close()
		return nil, err
	}
	if maxSeq > m.seq {
		m.seq = maxSeq
	}
	return m, nil
}

func normalizeLSMOptions(opts LSMOptions) LSMOptions {
	def := DefaultLSMOptions(opts.Dir)
	if opts.Dir == "" {
		opts.Dir = def.Dir
	}
	if opts.ValueLog.Dir == "" {
		opts.ValueLog = def.ValueLog
	}
	if opts.MemTableSize <= 0 {
		opts.MemTableSize = def.MemTableSize
	}
	if opts.ValueInlineThreshold <= 0 {
		opts.ValueInlineThreshold = def.ValueInlineThreshold
	}
	if opts.L0FileNumCompactionTrigger <= 0 {
		opts.L0FileNumCompactionTrigger = def.L0FileNumCompactionTrigger
	}
	if opts.LevelSizeMultiplier <= 0 {
		opts.LevelSizeMultiplier = def.LevelSizeMultiplier
	}
	if opts.LevelBaseSize <= 0 {
		opts.LevelBaseSize = def.LevelBaseSize
	}
	return opts
}

func (m *lsmStoreMgr) nextSeq() uint64 {
	m.seq++
	return m.seq
}

func (m *lsmStoreMgr) Put(ctx context.Context, key []byte, value *core.Record) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrStoreClosed
	}
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
	if rec.Timestamp == 0 {
		rec.Timestamp = uint64(time.Now().Unix())
	}

	var ref ValueRef
	if len(rec.Value) >= m.opts.ValueInlineThreshold {
		payload, err := encodePutPayload(key, &rec)
		if err != nil {
			return err
		}
		var loc fileio.Location
		if m.opts.WALSyncMode == fileio.SyncEveryWrite {
			loc, err = m.vlog.AppendSync(payload, fileio.RecordPut)
		} else {
			loc, err = m.vlog.Append(payload, fileio.RecordPut)
		}
		if err != nil {
			return err
		}
		ref = ValueRef{Kind: ValueKindLocation, Loc: loc, Timestamp: rec.Timestamp, TTL: rec.TTL}
	} else {
		ref = ValueRef{
			Kind:      ValueKindInline,
			Inline:    append([]byte(nil), rec.Value...),
			Timestamp: rec.Timestamp,
			TTL:       rec.TTL,
		}
	}
	seq := m.nextSeq()
	if err := m.wal.Append(walOpPut, key, ref, seq); err != nil {
		return err
	}
	if err := m.mem.Put(key, ref, seq); err != nil {
		return err
	}
	return m.maybeFlushLocked()
}

func (m *lsmStoreMgr) Delete(ctx context.Context, key []byte) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrStoreClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	ref := ValueRef{Kind: ValueKindTombstone, Timestamp: uint64(time.Now().Unix())}
	seq := m.nextSeq()
	if err := m.wal.Append(walOpDelete, key, ref, seq); err != nil {
		return err
	}
	if err := m.mem.Put(key, ref, seq); err != nil {
		return err
	}
	return m.maybeFlushLocked()
}

func (m *lsmStoreMgr) Get(ctx context.Context, key []byte) (*core.Record, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrStoreClosed
	}
	if len(key) == 0 {
		return nil, ErrKeyEmpty
	}
	if v, ok := m.mem.Get(key); ok {
		return m.resolveLocked(key, v.Ref)
	}
	for i := len(m.imms) - 1; i >= 0; i-- {
		if v, ok := m.imms[i].Get(key); ok {
			return m.resolveLocked(key, v.Ref)
		}
	}
	ver := m.vs.Current()
	// L0 newest last in Files[0] — search reverse
	if len(ver.Files) > 0 {
		for i := len(ver.Files[0]) - 1; i >= 0; i-- {
			fm := ver.Files[0][i]
			ref, _, ok, err := m.getFromSSTLocked(fm.FileNumber, key)
			if err != nil {
				return nil, err
			}
			if ok {
				return m.resolveLocked(key, ref)
			}
		}
	}
	for level := 1; level < len(ver.Files); level++ {
		files := ver.Files[level]
		for _, fm := range files {
			if bytes.Compare(key, fm.Smallest) < 0 || bytes.Compare(key, fm.Largest) > 0 {
				continue
			}
			ref, _, ok, err := m.getFromSSTLocked(fm.FileNumber, key)
			if err != nil {
				return nil, err
			}
			if ok {
				return m.resolveLocked(key, ref)
			}
		}
	}
	return nil, ErrKeyNotFound
}

func (m *lsmStoreMgr) getFromSSTLocked(num uint64, key []byte) (ValueRef, uint64, bool, error) {
	rd, err := m.getReaderLocked(num)
	if err != nil {
		return ValueRef{}, 0, false, err
	}
	return rd.Get(key)
}

func (m *lsmStoreMgr) getReaderLocked(num uint64) (*sstReader, error) {
	if rd, ok := m.readers[num]; ok {
		return rd, nil
	}
	rd, err := openSSTReader(m.sstDir, num)
	if err != nil {
		return nil, err
	}
	m.readers[num] = rd
	return rd, nil
}

func (m *lsmStoreMgr) resolveLocked(key []byte, ref ValueRef) (*core.Record, error) {
	now := uint64(time.Now().Unix())
	switch ref.Kind {
	case ValueKindTombstone:
		return nil, ErrKeyNotFound
	case ValueKindInline:
		rec := &core.Record{
			Key:       append([]byte(nil), key...),
			Value:     append([]byte(nil), ref.Inline...),
			Timestamp: ref.Timestamp,
			TTL:       ref.TTL,
		}
		if isExpired(rec, now) {
			return nil, ErrKeyNotFound
		}
		return rec, nil
	case ValueKindLocation:
		payload, typ, err := m.vlog.Read(ref.Loc)
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
		if isExpired(rec, now) {
			return nil, ErrKeyNotFound
		}
		return rec, nil
	default:
		return nil, ErrKeyNotFound
	}
}

func (m *lsmStoreMgr) maybeFlushLocked() error {
	if m.mem.ApproxBytes() < m.opts.MemTableSize {
		return nil
	}
	return m.flushMemLocked()
}

func (m *lsmStoreMgr) flushMemLocked() error {
	if m.mem.Size() == 0 {
		return nil
	}
	imm := m.mem
	imm.Freeze()
	m.imms = append(m.imms, imm)
	m.mem = newMemTable()

	fileNum := m.vs.NewFileNumber()
	w, err := newSSTWriter(m.sstDir, fileNum)
	if err != nil {
		return err
	}
	var maxSeq uint64
	imm.Iterate(func(key []byte, v memValue) bool {
		if v.Seq > maxSeq {
			maxSeq = v.Seq
		}
		if err2 := w.Add(key, v.Ref, v.Seq); err2 != nil {
			err = err2
			return false
		}
		return true
	})
	if err != nil {
		_ = w.Abandon()
		return err
	}
	meta, err := w.Finish()
	if err != nil {
		return err
	}
	nextFile := fileNum + 1
	edit := &VersionEdit{
		Added: []FileMeta{{
			FileNumber: meta.FileNumber,
			Level:      0,
			Size:       meta.Size,
			Smallest:   meta.Smallest,
			Largest:    meta.Largest,
		}},
		LastSequence:   &maxSeq,
		NextFileNumber: &nextFile,
		LogNumber:      &maxSeq, // reuse as last flushed seq
	}
	if err := m.vs.LogAndApply(edit); err != nil {
		return err
	}
	// drop immutable that we flushed (only one)
	m.imms = m.imms[:len(m.imms)-1]
	// reset WAL so reopen does not need flushed records
	walDir := defaultWALDir(m.opts.Dir)
	walBase := fileio.DefaultOptions(walDir)
	if err := m.wal.Reset(walDir, m.opts.WALSyncMode, walBase); err != nil {
		return err
	}
	return m.maybeCompactLocked()
}

func (m *lsmStoreMgr) Iterate(ctx context.Context, callback func(key []byte, value *core.Record) bool) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrStoreClosed
	}
	merged, err := m.collectMergedLocked()
	if err != nil {
		return err
	}
	now := uint64(time.Now().Unix())
	for _, e := range merged {
		rec, err := m.resolveLocked(e.key, e.ref)
		if err != nil {
			if err == ErrKeyNotFound {
				continue
			}
			return err
		}
		if isExpired(rec, now) {
			continue
		}
		if !callback(e.key, rec) {
			return nil
		}
	}
	return nil
}

type mergeItem struct {
	key []byte
	ref ValueRef
	seq uint64
}

func (m *lsmStoreMgr) collectMergedLocked() ([]mergeItem, error) {

	best := map[string]mergeItem{}
	add := func(key []byte, ref ValueRef, seq uint64) {
		s := string(key)
		if old, ok := best[s]; ok && old.seq >= seq {
			return
		}
		best[s] = mergeItem{key: append([]byte(nil), key...), ref: cloneValueRef(ref), seq: seq}
	}
	m.mem.Iterate(func(key []byte, v memValue) bool {
		add(key, v.Ref, v.Seq)
		return true
	})
	for _, imm := range m.imms {
		imm.Iterate(func(key []byte, v memValue) bool {
			add(key, v.Ref, v.Seq)
			return true
		})
	}
	ver := m.vs.Current()
	for level := range ver.Files {
		for _, fm := range ver.Files[level] {
			rd, err := m.getReaderLocked(fm.FileNumber)
			if err != nil {
				return nil, err
			}
			err = rd.Iterate(func(key []byte, ref ValueRef, seq uint64) error {
				add(key, ref, seq)
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
	}
	out := make([]mergeItem, 0, len(best))
	for _, c := range best {
		if c.ref.Kind == ValueKindTombstone {
			continue
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].key, out[j].key) < 0
	})
	return out, nil
}

func (m *lsmStoreMgr) BatchPut(ctx context.Context, records []struct {
	Key   []byte
	Value *core.Record
}) error {
	for _, r := range records {
		if err := m.Put(ctx, r.Key, r.Value); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrStoreClosed
	}
	_ = m.wal.Sync()
	_ = m.vlog.Sync()
	return nil
}

func (m *lsmStoreMgr) BatchDelete(ctx context.Context, keys [][]byte) error {
	for _, k := range keys {
		if err := m.Delete(ctx, k); err != nil {
			return err
		}
	}
	return nil
}

func (m *lsmStoreMgr) BatchGet(ctx context.Context, keys [][]byte) ([]struct {
	Key   []byte
	Value *core.Record
}, error) {
	out := make([]struct {
		Key   []byte
		Value *core.Record
	}, len(keys))
	for i, k := range keys {
		out[i].Key = k
		rec, err := m.Get(ctx, k)
		if err != nil {
			if err == ErrKeyNotFound {
				continue
			}
			return nil, err
		}
		out[i].Value = rec
	}
	return out, nil
}

func (m *lsmStoreMgr) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	if m.mem != nil && m.mem.Size() > 0 {
		if err := m.flushMemLocked(); err != nil {
			return err
		}
	}
	m.closed = true
	_ = m.wal.Sync()
	_ = m.vlog.Sync()
	_ = m.wal.Close()
	_ = m.vlog.Close()
	_ = m.vs.Close()
	m.readers = nil
	return nil
}

func checkCtx(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
