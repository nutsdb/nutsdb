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

package fileio

import (
	"encoding/binary"
	"os"
	"sort"
	"sync"

	"github.com/nutsdb/nutsdb/internal/utils"
)

// Store is the storage I/O layer facade for append-only segment files.
type Store interface {
	// Append encodes and appends a record to the active segment.
	// It does not wait for durability; callers that need fsync should use AppendSync or Sync.
	// Returns a stable Location of the written record.
	Append(payload []byte, typ RecordType) (Location, error)

	// AppendSync appends a record and then Syncs so the record is durable before return.
	AppendSync(payload []byte, typ RecordType) (Location, error)

	// Sync flushes the write buffer and fsyncs the active segment so durable_offset
	// catches up with written_offset.
	Sync() error

	// Read loads the record at loc, verifies its checksum, and returns a copy of the payload
	// together with its RecordType. The returned slice is owned by the caller.
	Read(loc Location) (payload []byte, typ RecordType, err error)

	// ReadInto is like Read but prefers writing the payload into buf to avoid allocation
	// when buf has enough capacity; otherwise it allocates a new slice.
	ReadInto(loc Location, buf []byte) (payload []byte, typ RecordType, err error)

	// Iterate walks records in fileID in offset order and invokes fn for each complete record.
	// Iteration stops and returns the first non-nil error from fn.
	Iterate(fileID uint32, fn func(loc Location, typ RecordType, payload []byte) error) error

	// SealIfNeeded seals the active segment when remaining space cannot fit MaxRecordSize,
	// then creates a new active segment.
	SealIfNeeded() error

	// DeleteSegment removes a sealed segment file and drops it from the open-segment cache.
	// Deleting the active segment is rejected.
	DeleteSegment(fileID uint32) error

	// ActiveFileID returns the current active segment FileID, or 0 if none.
	ActiveFileID() uint32

	// ListFileIDs returns all segment FileIDs found under the store directory.
	ListFileIDs() ([]uint32, error)

	// Close flushes buffered writes, releases mmap/fd resources, and makes the Store unusable.
	Close() error
}

type store struct {
	opts Options

	mu     sync.Mutex
	closed bool

	active *segment
	nextID uint32

	// opened segments cache (including active)
	segments map[uint32]*segment
	lru      []uint32 // front = most recent

	writeBuf       []byte
	encodeBuf      []byte
	bytesSinceSync uint64
}

// Open creates or recovers a Store under opts.Dir.
func Open(opts Options) (Store, error) {
	opts, err := opts.withDefaults()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}

	s := &store{
		opts:     opts,
		segments: make(map[uint32]*segment),
		writeBuf: make([]byte, 0, opts.WriteBufferSize),
	}
	utils.GetLogger().Printf("fileio: open dir=%s backend=%s segment_size=%d",
		opts.Dir, opts.ReadBackend, opts.SegmentSize)
	if err := s.recover(); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func (s *store) recover() error {
	ids, err := listSegmentIDs(s.opts.Dir)
	if err != nil {
		return err
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var maxID uint32
	var activeCandidate *segment
	for _, id := range ids {
		if id > maxID {
			maxID = id
		}
		seg, err := openSegment(segmentPath(s.opts.Dir, id), id)
		if err != nil {
			return err
		}
		s.segments[id] = seg
		s.touchLRU(id)
		s.maybeMapSealedLocked(seg)
		if !seg.sealed {
			if activeCandidate != nil {
				// Multiple unsealed segments: keep the newest as active, seal older ones best-effort.
				if err := s.sealSegment(activeCandidate); err != nil {
					return err
				}
			}
			activeCandidate = seg
		}
	}
	s.nextID = maxID

	if activeCandidate == nil {
		if err := s.createActiveLocked(); err != nil {
			return err
		}
	} else {
		s.active = activeCandidate
	}
	s.evictLRULocked()
	return nil
}

func (s *store) createActiveLocked() error {
	s.nextID++
	id := s.nextID
	seg, err := createSegment(s.opts.Dir, id, s.opts.SegmentSize)
	if err != nil {
		return err
	}
	s.active = seg
	s.segments[id] = seg
	s.touchLRU(id)
	return nil
}

func (s *store) Append(payload []byte, typ RecordType) (Location, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Location{}, ErrStoreClosed
	}
	return s.appendLocked(payload, typ, false)
}

func (s *store) AppendSync(payload []byte, typ RecordType) (Location, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Location{}, ErrStoreClosed
	}
	loc, err := s.appendLocked(payload, typ, false)
	if err != nil {
		return Location{}, err
	}
	if err := s.syncLocked(); err != nil {
		return Location{}, err
	}
	return loc, nil
}

func (s *store) appendLocked(payload []byte, typ RecordType, retried bool) (Location, error) {
	if uint64(len(payload))+uint64(RecordHeaderSize) > uint64(^uint32(0)) {
		return Location{}, ErrRecordTooLarge
	}
	length := recordLength(len(payload))
	if uint64(length) > s.opts.MaxRecordSize || uint64(length) > s.opts.usable() {
		return Location{}, ErrRecordTooLarge
	}
	if s.active == nil {
		if err := s.createActiveLocked(); err != nil {
			return Location{}, err
		}
	}
	if uint64(length) > s.active.remaining() {
		if retried {
			return Location{}, ErrSegmentFull
		}
		if err := s.sealActiveLocked(); err != nil {
			return Location{}, err
		}
		return s.appendLocked(payload, typ, true)
	}

	rec := encodeRecord(s.encodeBuf, payload, typ)
	s.encodeBuf = rec

	offset := s.active.usedBytes
	if int(length) > s.opts.WriteBufferSize {
		if err := s.flushBufferLocked(); err != nil {
			return Location{}, err
		}
		if err := s.active.writeAt(rec, offset); err != nil {
			return Location{}, err
		}
		s.active.usedBytes += uint64(length)
		s.active.writtenOffset = s.active.usedBytes
		s.active.recordCount++
		s.bytesSinceSync += uint64(length)
	} else {
		if len(s.writeBuf)+len(rec) > s.opts.WriteBufferSize {
			if err := s.flushBufferLocked(); err != nil {
				return Location{}, err
			}
		}
		if len(s.writeBuf) == 0 {
			// buffer starts at current usedBytes
		}
		s.writeBuf = append(s.writeBuf, rec...)
		s.active.usedBytes += uint64(length)
		s.active.recordCount++
		s.bytesSinceSync += uint64(length)
		if len(s.writeBuf) >= s.opts.WriteBufferSize {
			if err := s.flushBufferLocked(); err != nil {
				return Location{}, err
			}
		}
	}

	loc := Location{FileID: s.active.id, Offset: offset, Length: length}
	if s.opts.SyncMode == SyncEveryWrite {
		if err := s.syncLocked(); err != nil {
			return Location{}, err
		}
	} else if s.opts.SyncMode == SyncBatch && s.bytesSinceSync >= uint64(s.opts.WriteBufferSize) {
		if err := s.syncLocked(); err != nil {
			return Location{}, err
		}
	}
	return loc, nil
}

func (s *store) flushBufferLocked() error {
	if len(s.writeBuf) == 0 || s.active == nil {
		return nil
	}
	// Buffer always contains a contiguous suffix ending at usedBytes.
	off := s.active.usedBytes - uint64(len(s.writeBuf))
	if err := s.active.writeAt(s.writeBuf, off); err != nil {
		return err
	}
	s.active.writtenOffset = s.active.usedBytes
	s.writeBuf = s.writeBuf[:0]
	return nil
}

func (s *store) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	return s.syncLocked()
}

func (s *store) syncLocked() error {
	if err := s.flushBufferLocked(); err != nil {
		return err
	}
	if s.active == nil {
		return nil
	}
	if err := s.active.sync(); err != nil {
		return err
	}
	s.active.durableOffset = s.active.writtenOffset
	s.bytesSinceSync = 0
	return nil
}

func (s *store) SealIfNeeded() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	if s.active == nil {
		return nil
	}
	// Seal when less than MaxRecordSize remains (cannot fit a max record).
	if s.active.remaining() >= s.opts.MaxRecordSize {
		return nil
	}
	return s.sealActiveLocked()
}

func (s *store) sealActiveLocked() error {
	if s.active == nil {
		return nil
	}
	if err := s.syncLocked(); err != nil {
		return err
	}
	if err := s.sealSegment(s.active); err != nil {
		return err
	}
	s.active = nil
	return s.createActiveLocked()
}

func (s *store) sealSegment(seg *segment) error {
	if seg.sealed {
		return nil
	}
	if err := seg.writeFooter(); err != nil {
		return err
	}
	if err := seg.sync(); err != nil {
		return err
	}
	seg.sealed = true
	seg.durableOffset = seg.usedBytes
	seg.writtenOffset = seg.usedBytes
	utils.GetLogger().Printf("fileio: sealed segment id=%d used_bytes=%d records=%d",
		seg.id, seg.usedBytes, seg.recordCount)
	s.maybeMapSealedLocked(seg)
	return nil
}

// maybeMapSealedLocked maps a sealed segment when ReadBackendMMapSealed is enabled.
// On failure it logs and keeps FileIO reads (best-effort).
func (s *store) maybeMapSealedLocked(seg *segment) {
	if seg == nil || !seg.sealed || s.opts.ReadBackend != ReadBackendMMapSealed {
		return
	}
	if err := seg.mapReadonly(); err != nil {
		utils.GetLogger().Printf("fileio: mmap sealed segment id=%d failed: %v; fallback to fileio", seg.id, err)
	}
}

func (s *store) Read(loc Location) ([]byte, RecordType, error) {
	return s.ReadInto(loc, nil)
}

func (s *store) ReadInto(loc Location, buf []byte) ([]byte, RecordType, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, 0, ErrStoreClosed
	}
	raw, err := s.readRawLocked(loc)
	if err != nil {
		return nil, 0, err
	}
	return decodeRecordInto(raw, buf)
}

func (s *store) readRawLocked(loc Location) ([]byte, error) {
	if loc.Length == 0 || loc.Length < RecordHeaderSize {
		return nil, ErrInvalidLocation
	}
	if loc.Offset < HeaderSize {
		return nil, ErrInvalidLocation
	}
	end := loc.Offset + uint64(loc.Length)
	if end < loc.Offset {
		return nil, ErrInvalidLocation
	}

	seg, err := s.getSegmentLocked(loc.FileID)
	if err != nil {
		return nil, err
	}
	// In-process: active readable up to usedBytes (includes unflushed write buffer).
	// Sealed / recovered segments keep usedBytes == writtenOffset.
	limit := seg.usedBytes
	if end > limit {
		return nil, ErrInvalidLocation
	}
	if end > seg.capacity-FooterSize {
		return nil, ErrInvalidLocation
	}

	raw := make([]byte, loc.Length)
	if ok := s.copyFromWriteBufLocked(seg, raw, loc.Offset); ok {
		return raw, nil
	}
	if err := seg.readAt(raw, loc.Offset); err != nil {
		return nil, err
	}
	return raw, nil
}

// copyFromWriteBufLocked copies [off, off+len(dst)) from the active write buffer if fully covered.
func (s *store) copyFromWriteBufLocked(seg *segment, dst []byte, off uint64) bool {
	if s.active == nil || seg != s.active || len(s.writeBuf) == 0 {
		return false
	}
	bufStart := s.active.usedBytes - uint64(len(s.writeBuf))
	end := off + uint64(len(dst))
	if off < bufStart || end > s.active.usedBytes {
		return false
	}
	copy(dst, s.writeBuf[off-bufStart:end-bufStart])
	return true
}

func (s *store) Iterate(fileID uint32, fn func(loc Location, typ RecordType, payload []byte) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	seg, err := s.getSegmentLocked(fileID)
	if err != nil {
		return err
	}
	limit := seg.usedBytes
	offset := uint64(HeaderSize)
	hdr := make([]byte, RecordHeaderSize)
	for offset+RecordHeaderSize <= limit {
		if ok := s.copyFromWriteBufLocked(seg, hdr, offset); !ok {
			if err := seg.readAt(hdr, offset); err != nil {
				return err
			}
		}
		payloadLen := binary.LittleEndian.Uint32(hdr[4:8])
		recLen := recordLength(int(payloadLen))
		if offset+uint64(recLen) > limit {
			return ErrCorrupt
		}
		raw := make([]byte, recLen)
		if ok := s.copyFromWriteBufLocked(seg, raw, offset); !ok {
			if err := seg.readAt(raw, offset); err != nil {
				return err
			}
		}
		payload, typ, err := decodeRecord(raw)
		if err != nil {
			return err
		}
		loc := Location{FileID: fileID, Offset: offset, Length: recLen}
		if err := fn(loc, typ, payload); err != nil {
			return err
		}
		offset += uint64(recLen)
	}
	return nil
}

func (s *store) DeleteSegment(fileID uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	if s.active != nil && s.active.id == fileID {
		return ErrStaleLocation
	}
	seg, ok := s.segments[fileID]
	if ok {
		utils.GetLogger().Printf("fileio: delete segment id=%d path=%s mmap=%v",
			fileID, seg.path, seg.mmapData != nil)
		_ = seg.close()
		delete(s.segments, fileID)
		s.removeLRU(fileID)
	}
	path := segmentPath(s.opts.Dir, fileID)
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return ErrSegmentNotFound
		}
		return err
	}
	return nil
}

func (s *store) ActiveFileID() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == nil {
		return 0
	}
	return s.active.id
}

func (s *store) ListFileIDs() ([]uint32, error) {
	ids, err := listSegmentIDs(s.opts.Dir)
	if err != nil {
		return nil, err
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var firstErr error
	if s.active != nil && !s.active.sealed {
		if err := s.flushBufferLocked(); err != nil {
			firstErr = err
		}
		if err := s.active.sync(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for id, seg := range s.segments {
		if err := seg.close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(s.segments, id)
	}
	s.active = nil
	s.lru = nil
	return firstErr
}

func (s *store) getSegmentLocked(id uint32) (*segment, error) {
	if seg, ok := s.segments[id]; ok {
		s.touchLRU(id)
		return seg, nil
	}
	path := segmentPath(s.opts.Dir, id)
	seg, err := openSegment(path, id)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrSegmentNotFound
		}
		return nil, err
	}
	s.segments[id] = seg
	s.touchLRU(id)
	s.maybeMapSealedLocked(seg)
	s.evictLRULocked()
	return seg, nil
}

func (s *store) touchLRU(id uint32) {
	s.removeLRU(id)
	s.lru = append([]uint32{id}, s.lru...)
}

func (s *store) removeLRU(id uint32) {
	for i, v := range s.lru {
		if v == id {
			s.lru = append(s.lru[:i], s.lru[i+1:]...)
			return
		}
	}
}

func (s *store) evictLRULocked() {
	for len(s.segments) > s.opts.MaxOpenSegments {
		// Evict least recent non-active sealed segment.
		victim := uint32(0)
		found := false
		for i := len(s.lru) - 1; i >= 0; i-- {
			id := s.lru[i]
			if s.active != nil && s.active.id == id {
				continue
			}
			victim = id
			found = true
			break
		}
		if !found {
			return
		}
		if seg, ok := s.segments[victim]; ok {
			utils.GetLogger().Printf("fileio: evict segment id=%d mmap=%v open=%d/%d",
				victim, seg.mmapData != nil, len(s.segments), s.opts.MaxOpenSegments)
			_ = seg.close()
			delete(s.segments, victim)
		}
		s.removeLRU(victim)
	}
}
