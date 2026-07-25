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
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	mmap "github.com/edsrzf/mmap-go"

	"github.com/nutsdb/nutsdb/internal/utils"
)

const segmentSuffix = ".seg"

type segment struct {
	id            uint32
	path          string
	fd            *os.File
	capacity      uint64
	usedBytes     uint64
	writtenOffset uint64
	durableOffset uint64
	recordCount   uint64
	sealed        bool

	// mmapData is a read-only mapping of a sealed segment (optional).
	mmapData mmap.MMap
}

func segmentPath(dir string, id uint32) string {
	return filepath.Join(dir, strconv.FormatUint(uint64(id), 10)+segmentSuffix)
}

func parseSegmentID(name string) (uint32, bool) {
	if !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	idStr := strings.TrimSuffix(name, segmentSuffix)
	v, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(v), true
}

func createSegment(dir string, id uint32, capacity uint64) (*segment, error) {
	if capacity <= HeaderSize+FooterSize {
		return nil, ErrInvalidOptions
	}
	path := segmentPath(dir, id)
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	if err := preallocate(fd, int64(capacity)); err != nil {
		_ = fd.Close()
		_ = os.Remove(path)
		return nil, err
	}

	seg := &segment{
		id:            id,
		path:          path,
		fd:            fd,
		capacity:      capacity,
		usedBytes:     HeaderSize,
		writtenOffset: HeaderSize,
		durableOffset: HeaderSize,
	}
	if err := seg.writeHeader(); err != nil {
		_ = seg.close()
		_ = os.Remove(path)
		return nil, err
	}
	if err := seg.sync(); err != nil {
		_ = seg.close()
		_ = os.Remove(path)
		return nil, err
	}
	seg.durableOffset = HeaderSize
	return seg, nil
}

func openSegment(path string, id uint32) (*segment, error) {
	fd, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	seg := &segment{id: id, path: path, fd: fd}
	if err := seg.load(); err != nil {
		_ = fd.Close()
		return nil, err
	}
	return seg, nil
}

func (s *segment) load() error {
	header := make([]byte, HeaderSize)
	if _, err := s.fd.ReadAt(header, 0); err != nil {
		return err
	}
	capacity, err := parseHeader(header)
	if err != nil {
		return err
	}
	s.capacity = capacity

	footer := make([]byte, FooterSize)
	if _, err := s.fd.ReadAt(footer, int64(capacity-FooterSize)); err != nil {
		return err
	}
	if used, count, ok := parseFooter(footer); ok {
		if used < HeaderSize || used > capacity-FooterSize {
			return ErrCorrupt
		}
		s.usedBytes = used
		s.writtenOffset = used
		s.durableOffset = used
		s.recordCount = count
		s.sealed = true
		return nil
	}

	used, count, err := scanRecords(s.fd, capacity)
	if err != nil {
		return err
	}
	s.usedBytes = used
	s.writtenOffset = used
	s.durableOffset = used
	s.recordCount = count
	s.sealed = false
	return nil
}

func (s *segment) writeHeader() error {
	buf := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], SegmentMagic)
	binary.LittleEndian.PutUint16(buf[4:6], FormatVersion)
	binary.LittleEndian.PutUint16(buf[6:8], 0) // flags
	binary.LittleEndian.PutUint64(buf[8:16], s.capacity)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(time.Now().UnixNano()))
	crc := crc32.Checksum(buf[0:24], crc32cTable)
	binary.LittleEndian.PutUint32(buf[24:28], crc)
	_, err := s.fd.WriteAt(buf, 0)
	return err
}

func parseHeader(buf []byte) (capacity uint64, err error) {
	if len(buf) < 28 {
		return 0, ErrCorrupt
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != SegmentMagic {
		return 0, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(buf[4:6]) != FormatVersion {
		return 0, ErrCorrupt
	}
	capacity = binary.LittleEndian.Uint64(buf[8:16])
	got := binary.LittleEndian.Uint32(buf[24:28])
	want := crc32.Checksum(buf[0:24], crc32cTable)
	if got != want {
		return 0, ErrCorrupt
	}
	if capacity <= HeaderSize+FooterSize {
		return 0, ErrCorrupt
	}
	return capacity, nil
}

func (s *segment) writeFooter() error {
	buf := make([]byte, FooterSize)
	binary.LittleEndian.PutUint32(buf[0:4], SealMagic)
	binary.LittleEndian.PutUint64(buf[4:12], s.recordCount)
	binary.LittleEndian.PutUint64(buf[12:20], s.usedBytes)
	crc := crc32.Checksum(buf[0:20], crc32cTable)
	binary.LittleEndian.PutUint32(buf[20:24], crc)
	_, err := s.fd.WriteAt(buf, int64(s.capacity-FooterSize))
	return err
}

func parseFooter(buf []byte) (usedBytes, recordCount uint64, ok bool) {
	if len(buf) < 24 {
		return 0, 0, false
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != SealMagic {
		return 0, 0, false
	}
	recordCount = binary.LittleEndian.Uint64(buf[4:12])
	usedBytes = binary.LittleEndian.Uint64(buf[12:20])
	got := binary.LittleEndian.Uint32(buf[20:24])
	want := crc32.Checksum(buf[0:20], crc32cTable)
	if got != want {
		return 0, 0, false
	}
	return usedBytes, recordCount, true
}

func scanRecords(fd *os.File, capacity uint64) (usedBytes, recordCount uint64, err error) {
	limit := capacity - FooterSize
	offset := uint64(HeaderSize)
	hdr := make([]byte, RecordHeaderSize)

	for offset+RecordHeaderSize <= limit {
		if _, err := fd.ReadAt(hdr, int64(offset)); err != nil {
			return HeaderSize, 0, err
		}
		// All-zero header likely means unused free space in a preallocated file.
		if binary.LittleEndian.Uint32(hdr[0:4]) == 0 &&
			binary.LittleEndian.Uint32(hdr[4:8]) == 0 &&
			hdr[8] == 0 {
			break
		}
		payloadLen := binary.LittleEndian.Uint32(hdr[4:8])
		recLen := recordLength(int(payloadLen))
		if offset+uint64(recLen) > limit {
			break
		}
		buf := make([]byte, recLen)
		if _, err := fd.ReadAt(buf, int64(offset)); err != nil {
			return HeaderSize, 0, err
		}
		if _, _, err := decodeRecord(buf); err != nil {
			break
		}
		offset += uint64(recLen)
		recordCount++
	}
	return offset, recordCount, nil
}

func (s *segment) writeAt(p []byte, off uint64) error {
	if s.mmapData != nil {
		return fmt.Errorf("fileio: write through mmap is not allowed")
	}
	n, err := s.fd.WriteAt(p, int64(off))
	if err != nil {
		return err
	}
	if n != len(p) {
		return fmt.Errorf("fileio: short write: %d/%d", n, len(p))
	}
	return nil
}

func (s *segment) readAt(p []byte, off uint64) error {
	if len(p) == 0 {
		return nil
	}
	if s.mmapData != nil {
		end := off + uint64(len(p))
		if end < off || end > uint64(len(s.mmapData)) {
			return ErrInvalidLocation
		}
		copy(p, s.mmapData[off:end])
		return nil
	}
	n, err := s.fd.ReadAt(p, int64(off))
	if err != nil {
		return err
	}
	if n != len(p) {
		return fmt.Errorf("fileio: short read: %d/%d", n, len(p))
	}
	return nil
}

func (s *segment) sync() error {
	return s.fd.Sync()
}

// mapReadonly maps a sealed segment into memory for random reads.
// Active (writable) segments must never be mapped.
func (s *segment) mapReadonly() error {
	if !s.sealed {
		return fmt.Errorf("fileio: refuse to mmap unsealed segment %d", s.id)
	}
	if s.mmapData != nil {
		return nil
	}
	if s.fd == nil {
		return fmt.Errorf("fileio: mmap segment %d: nil fd", s.id)
	}
	data, err := mmap.Map(s.fd, mmap.RDONLY, 0)
	if err != nil {
		return err
	}
	s.mmapData = data
	utils.GetLogger().Printf("fileio: mmap sealed segment id=%d path=%s size=%d", s.id, s.path, len(data))
	return nil
}

func (s *segment) unmap() error {
	if s.mmapData == nil {
		return nil
	}
	size := len(s.mmapData)
	err := s.mmapData.Unmap()
	s.mmapData = nil
	if err != nil {
		utils.GetLogger().Printf("fileio: munmap sealed segment id=%d path=%s size=%d err=%v", s.id, s.path, size, err)
		return err
	}
	utils.GetLogger().Printf("fileio: munmap sealed segment id=%d path=%s size=%d", s.id, s.path, size)
	return nil
}

func (s *segment) close() error {
	var firstErr error
	if err := s.unmap(); err != nil {
		firstErr = err
	}
	if s.fd != nil {
		if err := s.fd.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.fd = nil
	}
	return firstErr
}

func (s *segment) remaining() uint64 {
	limit := s.capacity - FooterSize
	if s.usedBytes >= limit {
		return 0
	}
	return limit - s.usedBytes
}

func listSegmentIDs(dir string) ([]uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	ids := make([]uint32, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		id, ok := parseSegmentID(e.Name())
		if !ok {
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func preallocate(fd *os.File, size int64) error {
	if err := fallocate(fd, size); err == nil {
		return nil
	}
	return fd.Truncate(size)
}
