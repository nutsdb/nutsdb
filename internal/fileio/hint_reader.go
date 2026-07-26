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
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"strings"
)

// HintReader iterates complete hint files in entry order.
type HintReader interface {
	Iterate(fn func(HintEntry) error) error
	Close() error
}

type hintReader struct {
	fileID       uint32
	fd           *os.File
	data         []byte // payload region (entries only)
	entryCount   uint64
	payloadBytes uint64
	closed       bool
}

func openHintReader(path string, expectFileID uint32) (*hintReader, error) {
	fd, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrHintNotFound
		}
		return nil, err
	}
	info, err := fd.Stat()
	if err != nil {
		_ = fd.Close()
		return nil, err
	}
	size := info.Size()
	if size < int64(HintHeaderSize+HintFooterSize) {
		_ = fd.Close()
		return nil, ErrHintIncomplete
	}

	header := make([]byte, HintHeaderSize)
	if _, err := io.ReadFull(fd, header); err != nil {
		_ = fd.Close()
		return nil, err
	}
	fileID, flags, err := parseHintHeader(header)
	if err != nil {
		_ = fd.Close()
		return nil, err
	}
	if fileID != expectFileID {
		_ = fd.Close()
		return nil, ErrHintCorrupt
	}
	if flags&HintFlagComplete == 0 {
		_ = fd.Close()
		return nil, ErrHintIncomplete
	}

	footerOff := size - int64(HintFooterSize)
	footer := make([]byte, HintFooterSize)
	if _, err := fd.ReadAt(footer, footerOff); err != nil {
		_ = fd.Close()
		return nil, err
	}
	entryCount, payloadBytes, err := parseHintFooter(footer)
	if err != nil {
		_ = fd.Close()
		return nil, err
	}
	if int64(HintHeaderSize)+int64(payloadBytes)+int64(HintFooterSize) != size {
		_ = fd.Close()
		return nil, ErrHintCorrupt
	}

	payload := make([]byte, payloadBytes)
	if payloadBytes > 0 {
		if _, err := fd.ReadAt(payload, int64(HintHeaderSize)); err != nil {
			_ = fd.Close()
			return nil, err
		}
	}

	return &hintReader{
		fileID:       fileID,
		fd:           fd,
		data:         payload,
		entryCount:   entryCount,
		payloadBytes: payloadBytes,
	}, nil
}

func parseHintHeader(buf []byte) (fileID uint32, flags uint16, err error) {
	if len(buf) < HintHeaderSize {
		return 0, 0, ErrHintCorrupt
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != HintMagic {
		return 0, 0, ErrHintCorrupt
	}
	if binary.LittleEndian.Uint16(buf[4:6]) != HintVersion {
		return 0, 0, ErrHintCorrupt
	}
	flags = binary.LittleEndian.Uint16(buf[6:8])
	fileID = binary.LittleEndian.Uint32(buf[8:12])
	got := binary.LittleEndian.Uint32(buf[32:36])
	want := crc32.Checksum(buf[0:32], crc32cTable)
	if got != want {
		return 0, 0, ErrHintCorrupt
	}
	return fileID, flags, nil
}

func parseHintFooter(buf []byte) (entryCount, payloadBytes uint64, err error) {
	if len(buf) < 24 {
		return 0, 0, ErrHintIncomplete
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != HintSealMagic {
		return 0, 0, ErrHintIncomplete
	}
	entryCount = binary.LittleEndian.Uint64(buf[4:12])
	payloadBytes = binary.LittleEndian.Uint64(buf[12:20])
	got := binary.LittleEndian.Uint32(buf[20:24])
	want := crc32.Checksum(buf[0:20], crc32cTable)
	if got != want {
		return 0, 0, ErrHintCorrupt
	}
	return entryCount, payloadBytes, nil
}

func (r *hintReader) Iterate(fn func(HintEntry) error) error {
	if r.closed {
		return ErrHintClosed
	}
	off := 0
	var n uint64
	for off < len(r.data) {
		entry, size, err := decodeHintEntry(r.data[off:], r.fileID)
		if err != nil {
			return err
		}
		if err := fn(entry); err != nil {
			return err
		}
		off += size
		n++
	}
	if n != r.entryCount {
		return ErrHintCorrupt
	}
	if off != len(r.data) {
		return ErrHintCorrupt
	}
	return nil
}

func (r *hintReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.data = nil
	if r.fd == nil {
		return nil
	}
	err := r.fd.Close()
	r.fd = nil
	return err
}

func parseHintID(name string) (uint32, bool) {
	if !strings.HasSuffix(name, HintSuffix) {
		return 0, false
	}
	idStr := strings.TrimSuffix(name, HintSuffix)
	if len(idStr) != FileIDWidth {
		return 0, false
	}
	for i := 0; i < len(idStr); i++ {
		if idStr[i] < '0' || idStr[i] > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(v), true
}

func listHintIDs(dir string) ([]uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	ids := make([]uint32, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		id, ok := parseHintID(e.Name())
		if !ok {
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}
