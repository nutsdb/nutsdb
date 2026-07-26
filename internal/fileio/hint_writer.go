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
	"path/filepath"
	"time"

	"github.com/nutsdb/nutsdb/internal/utils"
)

// HintWriter appends HintEntry values to a temporary hint file and publishes
// it atomically via Finish (tmp → rename).
type HintWriter interface {
	Append(entry HintEntry) error
	Sync() error
	Finish() error
	Close() error
}

type hintWriter struct {
	dir    string
	fileID uint32
	path   string // final .hint path
	tmp    string
	fd     *os.File

	buf          []byte
	encodeBuf    []byte
	entryCount   uint64
	payloadBytes uint64
	finished     bool
	closed       bool
}

func hintPath(dir string, id uint32) string {
	return filepath.Join(dir, formatFileID(id)+HintSuffix)
}

func hintTmpPath(dir string, id uint32) string {
	return hintPath(dir, id) + ".tmp"
}

func newHintWriter(dir string, fileID uint32, bufSize int) (*hintWriter, error) {
	if bufSize <= 0 {
		bufSize = DefaultHintBufferSize
	}
	tmp := hintTmpPath(dir, fileID)
	_ = os.Remove(tmp)
	fd, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	w := &hintWriter{
		dir:    dir,
		fileID: fileID,
		path:   hintPath(dir, fileID),
		tmp:    tmp,
		fd:     fd,
		buf:    make([]byte, 0, bufSize),
	}
	if err := w.writeHeader(0); err != nil {
		_ = fd.Close()
		_ = os.Remove(tmp)
		return nil, err
	}
	if _, err := fd.Seek(int64(HintHeaderSize), io.SeekStart); err != nil {
		_ = fd.Close()
		_ = os.Remove(tmp)
		return nil, err
	}
	return w, nil
}

func (w *hintWriter) writeHeader(flags uint16) error {
	buf := make([]byte, HintHeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], HintMagic)
	binary.LittleEndian.PutUint16(buf[4:6], HintVersion)
	binary.LittleEndian.PutUint16(buf[6:8], flags)
	binary.LittleEndian.PutUint32(buf[8:12], w.fileID)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(time.Now().UnixNano()))
	// reserved [20:32)
	crc := crc32.Checksum(buf[0:32], crc32cTable)
	binary.LittleEndian.PutUint32(buf[32:36], crc)
	_, err := w.fd.WriteAt(buf, 0)
	return err
}

func (w *hintWriter) Append(entry HintEntry) error {
	if w.closed || w.finished {
		return ErrHintClosed
	}
	if entry.Loc.FileID != w.fileID {
		return ErrHintInvalidEntry
	}
	raw, err := encodeHintEntry(w.encodeBuf, entry)
	if err != nil {
		return err
	}
	w.encodeBuf = raw

	if len(raw) > cap(w.buf) {
		if err := w.flushBuf(); err != nil {
			return err
		}
		if _, err := w.fd.Write(raw); err != nil {
			return err
		}
	} else {
		if len(w.buf)+len(raw) > cap(w.buf) {
			if err := w.flushBuf(); err != nil {
				return err
			}
		}
		w.buf = append(w.buf, raw...)
	}
	w.entryCount++
	w.payloadBytes += uint64(len(raw))
	return nil
}

func (w *hintWriter) flushBuf() error {
	if len(w.buf) == 0 {
		return nil
	}
	_, err := w.fd.Write(w.buf)
	w.buf = w.buf[:0]
	return err
}

func (w *hintWriter) Sync() error {
	if w.closed || w.finished {
		return ErrHintClosed
	}
	if err := w.flushBuf(); err != nil {
		return err
	}
	return w.fd.Sync()
}

func (w *hintWriter) Finish() error {
	if w.closed {
		return ErrHintClosed
	}
	if w.finished {
		return nil
	}
	if err := w.flushBuf(); err != nil {
		return err
	}
	if err := w.writeHeader(HintFlagComplete); err != nil {
		return err
	}
	if _, err := w.fd.Seek(int64(HintHeaderSize)+int64(w.payloadBytes), io.SeekStart); err != nil {
		return err
	}
	footer := make([]byte, HintFooterSize)
	binary.LittleEndian.PutUint32(footer[0:4], HintSealMagic)
	binary.LittleEndian.PutUint64(footer[4:12], w.entryCount)
	binary.LittleEndian.PutUint64(footer[12:20], w.payloadBytes)
	crc := crc32.Checksum(footer[0:20], crc32cTable)
	binary.LittleEndian.PutUint32(footer[20:24], crc)
	if _, err := w.fd.Write(footer); err != nil {
		return err
	}
	if err := w.fd.Sync(); err != nil {
		return err
	}
	if err := w.fd.Close(); err != nil {
		return err
	}
	w.fd = nil
	if err := os.Rename(w.tmp, w.path); err != nil {
		return err
	}
	// Best-effort directory fsync.
	if dir, err := os.Open(w.dir); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	w.finished = true
	w.closed = true
	utils.GetLogger().Printf("fileio: hint finish id=%d entries=%d payload_bytes=%d path=%s",
		w.fileID, w.entryCount, w.payloadBytes, w.path)
	return nil
}

func (w *hintWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	var first error
	if w.fd != nil {
		if err := w.fd.Close(); err != nil {
			first = err
		}
		w.fd = nil
	}
	if !w.finished {
		if err := os.Remove(w.tmp); err != nil && !os.IsNotExist(err) && first == nil {
			first = err
		}
	}
	return first
}
