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
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	sstMagic          uint32 = 0x53535431 // SST1
	sstFooterSize            = 48
	sstFormatVersion         = uint16(1)
	sstDefaultBlockSize      = 4 << 10
)

type sstEntry struct {
	Key []byte
	Ref ValueRef
	Seq uint64
}

type sstFileMeta struct {
	FileNumber uint64
	Size       uint64
	Smallest   []byte
	Largest    []byte
	Level      int
}

type sstWriter struct {
	dir        string
	fileNumber uint64
	blockSize  int
	fd         *os.File
	tmpPath    string
	finalPath  string

	buf       []byte
	blockBuf  []byte
	index     []indexEntry
	count     int
	smallest  []byte
	largest   []byte
	lastKey   []byte
}

type indexEntry struct {
	key    []byte
	offset uint64
	length uint32
}

func sstPath(dir string, num uint64) string {
	return filepath.Join(dir, padUint64(num)+".sst")
}

func padUint64(n uint64) string {
	const w = 20
	var b [w]byte
	for i := w - 1; i >= 0; i-- {
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[:])
}

func newSSTWriter(sstDir string, fileNumber uint64) (*sstWriter, error) {
	if err := os.MkdirAll(sstDir, 0o755); err != nil {
		return nil, err
	}
	final := sstPath(sstDir, fileNumber)
	tmp := final + ".tmp"
	fd, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &sstWriter{
		dir:        sstDir,
		fileNumber: fileNumber,
		blockSize:  sstDefaultBlockSize,
		fd:         fd,
		tmpPath:    tmp,
		finalPath:  final,
		buf:        make([]byte, 0, 256),
		blockBuf:   make([]byte, 0, sstDefaultBlockSize),
	}, nil
}

func (w *sstWriter) Add(key []byte, ref ValueRef, seq uint64) error {
	if w.lastKey != nil && bytesCompare(key, w.lastKey) < 0 {
		return ErrCorruptEntry
	}
	if w.count == 0 {
		w.smallest = append([]byte(nil), key...)
	}
	w.largest = append([]byte(nil), key...)
	w.lastKey = append(w.lastKey[:0], key...)

	refBytes := encodeValueRef(w.buf[:0], ref)
	entryLen := 4 + len(key) + len(refBytes) + 8 + 1
	raw := make([]byte, entryLen)
	binary.LittleEndian.PutUint32(raw[0:4], uint32(len(key)))
	copy(raw[4:], key)
	off := 4 + len(key)
	copy(raw[off:], refBytes)
	off += len(refBytes)
	binary.LittleEndian.PutUint64(raw[off:off+8], seq)
	raw[off+8] = byte(ref.Kind)

	if len(w.blockBuf) > 0 && len(w.blockBuf)+len(raw) > w.blockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	if len(w.blockBuf) == 0 {
		pos, err := w.fd.Seek(0, os.SEEK_CUR)
		if err != nil {
			return err
		}
		w.index = append(w.index, indexEntry{
			key:    append([]byte(nil), key...), // first key; updated to last on flush
			offset: uint64(pos),
		})
	}
	w.blockBuf = append(w.blockBuf, raw...)
	w.count++
	return nil
}

func (w *sstWriter) flushBlock() error {
	if len(w.blockBuf) == 0 {
		return nil
	}
	n, err := w.fd.Write(w.blockBuf)
	if err != nil {
		return err
	}
	if len(w.index) > 0 {
		w.index[len(w.index)-1].length = uint32(n)
		// update separator to last key in block
		w.index[len(w.index)-1].key = append([]byte(nil), w.largest...)
	}
	w.blockBuf = w.blockBuf[:0]
	return nil
}

func (w *sstWriter) Finish() (sstFileMeta, error) {
	var meta sstFileMeta
	if err := w.flushBlock(); err != nil {
		return meta, err
	}
	indexOff, err := w.fd.Seek(0, os.SEEK_CUR)
	if err != nil {
		return meta, err
	}
	// write index
	for _, e := range w.index {
		ib := make([]byte, 4+len(e.key)+8+4)
		binary.LittleEndian.PutUint32(ib[0:4], uint32(len(e.key)))
		copy(ib[4:], e.key)
		o := 4 + len(e.key)
		binary.LittleEndian.PutUint64(ib[o:o+8], e.offset)
		binary.LittleEndian.PutUint32(ib[o+8:o+12], e.length)
		if _, err := w.fd.Write(ib); err != nil {
			return meta, err
		}
	}
	indexLen := uint32(0)
	cur, err := w.fd.Seek(0, os.SEEK_CUR)
	if err != nil {
		return meta, err
	}
	indexLen = uint32(cur - indexOff)

	footer := make([]byte, sstFooterSize)
	// filter unused
	binary.LittleEndian.PutUint64(footer[0:8], 0)
	binary.LittleEndian.PutUint32(footer[8:12], 0)
	binary.LittleEndian.PutUint64(footer[12:20], uint64(indexOff))
	binary.LittleEndian.PutUint32(footer[20:24], indexLen)
	binary.LittleEndian.PutUint64(footer[24:32], 0)
	binary.LittleEndian.PutUint32(footer[32:36], 0)
	binary.LittleEndian.PutUint32(footer[36:40], sstMagic)
	binary.LittleEndian.PutUint16(footer[40:42], sstFormatVersion)
	binary.LittleEndian.PutUint16(footer[42:44], 0)
	crc := crc32.ChecksumIEEE(footer[:44])
	binary.LittleEndian.PutUint32(footer[44:48], crc)
	if _, err := w.fd.Write(footer); err != nil {
		return meta, err
	}
	if err := w.fd.Sync(); err != nil {
		return meta, err
	}
	if err := w.fd.Close(); err != nil {
		return meta, err
	}
	w.fd = nil
	if err := os.Rename(w.tmpPath, w.finalPath); err != nil {
		return meta, err
	}
	fi, err := os.Stat(w.finalPath)
	if err != nil {
		return meta, err
	}
	meta = sstFileMeta{
		FileNumber: w.fileNumber,
		Size:       uint64(fi.Size()),
		Smallest:   append([]byte(nil), w.smallest...),
		Largest:    append([]byte(nil), w.largest...),
	}
	return meta, nil
}

func (w *sstWriter) Abandon() error {
	if w.fd != nil {
		_ = w.fd.Close()
		w.fd = nil
	}
	_ = os.Remove(w.tmpPath)
	return nil
}

type sstReader struct {
	path       string
	fileNumber uint64
	data       []byte
	index      []indexEntry
	smallest   []byte
	largest    []byte
}

func openSSTReader(sstDir string, fileNumber uint64) (*sstReader, error) {
	path := sstPath(sstDir, fileNumber)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < sstFooterSize {
		return nil, ErrCorruptEntry
	}
	footer := data[len(data)-sstFooterSize:]
	if binary.LittleEndian.Uint32(footer[36:40]) != sstMagic {
		return nil, ErrCorruptEntry
	}
	crc := binary.LittleEndian.Uint32(footer[44:48])
	if crc32.ChecksumIEEE(footer[:44]) != crc {
		return nil, ErrCorruptEntry
	}
	indexOff := binary.LittleEndian.Uint64(footer[12:20])
	indexLen := binary.LittleEndian.Uint32(footer[20:24])
	if indexOff+uint64(indexLen) > uint64(len(data)-sstFooterSize) {
		return nil, ErrCorruptEntry
	}
	r := &sstReader{path: path, fileNumber: fileNumber, data: data}
	idxBuf := data[indexOff : indexOff+uint64(indexLen)]
	for off := 0; off < len(idxBuf); {
		if off+4 > len(idxBuf) {
			return nil, ErrCorruptEntry
		}
		klen := int(binary.LittleEndian.Uint32(idxBuf[off : off+4]))
		off += 4
		if off+klen+12 > len(idxBuf) {
			return nil, ErrCorruptEntry
		}
		key := append([]byte(nil), idxBuf[off:off+klen]...)
		off += klen
		eoff := binary.LittleEndian.Uint64(idxBuf[off : off+8])
		elen := binary.LittleEndian.Uint32(idxBuf[off+8 : off+12])
		off += 12
		r.index = append(r.index, indexEntry{key: key, offset: eoff, length: elen})
	}
	if len(r.index) > 0 {
		r.smallest = r.index[0].key // approximate; refine by scanning first block if needed
		r.largest = r.index[len(r.index)-1].key
	}
	return r, nil
}

func (r *sstReader) Get(key []byte) (ref ValueRef, seq uint64, ok bool, err error) {
	if r == nil || len(r.index) == 0 {
		// fall back to full scan of data region
		return r.scanAllForKey(key)
	}
	// index key is the largest key in each block
	bi := 0
	for bi < len(r.index) && bytesCompare(r.index[bi].key, key) < 0 {
		bi++
	}
	if bi >= len(r.index) {
		return ValueRef{}, 0, false, nil
	}
	e := r.index[bi]
	blockEnd := e.offset + uint64(e.length)
	if blockEnd > uint64(len(r.data)) {
		return ValueRef{}, 0, false, ErrCorruptEntry
	}
	return scanBlockForKey(r.data[e.offset:blockEnd], key)
}

func (r *sstReader) scanAllForKey(key []byte) (ValueRef, uint64, bool, error) {
	var found bool
	var ref ValueRef
	var seq uint64
	err := r.Iterate(func(k []byte, rref ValueRef, s uint64) error {
		c := bytesCompare(k, key)
		if c == 0 {
			ref, seq, found = rref, s, true
			return errStopIterate
		}
		if c > 0 {
			return errStopIterate
		}
		return nil
	})
	if err != nil && err != errStopIterate {
		return ValueRef{}, 0, false, err
	}
	return ref, seq, found, nil
}

var errStopIterate = errSentinel("stop")

type errSentinel string

func (e errSentinel) Error() string { return string(e) }

func scanBlockForKey(block, key []byte) (ValueRef, uint64, bool, error) {
	for off := 0; off < len(block); {
		ent, n, err := decodeSSTEntry(block[off:])
		if err != nil {
			return ValueRef{}, 0, false, err
		}
		c := bytesCompare(ent.Key, key)
		if c == 0 {
			return ent.Ref, ent.Seq, true, nil
		}
		if c > 0 {
			return ValueRef{}, 0, false, nil
		}
		off += n
	}
	return ValueRef{}, 0, false, nil
}

func decodeSSTEntry(buf []byte) (sstEntry, int, error) {
	if len(buf) < 4 {
		return sstEntry{}, 0, ErrCorruptEntry
	}
	klen := int(binary.LittleEndian.Uint32(buf[0:4]))
	if klen <= 0 || len(buf) < 4+klen {
		return sstEntry{}, 0, ErrCorruptEntry
	}
	key := append([]byte(nil), buf[4:4+klen]...)
	ref, rn, err := decodeValueRef(buf[4+klen:])
	if err != nil {
		return sstEntry{}, 0, err
	}
	off := 4 + klen + rn
	if len(buf) < off+9 {
		return sstEntry{}, 0, ErrCorruptEntry
	}
	seq := binary.LittleEndian.Uint64(buf[off : off+8])
	// kind at off+8 redundant
	return sstEntry{Key: key, Ref: ref, Seq: seq}, off + 9, nil
}

func (r *sstReader) Iterate(fn func(key []byte, ref ValueRef, seq uint64) error) error {
	if r == nil {
		return nil
	}
	dataEnd := len(r.data) - sstFooterSize
	if len(r.index) > 0 {
		dataEnd = int(r.index[0].offset) // wait, data is before index
		// data region is [0, indexOff)
		footer := r.data[len(r.data)-sstFooterSize:]
		indexOff := int(binary.LittleEndian.Uint64(footer[12:20]))
		dataEnd = indexOff
	}
	for off := 0; off < dataEnd; {
		ent, n, err := decodeSSTEntry(r.data[off:dataEnd])
		if err != nil {
			return err
		}
		if err := fn(ent.Key, ent.Ref, ent.Seq); err != nil {
			return err
		}
		off += n
	}
	return nil
}

func (r *sstReader) Close() error { return nil }

func bytesCompare(a, b []byte) int {
	al, bl := len(a), len(b)
	for i := 0; i < al && i < bl; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	switch {
	case al < bl:
		return -1
	case al > bl:
		return 1
	default:
		return 0
	}
}
