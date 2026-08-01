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
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	manifestRecordHeader = 9 // crc4 + len4 + type1
	manifestTypeEdit     = 1
)

type FileMeta struct {
	FileNumber uint64
	Level      int
	Size       uint64
	Smallest   []byte
	Largest    []byte
}

type Version struct {
	Files          [][]FileMeta // by level
	LastSequence   uint64
	NextFileNumber uint64
	LogNumber      uint64
}

func (v *Version) clone() *Version {
	out := &Version{
		LastSequence:   v.LastSequence,
		NextFileNumber: v.NextFileNumber,
		LogNumber:      v.LogNumber,
		Files:          make([][]FileMeta, len(v.Files)),
	}
	for i := range v.Files {
		out.Files[i] = append([]FileMeta(nil), v.Files[i]...)
		for j := range out.Files[i] {
			out.Files[i][j].Smallest = append([]byte(nil), v.Files[i][j].Smallest...)
			out.Files[i][j].Largest = append([]byte(nil), v.Files[i][j].Largest...)
		}
	}
	return out
}

type VersionEdit struct {
	Added   []FileMeta
	Deleted []struct {
		Level      int
		FileNumber uint64
	}
	LastSequence   *uint64
	NextFileNumber *uint64
	LogNumber      *uint64
}

type VersionSet struct {
	mu       sync.Mutex
	dir      string
	sstDir   string
	current  *Version
	manifest *os.File
	manPath  string
	manSeq   uint64
}

func recoverVersionSet(dir, sstDir string) (*VersionSet, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(sstDir, 0o755); err != nil {
		return nil, err
	}
	vs := &VersionSet{
		dir:    dir,
		sstDir: sstDir,
		current: &Version{
			Files:          make([][]FileMeta, 7),
			NextFileNumber: 1,
		},
	}
	curName, err := readCURRENT(dir)
	if err != nil {
		if os.IsNotExist(err) {
			// bootstrap empty manifest
			return vs, vs.createNewManifest()
		}
		return nil, err
	}
	vs.manPath = filepath.Join(dir, curName)
	vs.manSeq = parseManifestSeq(curName)
	data, err := os.ReadFile(vs.manPath)
	if err != nil {
		return nil, err
	}
	off := 0
	for off < len(data) {
		if off+manifestRecordHeader > len(data) {
			break // truncated tail
		}
		crc := binary.LittleEndian.Uint32(data[off : off+4])
		plen := int(binary.LittleEndian.Uint32(data[off+4 : off+8]))
		typ := data[off+8]
		off += manifestRecordHeader
		if plen < 0 || off+plen > len(data) {
			break
		}
		payload := data[off : off+plen]
		off += plen
		if crc32.ChecksumIEEE(append([]byte{typ}, payload...)) != crc {
			break
		}
		if typ != manifestTypeEdit {
			continue
		}
		edit, err := decodeVersionEdit(payload)
		if err != nil {
			break
		}
		vs.applyEditLocked(edit)
	}
	fd, err := os.OpenFile(vs.manPath, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := fd.Seek(0, io.SeekEnd); err != nil {
		_ = fd.Close()
		return nil, err
	}
	vs.manifest = fd
	return vs, nil
}

func (vs *VersionSet) createNewManifest() error {
	vs.manSeq++
	name := formatManifestName(vs.manSeq)
	path := filepath.Join(vs.dir, name)
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	vs.manifest = fd
	vs.manPath = path
	return writeCURRENT(vs.dir, name)
}

func (vs *VersionSet) Current() *Version {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return vs.current.clone()
}

func (vs *VersionSet) NewFileNumber() uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	n := vs.current.NextFileNumber
	vs.current.NextFileNumber++
	return n
}

func (vs *VersionSet) LogAndApply(edit *VersionEdit) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	next := vs.current.clone()
	applyEditToVersion(next, edit)
	payload := encodeVersionEdit(edit)
	rec := make([]byte, manifestRecordHeader+len(payload))
	rec[8] = manifestTypeEdit
	binary.LittleEndian.PutUint32(rec[4:8], uint32(len(payload)))
	copy(rec[9:], payload)
	crc := crc32.ChecksumIEEE(append([]byte{manifestTypeEdit}, payload...))
	binary.LittleEndian.PutUint32(rec[0:4], crc)
	if _, err := vs.manifest.Write(rec); err != nil {
		return err
	}
	if err := vs.manifest.Sync(); err != nil {
		return err
	}
	vs.current = next
	return nil
}

func (vs *VersionSet) applyEditLocked(edit *VersionEdit) {
	applyEditToVersion(vs.current, edit)
}

func applyEditToVersion(v *Version, edit *VersionEdit) {
	if edit.LastSequence != nil {
		v.LastSequence = *edit.LastSequence
	}
	if edit.NextFileNumber != nil {
		v.NextFileNumber = *edit.NextFileNumber
	}
	if edit.LogNumber != nil {
		v.LogNumber = *edit.LogNumber
	}
	del := map[uint64]struct{}{}
	for _, d := range edit.Deleted {
		del[d.FileNumber] = struct{}{}
	}
	for level := range v.Files {
		filtered := v.Files[level][:0]
		for _, f := range v.Files[level] {
			if _, ok := del[f.FileNumber]; !ok {
				filtered = append(filtered, f)
			}
		}
		v.Files[level] = filtered
	}
	for _, f := range edit.Added {
		for len(v.Files) <= f.Level {
			v.Files = append(v.Files, nil)
		}
		v.Files[f.Level] = append(v.Files[f.Level], f)
		if f.Level > 0 {
			sort.Slice(v.Files[f.Level], func(i, j int) bool {
				return bytesCompare(v.Files[f.Level][i].Smallest, v.Files[f.Level][j].Smallest) < 0
			})
		}
	}
	if v.NextFileNumber == 0 {
		v.NextFileNumber = 1
	}
}

func (vs *VersionSet) Close() error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if vs.manifest != nil {
		err := vs.manifest.Close()
		vs.manifest = nil
		return err
	}
	return nil
}

func encodeVersionEdit(edit *VersionEdit) []byte {
	// layout:
	// flags(1): bit0 lastSeq, bit1 nextFile, bit2 logNum
	// optional u64s
	// deleted_count u32 + (level u32, file u64)*
	// added_count u32 + FileMeta*
	var flags byte
	buf := []byte{0} // placeholder flags
	if edit.LastSequence != nil {
		flags |= 1
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], *edit.LastSequence)
		buf = append(buf, b[:]...)
	}
	if edit.NextFileNumber != nil {
		flags |= 2
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], *edit.NextFileNumber)
		buf = append(buf, b[:]...)
	}
	if edit.LogNumber != nil {
		flags |= 4
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], *edit.LogNumber)
		buf = append(buf, b[:]...)
	}
	buf[0] = flags
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], uint32(len(edit.Deleted)))
	buf = append(buf, tmp[:]...)
	for _, d := range edit.Deleted {
		binary.LittleEndian.PutUint32(tmp[:], uint32(d.Level))
		buf = append(buf, tmp[:]...)
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], d.FileNumber)
		buf = append(buf, b[:]...)
	}
	binary.LittleEndian.PutUint32(tmp[:], uint32(len(edit.Added)))
	buf = append(buf, tmp[:]...)
	for _, f := range edit.Added {
		buf = append(buf, encodeFileMeta(f)...)
	}
	return buf
}

func decodeVersionEdit(payload []byte) (*VersionEdit, error) {
	if len(payload) < 1 {
		return nil, ErrCorruptEntry
	}
	edit := &VersionEdit{}
	flags := payload[0]
	off := 1
	if flags&1 != 0 {
		if len(payload) < off+8 {
			return nil, ErrCorruptEntry
		}
		v := binary.LittleEndian.Uint64(payload[off : off+8])
		edit.LastSequence = &v
		off += 8
	}
	if flags&2 != 0 {
		if len(payload) < off+8 {
			return nil, ErrCorruptEntry
		}
		v := binary.LittleEndian.Uint64(payload[off : off+8])
		edit.NextFileNumber = &v
		off += 8
	}
	if flags&4 != 0 {
		if len(payload) < off+8 {
			return nil, ErrCorruptEntry
		}
		v := binary.LittleEndian.Uint64(payload[off : off+8])
		edit.LogNumber = &v
		off += 8
	}
	if len(payload) < off+4 {
		return nil, ErrCorruptEntry
	}
	ndel := int(binary.LittleEndian.Uint32(payload[off : off+4]))
	off += 4
	for i := 0; i < ndel; i++ {
		if len(payload) < off+12 {
			return nil, ErrCorruptEntry
		}
		level := int(binary.LittleEndian.Uint32(payload[off : off+4]))
		num := binary.LittleEndian.Uint64(payload[off+4 : off+12])
		off += 12
		edit.Deleted = append(edit.Deleted, struct {
			Level      int
			FileNumber uint64
		}{level, num})
	}
	if len(payload) < off+4 {
		return nil, ErrCorruptEntry
	}
	nadd := int(binary.LittleEndian.Uint32(payload[off : off+4]))
	off += 4
	for i := 0; i < nadd; i++ {
		f, n, err := decodeFileMeta(payload[off:])
		if err != nil {
			return nil, err
		}
		edit.Added = append(edit.Added, f)
		off += n
	}
	return edit, nil
}

func encodeFileMeta(f FileMeta) []byte {
	buf := make([]byte, 4+8+8+4+len(f.Smallest)+4+len(f.Largest))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(f.Level))
	binary.LittleEndian.PutUint64(buf[4:12], f.FileNumber)
	binary.LittleEndian.PutUint64(buf[12:20], f.Size)
	binary.LittleEndian.PutUint32(buf[20:24], uint32(len(f.Smallest)))
	copy(buf[24:], f.Smallest)
	o := 24 + len(f.Smallest)
	binary.LittleEndian.PutUint32(buf[o:o+4], uint32(len(f.Largest)))
	copy(buf[o+4:], f.Largest)
	return buf
}

func decodeFileMeta(buf []byte) (FileMeta, int, error) {
	if len(buf) < 24 {
		return FileMeta{}, 0, ErrCorruptEntry
	}
	f := FileMeta{
		Level:      int(binary.LittleEndian.Uint32(buf[0:4])),
		FileNumber: binary.LittleEndian.Uint64(buf[4:12]),
		Size:       binary.LittleEndian.Uint64(buf[12:20]),
	}
	sl := int(binary.LittleEndian.Uint32(buf[20:24]))
	off := 24
	if sl < 0 || len(buf) < off+sl+4 {
		return FileMeta{}, 0, ErrCorruptEntry
	}
	f.Smallest = append([]byte(nil), buf[off:off+sl]...)
	off += sl
	ll := int(binary.LittleEndian.Uint32(buf[off : off+4]))
	off += 4
	if ll < 0 || len(buf) < off+ll {
		return FileMeta{}, 0, ErrCorruptEntry
	}
	f.Largest = append([]byte(nil), buf[off:off+ll]...)
	off += ll
	return f, off, nil
}

func readCURRENT(dir string) (string, error) {
	b, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

func writeCURRENT(dir, name string) error {
	tmp := filepath.Join(dir, "CURRENT.tmp")
	if err := os.WriteFile(tmp, []byte(name+"\n"), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, "CURRENT"))
}

func formatManifestName(seq uint64) string {
	return "MANIFEST-" + padUint64(seq)
}

func parseManifestSeq(name string) uint64 {
	if !strings.HasPrefix(name, "MANIFEST-") {
		return 0
	}
	n, _ := strconv.ParseUint(strings.TrimPrefix(name, "MANIFEST-"), 10, 64)
	return n
}
