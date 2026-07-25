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
	"bytes"
	"errors"
	"os"
	"sort"

	"github.com/nutsdb/nutsdb/internal/utils"
)

// HintManager manages per-FileID .hint files under a data directory.
type HintManager interface {
	// CreateWriter creates (or truncates) a temporary writer for <fileID>.hint.
	// Call Finish to publish atomically via rename; Close without Finish discards the temp file.
	CreateWriter(fileID uint32) (HintWriter, error)

	// OpenReader opens a complete hint file for fileID.
	// Returns ErrHintNotFound, ErrHintIncomplete, or ErrHintCorrupt when the file is missing or invalid.
	OpenReader(fileID uint32) (HintReader, error)

	// BuildFromSegment builds a complete hint from a sealed segment by iterating Store records
	// in offset order and appending one HintEntry per KV record using decodeKey.
	// Meta records may be skipped via ErrHintSkipMeta. On success it verifies key consistency
	// with the segment; on verify failure the published hint is removed.
	BuildFromSegment(store Store, fileID uint32, decodeKey DecodeKeyFunc) error

	// VerifyHintMatchesSegment checks that hint keys, Locations, and RecordTypes match the
	// segment's KV sequence exactly (same order and byte-identical keys).
	VerifyHintMatchesSegment(store Store, fileID uint32, decodeKey DecodeKeyFunc) error

	// Delete removes <fileID>.hint (and any leftover .tmp). Returns ErrHintNotFound if absent.
	Delete(fileID uint32) error

	// LoadAll loads all complete hints in ascending FileID order and invokes apply for each entry.
	// Incomplete or corrupt hints are skipped with a log line; apply errors abort loading.
	LoadAll(apply func(HintEntry) error) error
}

type hintManager struct {
	dir     string
	bufSize int
}

// NewHintManager creates a HintManager rooted at dir (usually Options.Dir).
func NewHintManager(dir string) HintManager {
	return &hintManager{dir: dir, bufSize: DefaultHintBufferSize}
}

func (m *hintManager) CreateWriter(fileID uint32) (HintWriter, error) {
	if err := os.MkdirAll(m.dir, 0o755); err != nil {
		return nil, err
	}
	return newHintWriter(m.dir, fileID, m.bufSize)
}

func (m *hintManager) OpenReader(fileID uint32) (HintReader, error) {
	return openHintReader(hintPath(m.dir, fileID), fileID)
}

func (m *hintManager) BuildFromSegment(store Store, fileID uint32, decodeKey DecodeKeyFunc) error {
	if store == nil || decodeKey == nil {
		return ErrInvalidOptions
	}
	w, err := m.CreateWriter(fileID)
	if err != nil {
		return err
	}
	defer func() { _ = w.Close() }()

	err = store.Iterate(fileID, func(loc Location, typ RecordType, payload []byte) error {
		key, kerr := decodeKey(payload, typ)
		if kerr != nil {
			if errors.Is(kerr, ErrHintSkipMeta) {
				return nil
			}
			return kerr
		}
		if len(key) == 0 {
			return ErrHintInvalidEntry
		}
		return w.Append(HintEntry{
			Key:  append([]byte(nil), key...),
			Loc:  loc,
			Type: typ,
		})
	})
	if err != nil {
		return err
	}
	if err := w.Finish(); err != nil {
		return err
	}
	if err := m.VerifyHintMatchesSegment(store, fileID, decodeKey); err != nil {
		_ = m.Delete(fileID)
		utils.GetLogger().Printf("fileio: hint verify failed id=%d err=%v; hint removed", fileID, err)
		return err
	}
	return nil
}

func (m *hintManager) VerifyHintMatchesSegment(store Store, fileID uint32, decodeKey DecodeKeyFunc) error {
	if store == nil || decodeKey == nil {
		return ErrInvalidOptions
	}
	rd, err := m.OpenReader(fileID)
	if err != nil {
		return err
	}
	defer func() { _ = rd.Close() }()

	type segItem struct {
		key  []byte
		loc  Location
		typ  RecordType
	}
	var segs []segItem
	err = store.Iterate(fileID, func(loc Location, typ RecordType, payload []byte) error {
		key, kerr := decodeKey(payload, typ)
		if kerr != nil {
			if errors.Is(kerr, ErrHintSkipMeta) {
				return nil
			}
			return kerr
		}
		if len(key) == 0 {
			return ErrHintInvalidEntry
		}
		segs = append(segs, segItem{
			key: append([]byte(nil), key...),
			loc: loc,
			typ: typ,
		})
		return nil
	})
	if err != nil {
		return err
	}

	var i int
	err = rd.Iterate(func(entry HintEntry) error {
		if i >= len(segs) {
			return ErrHintSegKeyMismatch
		}
		s := segs[i]
		i++
		if !bytes.Equal(entry.Key, s.key) ||
			entry.Loc != s.loc ||
			entry.Type != s.typ {
			return ErrHintSegKeyMismatch
		}
		return nil
	})
	if err != nil {
		return err
	}
	if i != len(segs) {
		return ErrHintSegKeyMismatch
	}
	return nil
}

func (m *hintManager) Delete(fileID uint32) error {
	path := hintPath(m.dir, fileID)
	tmp := hintTmpPath(m.dir, fileID)
	_ = os.Remove(tmp)
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return ErrHintNotFound
		}
		return err
	}
	utils.GetLogger().Printf("fileio: hint deleted id=%d path=%s", fileID, path)
	return nil
}

func (m *hintManager) LoadAll(apply func(HintEntry) error) error {
	if apply == nil {
		return ErrInvalidOptions
	}
	ids, err := listHintIDs(m.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		rd, err := m.OpenReader(id)
		if err != nil {
			utils.GetLogger().Printf("fileio: hint load skip id=%d err=%v", id, err)
			continue
		}
		err = rd.Iterate(apply)
		_ = rd.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
