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

package nutsdb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	bucketsMetaFileName = "buckets.meta"
	bucketsMetaMagic    = "NBKT"
	bucketsMetaVersion  = uint32(1)
	bucketsMetaFilePerm = 0o644
)

type bucketKey struct {
	ds   uint16
	name string
}

type bucketMetaEntry struct {
	DS   uint16
	ID   uint64
	Name string
}

// bucketManager holds in-memory bucket metadata and persists to buckets.meta.
// bucketManager 持有内存中的 bucket 元数据，并持久化到 buckets.meta。
type bucketManager struct {
	mu     sync.RWMutex
	path   string
	nextID uint64
	byKey  map[bucketKey]uint64
}

func newBucketManager(dir string) *bucketManager {
	return &bucketManager{
		path:   filepath.Join(dir, bucketsMetaFileName),
		nextID: 1,
		byKey:  make(map[bucketKey]uint64),
	}
}

func (m *bucketManager) load() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := os.ReadFile(m.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read buckets meta: %w", err)
	}
	entries, nextID, err := decodeBucketsMeta(data)
	if err != nil {
		return err
	}
	m.byKey = make(map[bucketKey]uint64, len(entries))
	for _, e := range entries {
		m.byKey[bucketKey{ds: e.DS, name: e.Name}] = e.ID
	}
	m.nextID = nextID
	if m.nextID == 0 {
		m.nextID = 1
	}
	return nil
}

func (m *bucketManager) saveLocked() error {
	entries := make([]bucketMetaEntry, 0, len(m.byKey))
	for k, id := range m.byKey {
		entries = append(entries, bucketMetaEntry{DS: k.ds, ID: id, Name: k.name})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].DS != entries[j].DS {
			return entries[i].DS < entries[j].DS
		}
		return entries[i].Name < entries[j].Name
	})
	data := encodeBucketsMeta(entries, m.nextID)

	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, data, bucketsMetaFilePerm); err != nil {
		return fmt.Errorf("write buckets meta tmp: %w", err)
	}
	if err := os.Rename(tmp, m.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename buckets meta: %w", err)
	}
	return nil
}

func (m *bucketManager) getID(ds uint16, name string) (uint64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	id, ok := m.byKey[bucketKey{ds: ds, name: name}]
	return id, ok
}

func (m *bucketManager) exists(ds uint16, name string) bool {
	_, ok := m.getID(ds, name)
	return ok
}

func (m *bucketManager) list(ds uint16) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0)
	for k := range m.byKey {
		if k.ds == ds {
			names = append(names, k.name)
		}
	}
	sort.Strings(names)
	return names
}

// allocID reserves a new bucket id (may leave gaps on rollback).
// allocID 预留新的 bucket id（回滚时可能产生空洞）。
func (m *bucketManager) allocID() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.nextID
	m.nextID++
	return id
}

func (m *bucketManager) applyCommit(creates map[bucketKey]uint64, deletes map[bucketKey]struct{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range deletes {
		delete(m.byKey, k)
	}
	for k, id := range creates {
		m.byKey[k] = id
	}
	return m.saveLocked()
}

func encodeBucketsMeta(entries []bucketMetaEntry, nextID uint64) []byte {
	// magic(4) + ver(4) + nextID(8) + count(4) + entries
	size := 4 + 4 + 8 + 4
	for _, e := range entries {
		size += 2 + 8 + 2 + len(e.Name)
	}
	buf := make([]byte, size)
	copy(buf[0:4], bucketsMetaMagic)
	binary.LittleEndian.PutUint32(buf[4:8], bucketsMetaVersion)
	binary.LittleEndian.PutUint64(buf[8:16], nextID)
	binary.LittleEndian.PutUint32(buf[16:20], uint32(len(entries)))
	off := 20
	for _, e := range entries {
		binary.LittleEndian.PutUint16(buf[off:off+2], e.DS)
		off += 2
		binary.LittleEndian.PutUint64(buf[off:off+8], e.ID)
		off += 8
		name := []byte(e.Name)
		binary.LittleEndian.PutUint16(buf[off:off+2], uint16(len(name)))
		off += 2
		copy(buf[off:], name)
		off += len(name)
	}
	return buf
}

func decodeBucketsMeta(data []byte) ([]bucketMetaEntry, uint64, error) {
	if len(data) < 20 {
		return nil, 0, fmt.Errorf("buckets meta: too short")
	}
	if string(data[0:4]) != bucketsMetaMagic {
		return nil, 0, fmt.Errorf("buckets meta: bad magic")
	}
	ver := binary.LittleEndian.Uint32(data[4:8])
	if ver != bucketsMetaVersion {
		return nil, 0, fmt.Errorf("buckets meta: unsupported version %d", ver)
	}
	nextID := binary.LittleEndian.Uint64(data[8:16])
	count := binary.LittleEndian.Uint32(data[16:20])
	off := 20
	entries := make([]bucketMetaEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		if off+12 > len(data) {
			return nil, 0, fmt.Errorf("buckets meta: truncated entry header")
		}
		ds := binary.LittleEndian.Uint16(data[off : off+2])
		off += 2
		id := binary.LittleEndian.Uint64(data[off : off+8])
		off += 8
		nameLen := int(binary.LittleEndian.Uint16(data[off : off+2]))
		off += 2
		if off+nameLen > len(data) {
			return nil, 0, fmt.Errorf("buckets meta: truncated name")
		}
		name := string(data[off : off+nameLen])
		off += nameLen
		entries = append(entries, bucketMetaEntry{DS: ds, ID: id, Name: name})
	}
	return entries, nextID, nil
}
