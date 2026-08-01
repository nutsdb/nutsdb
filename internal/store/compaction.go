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

import "os"

const maxLevel = 6

func (m *lsmStoreMgr) maybeCompactLocked() error {
	for {
		ver := m.vs.Current()
		if len(ver.Files) == 0 || len(ver.Files[0]) < m.opts.L0FileNumCompactionTrigger {
			break
		}
		if err := m.compactLevelLocked(0); err != nil {
			return err
		}
	}
	// push oversized lower levels
	for level := 1; level < maxLevel; level++ {
		for {
			ver := m.vs.Current()
			if level >= len(ver.Files) {
				break
			}
			var size uint64
			for _, f := range ver.Files[level] {
				size += f.Size
			}
			limit := uint64(m.opts.LevelBaseSize)
			for i := 1; i < level; i++ {
				limit *= uint64(m.opts.LevelSizeMultiplier)
			}
			if size <= limit {
				break
			}
			if err := m.compactLevelLocked(level); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *lsmStoreMgr) compactLevelLocked(level int) error {
	ver := m.vs.Current()
	if level >= len(ver.Files) || len(ver.Files[level]) == 0 {
		return nil
	}
	nextLevel := level + 1
	var inputs []FileMeta
	if level == 0 {
		inputs = append(inputs, ver.Files[0]...)
	} else {
		// pick first file at this level
		inputs = append(inputs, ver.Files[level][0])
	}
	// range covering inputs
	smallest, largest := inputs[0].Smallest, inputs[0].Largest
	for _, f := range inputs[1:] {
		if bytesCompare(f.Smallest, smallest) < 0 {
			smallest = f.Smallest
		}
		if bytesCompare(f.Largest, largest) > 0 {
			largest = f.Largest
		}
	}
	// overlapping files in next level
	var overlap []FileMeta
	if nextLevel < len(ver.Files) {
		for _, f := range ver.Files[nextLevel] {
			if bytesCompare(f.Largest, smallest) < 0 || bytesCompare(f.Smallest, largest) > 0 {
				continue
			}
			overlap = append(overlap, f)
		}
	}
	all := append(append([]FileMeta{}, inputs...), overlap...)

	type kv struct {
		key []byte
		ref ValueRef
		seq uint64
	}
	best := map[string]kv{}
	for _, fm := range all {
		rd, err := m.getReaderLocked(fm.FileNumber)
		if err != nil {
			return err
		}
		err = rd.Iterate(func(key []byte, ref ValueRef, seq uint64) error {
			s := string(key)
			if old, ok := best[s]; ok && old.seq >= seq {
				return nil
			}
			best[s] = kv{key: append([]byte(nil), key...), ref: cloneValueRef(ref), seq: seq}
			return nil
		})
		if err != nil {
			return err
		}
	}
	type pair struct {
		v kv
	}
	pairs := make([]pair, 0, len(best))
	for _, v := range best {
		pairs = append(pairs, pair{v})
	}
	for i := 0; i < len(pairs); i++ {
		for j := i + 1; j < len(pairs); j++ {
			if bytesCompare(pairs[j].v.key, pairs[i].v.key) < 0 {
				pairs[i], pairs[j] = pairs[j], pairs[i]
			}
		}
	}

	fileNum := m.vs.NewFileNumber()
	w, err := newSSTWriter(m.sstDir, fileNum)
	if err != nil {
		return err
	}
	var outMeta sstFileMeta
	hasOut := false
	for _, p := range pairs {
		if p.v.ref.Kind == ValueKindTombstone && nextLevel == maxLevel {
			continue // drop tombstone at bottom
		}
		if err := w.Add(p.v.key, p.v.ref, p.v.seq); err != nil {
			_ = w.Abandon()
			return err
		}
		hasOut = true
	}
	edit := &VersionEdit{}
	for _, f := range all {
		edit.Deleted = append(edit.Deleted, struct {
			Level      int
			FileNumber uint64
		}{f.Level, f.FileNumber})
	}
	if hasOut {
		outMeta, err = w.Finish()
		if err != nil {
			return err
		}
		edit.Added = append(edit.Added, FileMeta{
			FileNumber: outMeta.FileNumber,
			Level:      nextLevel,
			Size:       outMeta.Size,
			Smallest:   outMeta.Smallest,
			Largest:    outMeta.Largest,
		})
	} else {
		_ = w.Abandon()
	}
	next := fileNum + 1
	edit.NextFileNumber = &next
	if err := m.vs.LogAndApply(edit); err != nil {
		return err
	}
	// drop readers and delete old files
	for _, f := range all {
		delete(m.readers, f.FileNumber)
		_ = os.Remove(sstPath(m.sstDir, f.FileNumber))
	}
	return nil
}
