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
	"os"
	"sort"
)

const maxLevel = 6

type compactJob struct {
	level     int
	nextLevel int
	inputs    []FileMeta
	overlap   []FileMeta
	all       []FileMeta
	fileNum   uint64
}

func (m *lsmStoreMgr) pickCompactJob() (compactJob, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ver := m.vs.Current()
	if len(ver.Files) > 0 && len(ver.Files[0]) >= m.opts.L0FileNumCompactionTrigger {
		return m.buildCompactJobLocked(0, ver)
	}
	for level := 1; level < maxLevel; level++ {
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
		if size > limit {
			return m.buildCompactJobLocked(level, ver)
		}
	}
	return compactJob{}, false
}

func (m *lsmStoreMgr) buildCompactJobLocked(level int, ver *Version) (compactJob, bool) {
	if level >= len(ver.Files) || len(ver.Files[level]) == 0 {
		return compactJob{}, false
	}
	nextLevel := level + 1
	var inputs []FileMeta
	if level == 0 {
		inputs = append(inputs, ver.Files[0]...)
	} else {
		inputs = append(inputs, ver.Files[level][0])
	}
	smallest, largest := inputs[0].Smallest, inputs[0].Largest
	for _, f := range inputs[1:] {
		if bytes.Compare(f.Smallest, smallest) < 0 {
			smallest = f.Smallest
		}
		if bytes.Compare(f.Largest, largest) > 0 {
			largest = f.Largest
		}
	}
	var overlap []FileMeta
	if nextLevel < len(ver.Files) {
		for _, f := range ver.Files[nextLevel] {
			if bytes.Compare(f.Largest, smallest) < 0 || bytes.Compare(f.Smallest, largest) > 0 {
				continue
			}
			overlap = append(overlap, f)
		}
	}
	all := append(append([]FileMeta{}, inputs...), overlap...)
	fileNum := m.vs.NewFileNumber()
	return compactJob{
		level:     level,
		nextLevel: nextLevel,
		inputs:    inputs,
		overlap:   overlap,
		all:       all,
		fileNum:   fileNum,
	}, true
}

func (m *lsmStoreMgr) runCompactJob(job compactJob) error {
	type kv struct {
		key []byte
		ref ValueRef
		seq uint64
	}
	best := map[string]kv{}
	for _, fm := range job.all {
		rd, err := m.getReader(fm.FileNumber)
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
	pairs := make([]kv, 0, len(best))
	for _, v := range best {
		pairs = append(pairs, v)
	}
	sort.Slice(pairs, func(i, j int) bool {
		return bytes.Compare(pairs[i].key, pairs[j].key) < 0
	})

	w, err := newSSTWriter(m.sstDir, job.fileNum)
	if err != nil {
		return err
	}
	hasOut := false
	for _, p := range pairs {
		if p.ref.Kind == ValueKindTombstone && job.nextLevel == maxLevel {
			continue
		}
		if err := w.Add(p.key, p.ref, p.seq); err != nil {
			_ = w.Abandon()
			return err
		}
		hasOut = true
	}

	edit := &VersionEdit{}
	for _, f := range job.all {
		edit.Deleted = append(edit.Deleted, struct {
			Level      int
			FileNumber uint64
		}{f.Level, f.FileNumber})
	}
	var outMeta sstFileMeta
	if hasOut {
		outMeta, err = w.Finish()
		if err != nil {
			return err
		}
		edit.Added = append(edit.Added, FileMeta{
			FileNumber: outMeta.FileNumber,
			Level:      job.nextLevel,
			Size:       outMeta.Size,
			Smallest:   outMeta.Smallest,
			Largest:    outMeta.Largest,
		})
	} else {
		_ = w.Abandon()
	}
	next := job.fileNum + 1
	edit.NextFileNumber = &next

	m.mu.Lock()
	err = m.vs.LogAndApply(edit)
	if err == nil {
		m.readersMu.Lock()
		for _, f := range job.all {
			delete(m.readers, f.FileNumber)
		}
		m.readersMu.Unlock()
	}
	m.mu.Unlock()
	if err != nil {
		return err
	}
	for _, f := range job.all {
		_ = os.Remove(sstPath(m.sstDir, f.FileNumber))
	}
	return nil
}
