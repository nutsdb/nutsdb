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
	"time"

	"github.com/nutsdb/nutsdb/internal/logger"
)

func (m *lsmStoreMgr) startBackground() {
	m.flushCh = make(chan struct{}, 1)
	m.compactCh = make(chan struct{}, 1)
	m.stopCh = make(chan struct{})
	m.wg.Add(2)
	go m.flushLoop()
	go m.compactLoop()
}

func (m *lsmStoreMgr) signalFlush() {
	select {
	case m.flushCh <- struct{}{}:
	default:
	}
}

func (m *lsmStoreMgr) signalCompact() {
	select {
	case m.compactCh <- struct{}{}:
	default:
	}
}

func (m *lsmStoreMgr) flushLoop() {
	defer m.wg.Done()
	for {
		select {
		case <-m.stopCh:
			return
		case <-m.flushCh:
			for {
				imm, fileNum, ok := m.takeFlushWork()
				if !ok {
					break
				}
				meta, maxSeq, err := writeImmToSST(m.sstDir, fileNum, imm)
				if err != nil {
					logger.Errorf("store: flush SST failed: %v", err)
					m.retryFlushLater()
					break
				}
				if err := m.applyFlush(imm, meta, maxSeq); err != nil {
					logger.Errorf("store: flush apply failed: %v", err)
					m.retryFlushLater()
					break
				}
				m.signalCompact()
			}
		}
	}
}

func (m *lsmStoreMgr) retryFlushLater() {
	time.AfterFunc(50*time.Millisecond, func() {
		select {
		case <-m.stopCh:
			return
		default:
			m.signalFlush()
		}
	})
}

func (m *lsmStoreMgr) compactLoop() {
	defer m.wg.Done()
	for {
		select {
		case <-m.stopCh:
			return
		case <-m.compactCh:
			for {
				job, ok := m.pickCompactJob()
				if !ok {
					break
				}
				if err := m.runCompactJob(job); err != nil {
					logger.Errorf("store: compaction failed: %v", err)
					time.AfterFunc(50*time.Millisecond, func() {
						select {
						case <-m.stopCh:
							return
						default:
							m.signalCompact()
						}
					})
					break
				}
			}
		}
	}
}

func (m *lsmStoreMgr) takeFlushWork() (imm *MemTable, fileNum uint64, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.imms) == 0 {
		return nil, 0, false
	}
	return m.imms[0], m.vs.NewFileNumber(), true
}

// writeImmToSST writes a frozen memtable to a new L0 SST. No store write-lock held.
func writeImmToSST(sstDir string, fileNum uint64, imm *MemTable) (sstFileMeta, uint64, error) {
	var meta sstFileMeta
	w, err := newSSTWriter(sstDir, fileNum)
	if err != nil {
		return meta, 0, err
	}
	var maxSeq uint64
	var writeErr error
	imm.Iterate(func(key []byte, v memValue) bool {
		if v.Seq > maxSeq {
			maxSeq = v.Seq
		}
		if err2 := w.Add(key, v.Ref, v.Seq); err2 != nil {
			writeErr = err2
			return false
		}
		return true
	})
	if writeErr != nil {
		_ = w.Abandon()
		return meta, 0, writeErr
	}
	meta, err = w.Finish()
	if err != nil {
		return meta, 0, err
	}
	return meta, maxSeq, nil
}

func (m *lsmStoreMgr) applyFlush(imm *MemTable, meta sstFileMeta, maxSeq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.imms) == 0 || m.imms[0] != imm {
		// Already applied or queue changed unexpectedly; leave SST orphaned for now.
		return nil
	}
	nextFile := meta.FileNumber + 1
	edit := &VersionEdit{
		Added: []FileMeta{{
			FileNumber: meta.FileNumber,
			Level:      0,
			Size:       meta.Size,
			Smallest:   meta.Smallest,
			Largest:    meta.Largest,
		}},
		LastSequence:   &maxSeq,
		NextFileNumber: &nextFile,
		LogNumber:      &maxSeq,
	}
	if err := m.vs.LogAndApply(edit); err != nil {
		return err
	}
	m.imms = m.imms[1:]
	// Do not RemoveAll(wal/): concurrent Put may still append. Replay skips seq<=LogNumber.
	return nil
}

// flushAllSync drains mem + imms synchronously. Must be called after background workers stop.
func (m *lsmStoreMgr) flushAllSync() error {
	m.mu.Lock()
	if m.mem != nil && m.mem.Size() > 0 {
		m.mem.Freeze()
		m.imms = append(m.imms, m.mem)
		m.mem = newMemTable()
	}
	m.mu.Unlock()

	for {
		imm, fileNum, ok := m.takeFlushWork()
		if !ok {
			return nil
		}
		meta, maxSeq, err := writeImmToSST(m.sstDir, fileNum, imm)
		if err != nil {
			return err
		}
		if err := m.applyFlush(imm, meta, maxSeq); err != nil {
			return err
		}
	}
}
