// Copyright 2022 The nutsdb Author. All rights reserved.
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

import "fmt"

type Iterator struct {
	tx *Tx

	current *Node
	i       int

	bucket string

	entry *Entry
}

func NewIterator(tx *Tx, bucket string) *Iterator {
	return &Iterator{
		tx:     tx,
		bucket: bucket,
	}
}

// SetNext would set the next Entry item, and would return (true, nil) if the next item is available
// Otherwise if the next item is not available it would return (false, nil)
// If it faces error it would return (false, err)
func (it *Iterator) SetNext() (bool, error) {
	if it.tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		return false, fmt.Errorf("%s mode is not supported in iterators", "HintBPTSparseIdxMode")
	}

	if err := it.tx.checkTxIsClosed(); err != nil {
		return false, err
	}

	if it.i == -1 {
		return false, nil
	}

	if it.current == nil && (it.tx.db.opt.EntryIdxMode == HintKeyAndRAMIdxMode ||
		it.tx.db.opt.EntryIdxMode == HintKeyValAndRAMIdxMode) {
		if index, ok := it.tx.db.BPTreeIdx[it.bucket]; ok {
			err := it.Seek(index.FirstKey)
			if err != nil {
				return false, err
			}
		}
	}

	if it.i >= it.current.KeysNum {
		it.current, _ = it.current.pointers[order-1].(*Node)
		if it.current == nil {
			return false, nil
		}
		it.i = 0
	}

	pointer := it.current.pointers[it.i]
	record := pointer.(*Record)
	it.i++

	if record.H.Meta.Flag == DataDeleteFlag || record.IsExpired() {
		return it.SetNext()
	}

	if it.tx.db.opt.EntryIdxMode == HintKeyAndRAMIdxMode {
		path := it.tx.db.getDataPath(record.H.FileID)
		df, err := it.tx.db.fm.getDataFile(path, it.tx.db.opt.SegmentSize)
		if err != nil {
			return false, err
		}

		if item, err := df.ReadAt(int(record.H.DataPos)); err == nil {
			err = df.rwManager.Release()
			if err != nil {
				return false, err
			}

			it.entry = item
			return true, nil
		} else {
			releaseErr := df.rwManager.Release()
			if releaseErr != nil {
				return false, releaseErr
			}
			return false, fmt.Errorf("HintIdx r.Hi.dataPos %d, err %s", record.H.DataPos, err)
		}
	}

	if it.tx.db.opt.EntryIdxMode == HintKeyValAndRAMIdxMode {
		it.entry = record.E
		return true, nil
	}

	return false, nil
}

// Seek would seek to the key,
// If the key is not available it would seek to the first smallest greater key than the input key.
func (it *Iterator) Seek(key []byte) error {
	if it.tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		return fmt.Errorf("%s mode is not supported in iterators", "HintBPTSparseIdxMode")
	}

	it.current = it.tx.db.BPTreeIdx[it.bucket].FindLeaf(key)
	if it.current == nil {
		it.i = -1
	}

	for it.i = 0; it.i < it.current.KeysNum && compare(it.current.Keys[it.i], key) < 0; {
		it.i++
	}

	return nil
}

// Entry would return the current Entry item after calling SetNext
func (it *Iterator) Entry() *Entry {
	return it.entry
}
