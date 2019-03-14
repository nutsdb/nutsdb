// Copyright 2019 The nutsdb Authors. All rights reserved.
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
	"errors"
	"fmt"
	"time"
)

// Get retrieves the value for a key in the bucket.
// The returned value is only valid for the life of the transaction.
func (tx *Tx) Get(bucket string, key []byte) (e *Entry, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	idxMode := tx.db.opt.EntryIdxMode
	if idxMode == HintKeyValAndRAMIdxMode || idxMode == HintKeyAndRAMIdxMode {
		if idx, ok := tx.db.BPTreeIdx[bucket]; ok {
			r, err := idx.Find(key)
			if err != nil {
				return nil, err
			}

			if _, ok := tx.db.committedTxIds[r.H.meta.txID]; !ok {
				return nil, ErrNotFoundKey
			}

			if r.H.meta.Flag == DataDeleteFlag || r.IsExpired() {
				return nil, ErrNotFoundKey
			}

			if idxMode == HintKeyValAndRAMIdxMode {
				return r.E, nil
			}

			if idxMode == HintKeyAndRAMIdxMode {
				path := tx.db.getDataPath(r.H.fileID)
				df, err := NewDataFile(path, tx.db.opt.SegmentSize, tx.db.opt.RWMode)
				defer df.rwManager.Close()
				if err != nil {
					return nil, err
				}

				item, err := df.ReadAt(int(r.H.dataPos))
				if err != nil {
					return nil, fmt.Errorf("read err. pos %d, key %s, err %s", r.H.dataPos, string(key), err)
				}

				return item, nil
			}
		}
	}

	return nil, errors.New("not found bucket:" + bucket + ",key:" + string(key))
}

// RangeScan query a range at given bucket, start and end slice.
func (tx *Tx) RangeScan(bucket string, start, end []byte) (entries Entries, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	entries = make(Entries)

	if index, ok := tx.db.BPTreeIdx[bucket]; ok {
		records, err := index.Range(start, end)
		if err != nil {
			return nil, ErrRangeScan
		}

		entries, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, entries, RangeScan)
		if err != nil {
			return nil, ErrRangeScan
		}
	}

	if len(entries) == 0 {
		return nil, ErrRangeScan
	}

	return
}

// PrefixScan iterates over a key prefix at given bucket, prefix and limitNum.
// LimitNum will limit the number of entries return.
func (tx *Tx) PrefixScan(bucket string, prefix []byte, limitNum int) (es Entries, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	es = make(Entries)

	if idx, ok := tx.db.BPTreeIdx[bucket]; ok {
		records, err := idx.PrefixScan(prefix, limitNum)
		if err != nil {
			return nil, ErrPrefixScan
		}

		es, err = tx.getHintIdxDataItemsWrapper(records, limitNum, es, PrefixScan)
		if err != nil {
			return nil, ErrPrefixScan
		}
	}

	if len(es) == 0 {
		return nil, ErrPrefixScan
	}

	return
}

// Delete removes a key from the bucket at given bucket and key.
func (tx *Tx) Delete(bucket string, key []byte) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	return tx.put(bucket, key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBPTree)
}

// getHintIdxDataItemsWrapper returns wrapped entries when prefix scanning or range scanning.
func (tx *Tx) getHintIdxDataItemsWrapper(records Records, limitNum int, es Entries, scanMode string) (Entries, error) {
	for k, r := range records {
		if r.H.meta.Flag == DataDeleteFlag || r.IsExpired() {
			continue
		}

		if limitNum > 0 && len(es) < limitNum || limitNum == ScanNoLimit {
			idxMode := tx.db.opt.EntryIdxMode
			if idxMode == HintKeyAndRAMIdxMode {
				path := tx.db.getDataPath(r.H.fileID)
				df, err := NewDataFile(path, tx.db.opt.SegmentSize, tx.db.opt.RWMode)

				if err != nil {
					return nil, err
				}
				if item, err := df.ReadAt(int(r.H.dataPos)); err == nil {
					es[k] = item
				} else {
					df.rwManager.Close()
					return nil, fmt.Errorf("HintIdx r.Hi.dataPos %d, err %s", r.H.dataPos, err)
				}
				df.rwManager.Close()
			}

			if idxMode == HintKeyValAndRAMIdxMode {
				es[k] = r.E
			}
		}
	}

	return es, nil
}
