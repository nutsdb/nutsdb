// Copyright 2019 The nutsdb Author. All rights reserved.
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
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/xujiajun/utils/strconv2"
)

func getNewKey(bucket string, key []byte) []byte {
	newKey := []byte(bucket)
	newKey = append(newKey, key...)
	return newKey
}

// Get retrieves the value for a key in the bucket.
// The returned value is only valid for the life of the transaction.
func (tx *Tx) Get(bucket string, key []byte) (e *Entry, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	idxMode := tx.db.opt.EntryIdxMode

	if idxMode == HintBPTSparseIdxMode {
		// Read in memory.
		newKey := getNewKey(bucket, key)
		r, err := tx.db.ActiveBPTreeIdx.Find(newKey)
		if err == nil && r != nil {
			if r.H.meta.Flag == DataDeleteFlag || r.IsExpired() {
				return nil, ErrNotFoundKey
			}

			if _, err := tx.db.ActiveCommittedTxIdsIdx.Find([]byte(strconv2.Int64ToStr(int64(r.H.meta.txID)))); err == nil {
				path := tx.db.getDataPath(r.H.fileID)
				df, err := NewDataFile(path, tx.db.opt.SegmentSize, tx.db.opt.RWMode)
				defer df.rwManager.Close()
				if err != nil {
					return nil, err
				}

				return df.ReadAt(int(r.H.dataPos))
			}

			return nil, ErrNotFoundKey
		}

		// Read on disk.
		var bptSparseIdxGroup []*BPTreeRootIdx
		for _, bptRootIdxPointer := range tx.db.BPTreeRootIdxes {
			bptSparseIdxGroup = append(bptSparseIdxGroup, &BPTreeRootIdx{
				fID:     bptRootIdxPointer.fID,
				rootOff: bptRootIdxPointer.rootOff,
				start:   bptRootIdxPointer.start,
				end:     bptRootIdxPointer.end,
			})
		}

		// Sort the fid from largest to smallest, to ensure that the latest data is first compared.
		SortFID(bptSparseIdxGroup, func(p, q *BPTreeRootIdx) bool {
			return p.fID > q.fID
		})

		for _, bptSparse := range bptSparseIdxGroup {
			if compare(newKey, bptSparse.start) >= 0 && compare(newKey, bptSparse.end) <= 0 {
				fID := bptSparse.fID
				rootOff := bptSparse.rootOff

				e, err = tx.FindOnDisk(fID, rootOff, newKey)
				if err == nil && e != nil {
					if e.Meta.Flag == DataDeleteFlag || IsExpired(e.Meta.TTL, e.Meta.timestamp) {
						return nil, ErrNotFoundKey
					}

					txIDStr := strconv2.Int64ToStr(int64(e.Meta.txID))
					if _, err := tx.db.ActiveCommittedTxIdsIdx.Find([]byte(txIDStr)); err == nil {
						return e, err
					}
					if ok, _ := tx.FindTxIdOnDisk(fID, e.Meta.txID); !ok {
						return nil, ErrNotFoundKey
					}

					return e, err
				}
			}
			continue
		}

		return nil, ErrNotFoundKey
	}

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

//GetAll returns all keys and values of the bucket stored at given bucket.
func (tx *Tx) GetAll(bucket string) (entries Entries, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	entries = Entries{}

	if index, ok := tx.db.BPTreeIdx[bucket]; ok {
		records, err := index.All()
		if err != nil {
			return nil, ErrBucketEmpty
		}

		entries, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, entries, RangeScan)
		if err != nil {
			return nil, ErrBucketEmpty
		}
	}

	if len(entries) == 0 {
		return nil, ErrBucketEmpty
	}

	return
}

// RangeScan query a range at given bucket, start and end slice.
func (tx *Tx) RangeScan(bucket string, start, end []byte) (es Entries, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		newStart, newEnd := getNewKey(bucket, start), getNewKey(bucket, end)
		records, err := tx.db.ActiveBPTreeIdx.Range(newStart, newEnd)

		if err == nil && records != nil {
			for _, r := range records {
				path := tx.db.getDataPath(r.H.fileID)
				df, err := NewDataFile(path, tx.db.opt.SegmentSize, tx.db.opt.RWMode)
				if err != nil {
					df.rwManager.Close()
					return nil, err
				}
				if item, err := df.ReadAt(int(r.H.dataPos)); err == nil {
					es = append(es, item)
				} else {
					df.rwManager.Close()
					return nil, fmt.Errorf("HintIdx r.Hi.dataPos %d, err %s", r.H.dataPos, err)
				}
				df.rwManager.Close()
			}
		}

		es = append(es, tx.rangeScanOnDisk(bucket, start, end)...)

		return processEntriesScanOnDisk(es), nil
	}

	if index, ok := tx.db.BPTreeIdx[bucket]; ok {
		records, err := index.Range(start, end)
		if err != nil {
			return nil, ErrRangeScan
		}

		es, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, es, RangeScan)
		if err != nil {
			return nil, ErrRangeScan
		}
	}

	if len(es) == 0 {
		return nil, ErrRangeScan
	}

	return
}

func (tx *Tx) rangeScanOnDisk(bucket string, start, end []byte) []*Entry {
	var result []*Entry

	bptSparseIdxGroup := tx.db.BPTreeRootIdxes

	SortFID(bptSparseIdxGroup, func(p, q *BPTreeRootIdx) bool {
		return p.fID > q.fID
	})

	newStart, newEnd := getNewKey(bucket, start), getNewKey(bucket, end)

	for _, bptSparseIdx := range bptSparseIdxGroup {
		if compare(newStart, bptSparseIdx.start) >= 0 && compare(newEnd, bptSparseIdx.end) <= 0 {
			entries := tx.findRangeOnDisk(int64(bptSparseIdx.fID), int64(bptSparseIdx.rootOff), newStart, newEnd)
			result = append(
				result,
				entries...,
			)
		}
	}

	return result
}

func (tx *Tx) prefixScanOnDisk(bucket string, prefix []byte, limitNum int) []*Entry {
	var result []*Entry
	bptSparseIdxGroup := tx.db.BPTreeRootIdxes
	SortFID(bptSparseIdxGroup, func(p, q *BPTreeRootIdx) bool {
		return p.fID > q.fID
	})

	newPrefix := getNewKey(bucket, prefix)
	for _, bptSparseIdx := range bptSparseIdxGroup {
		if compare(newPrefix, bptSparseIdx.end) <= 0 {
			entries := tx.findPrefixOnDisk(int64(bptSparseIdx.fID), int64(bptSparseIdx.rootOff), prefix, limitNum)
			result = append(
				result,
				entries...,
			)
			if len(result) == limitNum {
				return result
			}
		}
	}
	return result
}

func processEntriesScanOnDisk(entriesTemp []*Entry) (result []*Entry) {
	var entriesMap map[string]*Entry
	entriesMap = make(map[string]*Entry)

	for _, entry := range entriesTemp {
		if tempEntry, ok := entriesMap[string(entry.Key)]; ok {
			if tempEntry.Meta.timestamp < entry.Meta.timestamp {
				if !IsExpired(entry.Meta.TTL, entry.Meta.timestamp) || entry.Meta.Flag != DataDeleteFlag {
					delete(entriesMap, string(entry.Key))
				}
				entriesMap[string(entry.Key)] = entry
			}
		} else {
			if !IsExpired(entry.Meta.TTL, entry.Meta.timestamp) && entry.Meta.Flag != DataDeleteFlag {
				entriesMap[string(entry.Key)] = entry
			}
		}
	}

	keys, es := SortedEntryKeys(entriesMap)

	for _, key := range keys {
		result = append(result, es[key])
	}

	return result
}

func (tx *Tx) findPrefixOnDisk(fID, rootOff int64, prefix []byte, limitNum int) (es []*Entry) {
	var (
		i, j     uint16
		entry    *Entry
		scanFlag bool
		curr     *BinaryNode
		err      error
		numFound int
	)

	es = []*Entry{}
	if curr, err = tx.FindLeafOnDisk(fID, rootOff, prefix); err != nil && curr == nil {
		return nil
	}

	for j = 0; j < curr.KeysNum; j++ {
		df, err := NewDataFile(tx.db.getDataPath(int64(fID)), tx.db.opt.SegmentSize, tx.db.opt.RWMode)
		if err != nil {
			return nil
		}

		entry, err = df.ReadAt(int(curr.Keys[j]))
		df.rwManager.Close()

		if compare(entry.Key, prefix) >= 0 {
			break
		}
	}

	scanFlag = true
	numFound = 0
	filepath := tx.db.getBPTPath(fID)

	for curr != nil && scanFlag {
		for i = j; i < curr.KeysNum; i++ {
			df, err := NewDataFile(tx.db.getDataPath(int64(fID)), tx.db.opt.SegmentSize, tx.db.opt.RWMode)
			if err != nil {
				return nil
			}

			entry, err = df.ReadAt(int(curr.Keys[i]))
			df.rwManager.Close()

			if !bytes.HasPrefix(entry.Key, prefix) {
				scanFlag = false
				break
			}

			es = append(es, entry)
			numFound++

			if limitNum > 0 && numFound == limitNum {
				scanFlag = false
				break
			}
		}

		address := curr.NextAddress
		curr, err = ReadNode(filepath, address)

		j = 0
	}

	return
}

func (tx *Tx) findRangeOnDisk(fID, rootOff int64, start, end []byte) (es []*Entry) {
	var (
		i, j     uint16
		entry    *Entry
		scanFlag bool
		curr     *BinaryNode
		err      error
	)

	if curr, err = tx.FindLeafOnDisk(fID, rootOff, start); err != nil && curr == nil {
		return nil
	}

	for j = 0; j < curr.KeysNum; j++ {
		df, err := NewDataFile(tx.db.getDataPath(int64(fID)), tx.db.opt.SegmentSize, tx.db.opt.RWMode)
		if err != nil {
			return nil
		}

		entry, err = df.ReadAt(int(curr.Keys[j]))
		df.rwManager.Close()

		newStart := getNewKey(string(entry.Meta.bucket), entry.Key)
		if compare(newStart, start) >= 0 {
			break
		}
	}

	scanFlag = true
	filepath := tx.db.getBPTPath(fID)

	for curr != nil && scanFlag {
		for i = j; i < curr.KeysNum; i++ {
			df, err := NewDataFile(tx.db.getDataPath(int64(fID)), tx.db.opt.SegmentSize, tx.db.opt.RWMode)
			if err != nil {
				return nil
			}

			entry, err = df.ReadAt(int(curr.Keys[i]))
			df.rwManager.Close()

			newEnd := getNewKey(string(entry.Meta.bucket), entry.Key)
			if compare(newEnd, end) > 0 {
				scanFlag = false
				break
			}

			es = append(es, entry)

		}

		address := curr.NextAddress
		curr, err = ReadNode(filepath, address)

		j = 0
	}

	return
}

// PrefixScan iterates over a key prefix at given bucket, prefix and limitNum.
// LimitNum will limit the number of entries return.
func (tx *Tx) PrefixScan(bucket string, prefix []byte, limitNum int) (es Entries, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		newPrefix := getNewKey(bucket, prefix)
		records, err := tx.db.ActiveBPTreeIdx.PrefixScan(newPrefix, limitNum)
		if err == nil && records != nil {
			for _, r := range records {
				path := tx.db.getDataPath(r.H.fileID)
				df, err := NewDataFile(path, tx.db.opt.SegmentSize, tx.db.opt.RWMode)
				if err != nil {
					df.rwManager.Close()
					return nil, err
				}
				if item, err := df.ReadAt(int(r.H.dataPos)); err == nil {
					es = append(es, item)
					if len(es) == limitNum {
						return es, nil
					}
				} else {
					df.rwManager.Close()
					return nil, fmt.Errorf("HintIdx r.Hi.dataPos %d, err %s", r.H.dataPos, err)
				}
				df.rwManager.Close()
			}
		}

		leftNum := limitNum - len(es)
		if leftNum > 0 {
			es = append(es, tx.prefixScanOnDisk(bucket, prefix, leftNum)...)
		}

		return processEntriesScanOnDisk(es), nil
	}

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
	for _, r := range records {
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
					es = append(es, item)
				} else {
					df.rwManager.Close()
					return nil, fmt.Errorf("HintIdx r.Hi.dataPos %d, err %s", r.H.dataPos, err)
				}
				df.rwManager.Close()
			}

			if idxMode == HintKeyValAndRAMIdxMode {
				es = append(es, r.E)
			}
		}
	}

	return es, nil
}

func (tx *Tx) FindTxIdOnDisk(fID, txId uint64) (ok bool, err error) {
	var i uint16

	filepath := tx.db.getBPTRootTxIdPath(int64(fID))
	node, err := ReadNode(filepath, 0)

	if err != nil {
		return false, err
	}

	filepath = tx.db.getBPTTxIdPath(int64(fID))
	rootAddress := node.Keys[0]
	curr, err := ReadNode(filepath, rootAddress)

	if err != nil {
		return false, err
	}

	txIdStr := strconv2.IntToStr(int(txId))

	for curr.IsLeaf != 1 {
		i = 0
		for i < curr.KeysNum {
			if compare([]byte(txIdStr), []byte(strconv2.Int64ToStr(curr.Keys[i]))) >= 0 {
				i++
			} else {
				break
			}
		}

		address := curr.Pointers[i]
		curr, err = ReadNode(filepath, int64(address))
	}

	if curr == nil {
		return false, ErrKeyNotFound
	}

	for i = 0; i < curr.KeysNum; i++ {
		if compare([]byte(txIdStr), []byte(strconv2.Int64ToStr(curr.Keys[i]))) == 0 {
			break
		}
	}

	if i == curr.KeysNum {
		return false, ErrKeyNotFound
	}

	return true, nil
}

func (tx *Tx) FindOnDisk(fID uint64, rootOff uint64, key []byte) (entry *Entry, err error) {
	var (
		bnLeaf *BinaryNode
		i      uint16
		df     *DataFile
	)

	bnLeaf, err = tx.FindLeafOnDisk(int64(fID), int64(rootOff), key)

	if bnLeaf == nil {
		return nil, ErrKeyNotFound
	}

	for i = 0; i < bnLeaf.KeysNum; i++ {
		df, err = NewDataFile(tx.db.getDataPath(int64(fID)), tx.db.opt.SegmentSize, tx.db.opt.RWMode)
		if err != nil {
			return nil, err
		}

		entry, err = df.ReadAt(int(bnLeaf.Keys[i]))
		df.rwManager.Close()
		if err != nil {
			return nil, err
		}

		newKey := getNewKey(string(entry.Meta.bucket), entry.Key)
		if entry != nil && compare(key, newKey) == 0 {
			return entry, nil
		}
	}

	if i == bnLeaf.KeysNum {
		return nil, ErrKeyNotFound
	}

	return
}

func (tx *Tx) FindLeafOnDisk(fId int64, rootOff int64, key []byte) (bn *BinaryNode, err error) {
	var i uint16
	var curr *BinaryNode

	filepath := tx.db.getBPTPath(fId)
	curr, err = ReadNode(filepath, rootOff)
	if err != nil {
		return nil, err
	}

	for curr.IsLeaf != 1 {
		i = 0
		for i < curr.KeysNum {
			df, err := NewDataFile(tx.db.getDataPath(fId), tx.db.opt.SegmentSize, tx.db.opt.RWMode)
			if err != nil {
				return nil, err
			}

			item, err := df.ReadAt(int(curr.Keys[i]))
			df.rwManager.Close()

			newKey := getNewKey(string(item.Meta.bucket), item.Key)
			if compare(key, newKey) >= 0 {
				i++
			} else {
				break
			}
		}
		address := curr.Pointers[i]

		curr, err = ReadNode(filepath, int64(address))
	}

	return curr, nil
}
