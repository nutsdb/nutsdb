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
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sort"
	"strings"

	"github.com/xujiajun/utils/strconv2"
)

var (
	ErrPayLoadSizeMismatch   = errors.New("the payload size in Meta mismatch with the payload size needed")
	ErrHeaderSizeOutOfBounds = errors.New("the header size is out of bounds")
)

const (
	maxEntryHeaderSize = 4 + binary.MaxVarintLen32*3 + binary.MaxVarintLen64*3 + binary.MaxVarintLen16*3
	minEntryHeaderSize = 4 + 9
)

type (
	// entry represents the data item.
	entry struct {
		Key   []byte
		Value []byte
		Meta  *metaData
	}
)

// size returns the size of the entry.
func (e *entry) size() int64 {
	return e.Meta.size() + int64(e.Meta.KeySize+e.Meta.ValueSize)
}

// encode returns the slice after the entry be encoded.
//
//	the entry stored format:
//	|----------------------------------------------------------------------------------------------------------|
//	|  crc  | timestamp | ksz | valueSize | flag  | TTL  | status | ds   | txId |  bucketId |  key  | value    |
//	|----------------------------------------------------------------------------------------------------------|
//	| uint32| uint64  |uint32 |  uint32 | uint16  | uint32| uint16 | uint16 |uint64 | uint64 | []byte | []byte |
//	|----------------------------------------------------------------------------------------------------------|
func (e *entry) encode() []byte {
	keySize := e.Meta.KeySize
	valueSize := e.Meta.ValueSize

	buf := make([]byte, maxEntryHeaderSize+keySize+valueSize)

	index := e.setEntryHeaderBuf(buf)
	copy(buf[index:], e.Key)
	index += int(keySize)
	copy(buf[index:], e.Value)
	index += int(valueSize)

	buf = buf[:index]

	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)

	return buf
}

// setEntryHeaderBuf sets the entry header buff.
func (e *entry) setEntryHeaderBuf(buf []byte) int {
	index := 4

	index += binary.PutUvarint(buf[index:], e.Meta.Timestamp)
	index += binary.PutUvarint(buf[index:], uint64(e.Meta.KeySize))
	index += binary.PutUvarint(buf[index:], uint64(e.Meta.ValueSize))
	index += binary.PutUvarint(buf[index:], uint64(e.Meta.Flag))
	index += binary.PutUvarint(buf[index:], uint64(e.Meta.TTL))
	index += binary.PutUvarint(buf[index:], uint64(e.Meta.Status))
	index += binary.PutUvarint(buf[index:], uint64(e.Meta.Ds))
	index += binary.PutUvarint(buf[index:], e.Meta.TxID)
	index += binary.PutUvarint(buf[index:], e.Meta.BucketId)

	return index
}

// isZero checks if the entry is zero or not.
func (e *entry) isZero() bool {
	if e.Meta.Crc == 0 && e.Meta.KeySize == 0 && e.Meta.ValueSize == 0 && e.Meta.Timestamp == 0 {
		return true
	}
	return false
}

// getCrc returns the crc at given buf slice.
func (e *entry) getCrc(buf []byte) uint32 {
	crc := crc32.ChecksumIEEE(buf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)

	return crc
}

// parsePayload means this function will parse a byte array to bucket, key, size of an entry
func (e *entry) parsePayload(data []byte) error {
	meta := e.Meta
	keyLowBound := 0
	keyHighBound := meta.KeySize
	valueLowBound := keyHighBound
	valueHighBound := meta.KeySize + meta.ValueSize

	// parse key
	e.Key = data[keyLowBound:keyHighBound]
	// parse value
	e.Value = data[valueLowBound:valueHighBound]
	return nil
}

// checkPayloadSize checks the payload size
func (e *entry) checkPayloadSize(size int64) error {
	if e.Meta.payloadSize() != size {
		return ErrPayLoadSizeMismatch
	}
	return nil
}

// parseMeta parse Meta object to entry
func (e *entry) parseMeta(buf []byte) (int64, error) {
	// If the length of the header is less than minEntryHeaderSize,
	// it means that the final remaining capacity of the file is not enough to write a record,
	// and an error needs to be returned.
	if len(buf) < minEntryHeaderSize {
		return 0, ErrHeaderSizeOutOfBounds
	}

	e.Meta = newMetaData()

	e.Meta.withCrc(binary.LittleEndian.Uint32(buf[0:4]))

	index := 4

	timestamp, n := binary.Uvarint(buf[index:])
	index += n
	keySize, n := binary.Uvarint(buf[index:])
	index += n
	valueSize, n := binary.Uvarint(buf[index:])
	index += n
	flag, n := binary.Uvarint(buf[index:])
	index += n
	ttl, n := binary.Uvarint(buf[index:])
	index += n
	status, n := binary.Uvarint(buf[index:])
	index += n
	ds, n := binary.Uvarint(buf[index:])
	index += n
	txId, n := binary.Uvarint(buf[index:])
	index += n
	bucketId, n := binary.Uvarint(buf[index:])
	index += n

	e.Meta.
		withTimeStamp(timestamp).
		withKeySize(uint32(keySize)).
		withValueSize(uint32(valueSize)).
		withFlag(uint16(flag)).
		withTTL(uint32(ttl)).
		withStatus(uint16(status)).
		withDs(uint16(ds)).
		withTxID(txId).
		withBucketId(bucketId)

	return int64(index), nil
}

// isFilter to confirm if this entry is can be filtered
func (e *entry) isFilter() bool {
	meta := e.Meta
	var filterDataSet = []uint16{
		DataDeleteFlag,
		DataRPopFlag,
		DataLPopFlag,
		DataLRemFlag,
		DataLTrimFlag,
		DataZRemFlag,
		DataZRemRangeByRankFlag,
		DataZPopMaxFlag,
		DataZPopMinFlag,
		DataLRemByIndex,
	}
	return oneOfUint16Array(meta.Flag, filterDataSet)
}

// valid check the entry fields valid or not
func (e *entry) valid() error {
	if len(e.Key) == 0 {
		return ErrKeyEmpty
	}
	if len(e.Key) > MAX_SIZE || len(e.Value) > MAX_SIZE {
		return ErrDataSizeExceed
	}
	return nil
}

// newEntry new entry Object
func newEntry() *entry {
	return new(entry)
}

// withKey set key to entry
func (e *entry) withKey(key []byte) *entry {
	e.Key = key
	return e
}

// withValue set value to entry
func (e *entry) withValue(value []byte) *entry {
	e.Value = value
	return e
}

// withMeta set Meta to entry
func (e *entry) withMeta(meta *metaData) *entry {
	e.Meta = meta
	return e
}

// getTxIDBytes return the bytes of TxID
func (e *entry) getTxIDBytes() []byte {
	return []byte(strconv2.Int64ToStr(int64(e.Meta.TxID)))
}

func (e *entry) isBelongsToBPlusTree() bool {
	return e.Meta.isBPlusTree()
}

func (e *entry) isBelongsToList() bool {
	return e.Meta.isList()
}

func (e *entry) isBelongsToSet() bool {
	return e.Meta.isSet()
}

func (e *entry) isBelongsToSortSet() bool {
	return e.Meta.isSortSet()
}

// entries represents entries
type entries []*entry

func (e entries) Len() int { return len(e) }

func (e entries) Less(i, j int) bool {
	l := string(e[i].Key)
	r := string(e[j].Key)

	return strings.Compare(l, r) == -1
}

func (e entries) Swap(i, j int) { e[i], e[j] = e[j], e[i] }

func (e entries) processEntriesScanOnDisk() (result []*entry) {
	sort.Sort(e)
	for _, ele := range e {
		curE := ele
		if !isExpired(curE.Meta.TTL, curE.Meta.Timestamp) && curE.Meta.Flag != DataDeleteFlag {
			result = append(result, curE)
		}
	}

	return result
}

func (e entries) toCEntries(lFunc func(l, r string) bool) cEntries {
	return cEntries{
		entries:  e,
		LessFunc: lFunc,
	}
}

type cEntries struct {
	entries
	LessFunc func(l, r string) bool
}

func (c cEntries) Len() int { return len(c.entries) }

func (c cEntries) Less(i, j int) bool {
	l := string(c.entries[i].Key)
	r := string(c.entries[j].Key)
	if c.LessFunc != nil {
		return c.LessFunc(l, r)
	}

	return c.entries.Less(i, j)
}

func (c cEntries) Swap(i, j int) { c.entries[i], c.entries[j] = c.entries[j], c.entries[i] }

func (c cEntries) processEntriesScanOnDisk() (result []*entry) {
	sort.Sort(c)
	for _, ele := range c.entries {
		curE := ele
		if !isExpired(curE.Meta.TTL, curE.Meta.Timestamp) && curE.Meta.Flag != DataDeleteFlag {
			result = append(result, curE)
		}
	}

	return result
}

type entryWhenRecovery struct {
	entry
	fid int64
	off int64
}

type dataInTx struct {
	es       []*entryWhenRecovery
	txId     uint64
	startOff int64
}

func (dt *dataInTx) isSameTx(e *entryWhenRecovery) bool {
	return dt.txId == e.Meta.TxID
}

func (dt *dataInTx) appendEntry(e *entryWhenRecovery) {
	dt.es = append(dt.es, e)
}

func (dt *dataInTx) reset() {
	dt.es = make([]*entryWhenRecovery, 0)
	dt.txId = 0
}
