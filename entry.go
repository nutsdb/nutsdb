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

var payLoadSizeMismatchErr = errors.New("the payload size in meta mismatch with the payload size needed")

type (
	// Entry represents the data item.
	Entry struct {
		Key    []byte
		Value  []byte
		Bucket []byte
		Meta   *MetaData
	}

	// Hint represents the index of the key
	Hint struct {
		Key     []byte
		FileID  int64
		Meta    *MetaData
		DataPos uint64
	}

	// MetaData represents the meta information of the data item.
	MetaData struct {
		KeySize    uint32
		ValueSize  uint32
		Timestamp  uint64
		TTL        uint32
		Flag       uint16 // delete / set
		BucketSize uint32
		TxID       uint64
		Status     uint16 // committed / uncommitted
		Ds         uint16 // data structure
		Crc        uint32
	}
)

func (meta *MetaData) PayloadSize() int64 {
	return int64(meta.BucketSize) + int64(meta.KeySize) + int64(meta.ValueSize)
}

// Size returns the size of the entry.
func (e *Entry) Size() int64 {
	return DataEntryHeaderSize + int64(e.Meta.KeySize+e.Meta.ValueSize+e.Meta.BucketSize)
}

// Encode returns the slice after the entry be encoded.
//
//	the entry stored format:
//	|----------------------------------------------------------------------------------------------------------------|
//	|  crc  | timestamp | ksz | valueSize | flag  | TTL  |bucketSize| status | ds   | txId |  bucket |  key  | value |
//	|----------------------------------------------------------------------------------------------------------------|
//	| uint32| uint64  |uint32 |  uint32 | uint16  | uint32| uint32 | uint16 | uint16 |uint64 |[]byte|[]byte | []byte |
//	|----------------------------------------------------------------------------------------------------------------|
func (e *Entry) Encode() []byte {
	keySize := e.Meta.KeySize
	valueSize := e.Meta.ValueSize
	bucketSize := e.Meta.BucketSize

	// set DataItemHeader buf
	buf := make([]byte, e.Size())
	buf = e.setEntryHeaderBuf(buf)
	// set bucket\key\value
	copy(buf[DataEntryHeaderSize:(DataEntryHeaderSize+int64(bucketSize))], e.Bucket)
	copy(buf[(DataEntryHeaderSize+int64(bucketSize)):(DataEntryHeaderSize+int64(bucketSize+keySize))], e.Key)
	copy(buf[(DataEntryHeaderSize+int64(bucketSize+keySize)):(DataEntryHeaderSize+int64(bucketSize+keySize+valueSize))], e.Value)

	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)

	return buf
}

// setEntryHeaderBuf sets the entry header buff.
func (e *Entry) setEntryHeaderBuf(buf []byte) []byte {
	binary.LittleEndian.PutUint32(buf[0:4], e.Meta.Crc)
	binary.LittleEndian.PutUint64(buf[4:12], e.Meta.Timestamp)
	binary.LittleEndian.PutUint32(buf[12:16], e.Meta.KeySize)
	binary.LittleEndian.PutUint32(buf[16:20], e.Meta.ValueSize)
	binary.LittleEndian.PutUint16(buf[20:22], e.Meta.Flag)
	binary.LittleEndian.PutUint32(buf[22:26], e.Meta.TTL)
	binary.LittleEndian.PutUint32(buf[26:30], e.Meta.BucketSize)
	binary.LittleEndian.PutUint16(buf[30:32], e.Meta.Status)
	binary.LittleEndian.PutUint16(buf[32:34], e.Meta.Ds)
	binary.LittleEndian.PutUint64(buf[34:42], e.Meta.TxID)

	return buf
}

// IsZero checks if the entry is zero or not.
func (e *Entry) IsZero() bool {
	if e.Meta.Crc == 0 && e.Meta.KeySize == 0 && e.Meta.ValueSize == 0 && e.Meta.Timestamp == 0 {
		return true
	}
	return false
}

// GetCrc returns the crc at given buf slice.
func (e *Entry) GetCrc(buf []byte) uint32 {
	crc := crc32.ChecksumIEEE(buf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Bucket)
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)

	return crc
}

// ParsePayload means this function will parse a byte array to bucket, key, size of an entry
func (e *Entry) ParsePayload(data []byte) error {
	meta := e.Meta
	bucketLowBound := 0
	bucketHighBound := meta.BucketSize
	keyLowBound := bucketHighBound
	keyHighBound := meta.BucketSize + meta.KeySize
	valueLowBound := keyHighBound
	valueHighBound := meta.BucketSize + meta.KeySize + meta.ValueSize

	// parse bucket
	e.Bucket = data[bucketLowBound:bucketHighBound]
	// parse key
	e.Key = data[keyLowBound:keyHighBound]
	// parse value
	e.Value = data[valueLowBound:valueHighBound]
	return nil
}

// checkPayloadSize checks the payload size
func (e *Entry) checkPayloadSize(size int64) error {
	if e.Meta.PayloadSize() != size {
		return payLoadSizeMismatchErr
	}
	return nil
}

// ParseMeta parse meta object to entry
func (e *Entry) ParseMeta(buf []byte) error {
	e.Meta = NewMetaData().WithCrc(binary.LittleEndian.Uint32(buf[0:4])).
		WithTimeStamp(binary.LittleEndian.Uint64(buf[4:12])).WithKeySize(binary.LittleEndian.Uint32(buf[12:16])).
		WithValueSize(binary.LittleEndian.Uint32(buf[16:20])).WithFlag(binary.LittleEndian.Uint16(buf[20:22])).
		WithTTL(binary.LittleEndian.Uint32(buf[22:26])).WithBucketSize(binary.LittleEndian.Uint32(buf[26:30])).
		WithStatus(binary.LittleEndian.Uint16(buf[30:32])).WithDs(binary.LittleEndian.Uint16(buf[32:34])).
		WithTxID(binary.LittleEndian.Uint64(buf[34:42]))
	return nil
}

// isFilter to confirm if this entry is can be filtered
func (e *Entry) isFilter() bool {
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
	return OneOfUint16Array(meta.Flag, filterDataSet)
}

// valid check the entry fields valid or not
func (e *Entry) valid() error {
	if len(e.Key) == 0 {
		return ErrKeyEmpty
	}
	if len(e.Bucket) > MAX_SIZE || len(e.Key) > MAX_SIZE || len(e.Value) > MAX_SIZE {
		return ErrDataSizeExceed
	}
	return nil
}

// NewHint new Hint object
func NewHint() *Hint {
	return new(Hint)
}

// WithKey set key to Hint
func (h *Hint) WithKey(key []byte) *Hint {
	h.Key = key
	return h
}

// WithFileId set FileID to Hint
func (h *Hint) WithFileId(fid int64) *Hint {
	h.FileID = fid
	return h
}

// WithMeta set Meta to Hint
func (h *Hint) WithMeta(meta *MetaData) *Hint {
	h.Meta = meta
	return h
}

// WithDataPos set DataPos to Hint
func (h *Hint) WithDataPos(pos uint64) *Hint {
	h.DataPos = pos
	return h
}

// NewEntry new Entry Object
func NewEntry() *Entry {
	return new(Entry)
}

// WithKey set key to Entry
func (e *Entry) WithKey(key []byte) *Entry {
	e.Key = key
	return e
}

// WithValue set value to Entry
func (e *Entry) WithValue(value []byte) *Entry {
	e.Value = value
	return e
}

// WithMeta set meta to Entry
func (e *Entry) WithMeta(meta *MetaData) *Entry {
	e.Meta = meta
	return e
}

// WithBucket set bucket to Entry
func (e *Entry) WithBucket(bucket []byte) *Entry {
	e.Bucket = bucket
	return e
}

// GetBucketString return the string of bucket
func (e *Entry) GetBucketString() string {
	return string(e.Bucket)
}

// GetTxIDBytes return the bytes of TxID
func (e *Entry) GetTxIDBytes() []byte {
	return []byte(strconv2.Int64ToStr(int64(e.Meta.TxID)))
}

func NewMetaData() *MetaData {
	return new(MetaData)
}

func (meta *MetaData) WithKeySize(keySize uint32) *MetaData {
	meta.KeySize = keySize
	return meta
}

func (meta *MetaData) WithValueSize(valueSize uint32) *MetaData {
	meta.ValueSize = valueSize
	return meta
}

func (meta *MetaData) WithTimeStamp(timestamp uint64) *MetaData {
	meta.Timestamp = timestamp
	return meta
}

func (meta *MetaData) WithTTL(ttl uint32) *MetaData {
	meta.TTL = ttl
	return meta
}

func (meta *MetaData) WithFlag(flag uint16) *MetaData {
	meta.Flag = flag
	return meta
}

func (meta *MetaData) WithBucketSize(bucketSize uint32) *MetaData {
	meta.BucketSize = bucketSize
	return meta
}

func (meta *MetaData) WithTxID(txID uint64) *MetaData {
	meta.TxID = txID
	return meta
}

func (meta *MetaData) WithStatus(status uint16) *MetaData {
	meta.Status = status
	return meta
}

func (meta *MetaData) WithDs(ds uint16) *MetaData {
	meta.Ds = ds
	return meta
}

func (meta *MetaData) WithCrc(crc uint32) *MetaData {
	meta.Crc = crc
	return meta
}

// Entries represents entries
type Entries []*Entry

func (e Entries) Len() int { return len(e) }

func (e Entries) Less(i, j int) bool {
	l := string(e[i].Key)
	r := string(e[j].Key)

	return strings.Compare(l, r) == -1
}

func (e Entries) Swap(i, j int) { e[i], e[j] = e[j], e[i] }

func (e Entries) processEntriesScanOnDisk() (result []*Entry) {
	sort.Sort(e)
	for _, ele := range e {
		curE := ele
		if !IsExpired(curE.Meta.TTL, curE.Meta.Timestamp) && curE.Meta.Flag != DataDeleteFlag {
			result = append(result, curE)
		}
	}

	return result
}

func (e Entries) ToCEntries(lFunc func(l, r string) bool) CEntries {
	return CEntries{
		Entries:  e,
		LessFunc: lFunc,
	}
}

type CEntries struct {
	Entries
	LessFunc func(l, r string) bool
}

func (c CEntries) Len() int { return len(c.Entries) }

func (c CEntries) Less(i, j int) bool {
	l := string(c.Entries[i].Key)
	r := string(c.Entries[j].Key)
	if c.LessFunc != nil {
		return c.LessFunc(l, r)
	}

	return c.Entries.Less(i, j)
}

func (c CEntries) Swap(i, j int) { c.Entries[i], c.Entries[j] = c.Entries[j], c.Entries[i] }

func (c CEntries) processEntriesScanOnDisk() (result []*Entry) {
	sort.Sort(c)
	for _, ele := range c.Entries {
		curE := ele
		if !IsExpired(curE.Meta.TTL, curE.Meta.Timestamp) && curE.Meta.Flag != DataDeleteFlag {
			result = append(result, curE)
		}
	}

	return result
}

type EntryWhenRecovery struct {
	Entry
	fid int64
	off int64
}

type dataInTx struct {
	es       []*EntryWhenRecovery
	txId     uint64
	startOff int64
}

func (dt *dataInTx) isSameTx(e *EntryWhenRecovery) bool {
	return dt.txId == e.Meta.TxID
}

func (dt *dataInTx) appendEntry(e *EntryWhenRecovery) {
	dt.es = append(dt.es, e)
}

func (dt *dataInTx) reset() {
	dt.es = make([]*EntryWhenRecovery, 0)
	dt.txId = 0
}
