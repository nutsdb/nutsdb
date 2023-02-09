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
)

var payLoadSizeMismatchErr = errors.New("the payload size in meta mismatch with the payload size needed")

type (
	// Entry represents the data item.
	Entry struct {
		Key      []byte
		Value    []byte
		Meta     *MetaData
		crc      uint32
		position uint64
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
		Bucket     []byte
		BucketSize uint32
		TxID       uint64
		Status     uint16 // committed / uncommitted
		Ds         uint16 // data structure
	}
)

func (meta *MetaData) PayloadSize() int64 {
	return int64(meta.BucketSize) + int64(meta.KeySize) + int64(meta.ValueSize)
}

// Size returns the size of the entry.
func (e *Entry) Size() int64 {
	return int64(DataEntryHeaderSize + e.Meta.KeySize + e.Meta.ValueSize + e.Meta.BucketSize)
}

// Encode returns the slice after the entry be encoded.
//
//  the entry stored format:
//  |----------------------------------------------------------------------------------------------------------------|
//  |  crc  | timestamp | ksz | valueSize | flag  | TTL  |bucketSize| status | ds   | txId |  bucket |  key  | value |
//  |----------------------------------------------------------------------------------------------------------------|
//  | uint32| uint64  |uint32 |  uint32 | uint16  | uint32| uint32 | uint16 | uint16 |uint64 |[]byte|[]byte | []byte |
//  |----------------------------------------------------------------------------------------------------------------|
//
func (e *Entry) Encode() []byte {
	keySize := e.Meta.KeySize
	valueSize := e.Meta.ValueSize
	bucketSize := e.Meta.BucketSize

	// set DataItemHeader buf
	buf := make([]byte, e.Size())
	buf = e.setEntryHeaderBuf(buf)
	// set bucket\key\value
	copy(buf[DataEntryHeaderSize:(DataEntryHeaderSize+bucketSize)], e.Meta.Bucket)
	copy(buf[(DataEntryHeaderSize+bucketSize):(DataEntryHeaderSize+bucketSize+keySize)], e.Key)
	copy(buf[(DataEntryHeaderSize+bucketSize+keySize):(DataEntryHeaderSize+bucketSize+keySize+valueSize)], e.Value)

	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)

	return buf
}

// setEntryHeaderBuf sets the entry header buff.
func (e *Entry) setEntryHeaderBuf(buf []byte) []byte {
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
	if e.crc == 0 && e.Meta.KeySize == 0 && e.Meta.ValueSize == 0 && e.Meta.Timestamp == 0 {
		return true
	}
	return false
}

// GetCrc returns the crc at given buf slice.
func (e *Entry) GetCrc(buf []byte) uint32 {
	crc := crc32.ChecksumIEEE(buf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Meta.Bucket)
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
	e.Meta.Bucket = data[bucketLowBound:bucketHighBound]
	// parse key
	e.Key = data[keyLowBound:keyHighBound]
	// parse value
	e.Value = data[valueLowBound:valueHighBound]
	return nil
}

func (e *Entry) checkPayloadSize(size int64) error {
	if e.Meta.PayloadSize() != size {
		return payLoadSizeMismatchErr
	}
	return nil
}

func (e *Entry) ParseMeta(buf []byte) error {
	meta := &MetaData{
		Timestamp:  binary.LittleEndian.Uint64(buf[4:12]),
		KeySize:    binary.LittleEndian.Uint32(buf[12:16]),
		ValueSize:  binary.LittleEndian.Uint32(buf[16:20]),
		Flag:       binary.LittleEndian.Uint16(buf[20:22]),
		TTL:        binary.LittleEndian.Uint32(buf[22:26]),
		BucketSize: binary.LittleEndian.Uint32(buf[26:30]),
		Status:     binary.LittleEndian.Uint16(buf[30:32]),
		Ds:         binary.LittleEndian.Uint16(buf[32:34]),
		TxID:       binary.LittleEndian.Uint64(buf[34:42]),
	}
	e.Meta = meta
	return nil
}
