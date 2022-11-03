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

package model

import (
	"encoding/binary"
	"github.com/xujiajun/nutsdb/consts"
	"hash/crc32"
)

type (
	// Entry represents the data item.
	Entry struct {
		Key      []byte
		Value    []byte
		Meta     *MetaData
		Crc      uint32
		Position uint64
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
		Flag       consts.DataFlag // delete / set
		Bucket     []byte
		BucketSize uint32
		TxID       uint64
		Status     consts.DataStatus // committed / uncommitted
		Ds         consts.DataStruct // data structure
	}
)

// Size returns the size of the entry.
func (e *Entry) Size() int64 {
	return int64(consts.DataEntryHeaderSize + e.Meta.KeySize + e.Meta.ValueSize + e.Meta.BucketSize)
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
	buf = e.DecodeHeader(buf)
	// set bucket\key\value
	copy(buf[consts.DataEntryHeaderSize:(consts.DataEntryHeaderSize+bucketSize)], e.Meta.Bucket)
	copy(buf[(consts.DataEntryHeaderSize+bucketSize):(consts.DataEntryHeaderSize+bucketSize+keySize)], e.Key)
	copy(buf[(consts.DataEntryHeaderSize+bucketSize+keySize):(consts.DataEntryHeaderSize+bucketSize+keySize+valueSize)], e.Value)

	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)

	return buf
}

// DecodeHeader sets the entry header buff.
func (e *Entry) DecodeHeader(buf []byte) []byte {
	binary.LittleEndian.PutUint64(buf[4:12], e.Meta.Timestamp)
	binary.LittleEndian.PutUint32(buf[12:16], e.Meta.KeySize)
	binary.LittleEndian.PutUint32(buf[16:20], e.Meta.ValueSize)
	binary.LittleEndian.PutUint16(buf[20:22], uint16(e.Meta.Flag))
	binary.LittleEndian.PutUint32(buf[22:26], e.Meta.TTL)
	binary.LittleEndian.PutUint32(buf[26:30], e.Meta.BucketSize)
	binary.LittleEndian.PutUint16(buf[30:32], uint16(e.Meta.Status))
	binary.LittleEndian.PutUint16(buf[32:34], uint16(e.Meta.Ds))
	binary.LittleEndian.PutUint64(buf[34:42], e.Meta.TxID)

	return buf
}

// IsZero checks if the entry is zero or not.
func (e *Entry) IsZero() bool {
	if e.Crc == 0 && e.Meta.KeySize == 0 && e.Meta.ValueSize == 0 && e.Meta.Timestamp == 0 {
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
