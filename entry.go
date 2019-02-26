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
	"encoding/binary"
	"hash/crc32"
)

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
		key     []byte
		fileID  int64
		meta    *MetaData
		dataPos uint64
	}

	// MetaData represents the meta information of the data item.
	MetaData struct {
		keySize    uint32
		valueSize  uint32
		timestamp  uint64
		TTL        uint32
		Flag       uint16 // delete / set
		bucket     []byte
		bucketSize uint32
		txID       uint64
		status     uint16 // committed / uncommitted
		ds         uint16 // data structure
	}
)

// Size returns the size of the entry.
func (e *Entry) Size() int64 {
	return int64(DataEntryHeaderSize + e.Meta.keySize + e.Meta.valueSize + e.Meta.bucketSize)
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
	keySize := e.Meta.keySize
	valueSize := e.Meta.valueSize
	bucketSize := e.Meta.bucketSize

	//set DataItemHeader buf
	buf := make([]byte, e.Size())
	buf = e.setEntryHeaderBuf(buf)
	//set bucket\key\value
	copy(buf[DataEntryHeaderSize:(DataEntryHeaderSize+bucketSize)], e.Meta.bucket)
	copy(buf[(DataEntryHeaderSize+bucketSize):(DataEntryHeaderSize+bucketSize+keySize)], e.Key)
	copy(buf[(DataEntryHeaderSize+bucketSize+keySize):(DataEntryHeaderSize+bucketSize+keySize+valueSize)], e.Value)

	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)

	return buf
}

// setEntryHeaderBuf sets the entry header buff.
func (e *Entry) setEntryHeaderBuf(buf []byte) []byte {
	binary.LittleEndian.PutUint64(buf[4:12], e.Meta.timestamp)
	binary.LittleEndian.PutUint32(buf[12:16], e.Meta.keySize)
	binary.LittleEndian.PutUint32(buf[16:20], e.Meta.valueSize)
	binary.LittleEndian.PutUint16(buf[20:22], e.Meta.Flag)
	binary.LittleEndian.PutUint32(buf[22:26], e.Meta.TTL)
	binary.LittleEndian.PutUint32(buf[26:30], e.Meta.bucketSize)
	binary.LittleEndian.PutUint16(buf[30:32], e.Meta.status)
	binary.LittleEndian.PutUint16(buf[32:34], e.Meta.ds)
	binary.LittleEndian.PutUint64(buf[34:42], e.Meta.txID)

	return buf
}

// IsZero checks if the entry is zero or not.
func (e *Entry) IsZero() bool {
	if e.crc == 0 && e.Meta.keySize == 0 && e.Meta.valueSize == 0 && e.Meta.timestamp == 0 {
		return true
	}
	return false
}

// GetCrc returns the crc at given buf slice.
func (e *Entry) GetCrc(buf []byte) uint32 {
	crc := crc32.ChecksumIEEE(buf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Meta.bucket)
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)

	return crc
}
