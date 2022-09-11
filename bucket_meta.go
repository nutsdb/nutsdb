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
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	// BucketMetaHeaderSize returns the header size of the BucketMeta.
	BucketMetaHeaderSize = 12

	// BucketMetaSuffix returns b+ tree index suffix.
	BucketMetaSuffix = ".meta"
)

// BucketMeta represents the bucket's meta-information.
type BucketMeta struct {
	startSize uint32
	endSize   uint32
	start     []byte
	end       []byte
	crc       uint32
}

// Encode returns the slice after the BucketMeta be encoded.
func (bm *BucketMeta) Encode() []byte {
	buf := make([]byte, bm.Size())

	binary.LittleEndian.PutUint32(buf[4:8], bm.startSize)
	binary.LittleEndian.PutUint32(buf[8:12], bm.endSize)

	startBuf := buf[BucketMetaHeaderSize:(BucketMetaHeaderSize + bm.startSize)]
	copy(startBuf, bm.start)
	endBuf := buf[BucketMetaHeaderSize+bm.startSize : (BucketMetaHeaderSize + bm.startSize + bm.endSize)]
	copy(endBuf, bm.end)
	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)

	return buf
}

// GetCrc returns the crc at given buf slice.
func (bm *BucketMeta) GetCrc(buf []byte) uint32 {
	crc := crc32.ChecksumIEEE(buf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, bm.start)
	crc = crc32.Update(crc, crc32.IEEETable, bm.end)

	return crc
}

// Size returns the size of the BucketMeta.
func (bm *BucketMeta) Size() int64 {
	return int64(BucketMetaHeaderSize + bm.startSize + bm.endSize)
}

// ReadBucketMeta returns bucketMeta at given file path name.
func ReadBucketMeta(name string) (bucketMeta *BucketMeta, err error) {
	var off int64
	fd, err := os.OpenFile(filepath.Clean(name), os.O_RDWR, os.ModePerm)
	if err != nil {
		return
	}
	defer fd.Close()

	buf := make([]byte, BucketMetaHeaderSize)
	_, err = fd.ReadAt(buf, off)
	if err != nil {
		return
	}
	startSize := binary.LittleEndian.Uint32(buf[4:8])
	endSize := binary.LittleEndian.Uint32(buf[8:12])
	bucketMeta = &BucketMeta{
		startSize: startSize,
		endSize:   endSize,
		crc:       binary.LittleEndian.Uint32(buf[0:4]),
	}

	// read start
	off += BucketMetaHeaderSize
	startBuf := make([]byte, startSize)
	if _, err = fd.ReadAt(startBuf, off); err != nil {
		return nil, err
	}
	bucketMeta.start = startBuf

	// read end
	off += int64(startSize)
	endBuf := make([]byte, endSize)
	if _, err = fd.ReadAt(endBuf, off); err != nil {
		return nil, err
	}
	bucketMeta.end = endBuf

	if bucketMeta.GetCrc(buf) != bucketMeta.crc {
		return nil, ErrCrc
	}
	return
}
