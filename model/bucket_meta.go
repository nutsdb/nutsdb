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
	"github.com/xujiajun/nutsdb/errs"
	"hash/crc32"
	"os"
	"path/filepath"
)

// BucketMeta represents the bucket's meta-information.
type BucketMeta struct {
	StartSize uint32
	EndSize   uint32
	Start     []byte
	End       []byte
	Crc       uint32
}

// Encode returns the slice after the BucketMeta be encoded.
func (bm *BucketMeta) Encode() []byte {
	buf := make([]byte, bm.Size())

	binary.LittleEndian.PutUint32(buf[4:8], bm.StartSize)
	binary.LittleEndian.PutUint32(buf[8:12], bm.EndSize)

	startBuf := buf[consts.BucketMetaHeaderSize:(consts.BucketMetaHeaderSize + bm.StartSize)]
	copy(startBuf, bm.Start)
	endBuf := buf[consts.BucketMetaHeaderSize+bm.StartSize : (consts.BucketMetaHeaderSize + bm.StartSize + bm.EndSize)]
	copy(endBuf, bm.End)
	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)

	return buf
}

// GetCrc returns the crc at given buf slice.
func (bm *BucketMeta) GetCrc(buf []byte) uint32 {
	crc := crc32.ChecksumIEEE(buf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, bm.Start)
	crc = crc32.Update(crc, crc32.IEEETable, bm.End)

	return crc
}

// Size returns the size of the BucketMeta.
func (bm *BucketMeta) Size() int64 {
	return int64(consts.BucketMetaHeaderSize + bm.StartSize + bm.EndSize)
}

// ReadBucketMeta returns bucketMeta at given file path name.
func ReadBucketMeta(name string) (bucketMeta *BucketMeta, err error) {
	var off int64
	fd, err := os.OpenFile(filepath.Clean(name), os.O_RDWR, os.ModePerm)
	if err != nil {
		return
	}
	defer fd.Close()

	buf := make([]byte, consts.BucketMetaHeaderSize)
	_, err = fd.ReadAt(buf, off)
	if err != nil {
		return
	}
	startSize := binary.LittleEndian.Uint32(buf[4:8])
	endSize := binary.LittleEndian.Uint32(buf[8:12])
	bucketMeta = &BucketMeta{
		StartSize: startSize,
		EndSize:   endSize,
		Crc:       binary.LittleEndian.Uint32(buf[0:4]),
	}

	// read start
	off += consts.BucketMetaHeaderSize
	startBuf := make([]byte, startSize)
	if _, err = fd.ReadAt(startBuf, off); err != nil {
		return nil, err
	}
	bucketMeta.Start = startBuf

	// read end
	off += int64(startSize)
	endBuf := make([]byte, endSize)
	if _, err = fd.ReadAt(endBuf, off); err != nil {
		return nil, err
	}
	bucketMeta.End = endBuf

	if bucketMeta.GetCrc(buf) != bucketMeta.Crc {
		return nil, errs.ErrCrc
	}
	return
}
