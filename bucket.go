package nutsdb

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var BucketMetaSize int64

const (
	IdSize = 8
	DsSize = 2
)

type BucketOperation uint16

const (
	BucketInsertOperation = 1
	BucketUpdateOperation = 2
	BucketDeleteOperation = 3
)

var ErrBucketCrcInvalid = errors.New("bucket crc invalid")

func init() {
	BucketMetaSize = GetDiskSizeFromSingleObject(BucketMeta{})
}

// BucketMeta stores the meta info of a Bucket. E.g. the size of bucket it store in disk.
type BucketMeta struct {
	Crc  uint32
	Size uint32
	Op   BucketOperation
}

// Bucket is the disk structure of bucket
type Bucket struct {
	meta *BucketMeta
	Id   uint64
	Ds   Ds
	Name string
}

func (meta *BucketMeta) Decode(bytes []byte) {
	_ = bytes[BucketMetaSize]
	crc := binary.LittleEndian.Uint32(bytes[:4])
	size := binary.LittleEndian.Uint32(bytes[4:8])
	op := binary.LittleEndian.Uint16(bytes[8:10])
	meta.Crc = crc
	meta.Size = size
	meta.Op = BucketOperation(op)
}

func (b *Bucket) Encode() []byte {
	entrySize := b.GetEntrySize()
	buf := make([]byte, entrySize)
	binary.LittleEndian.PutUint64(buf[BucketMetaSize:BucketMetaSize+IdSize], b.Id)
	binary.LittleEndian.PutUint16(buf[BucketMetaSize+IdSize:BucketMetaSize+IdSize+DsSize], uint16(b.Ds))
	copy(buf[BucketMetaSize+IdSize:], b.Name)
	c32 := crc32.ChecksumIEEE(buf[4:])
	b.meta.Crc = c32
	return buf
}

func (b *Bucket) Decode(bytes []byte) error {
	crc := b.meta.Crc
	crcInDisk := crc32.ChecksumIEEE(bytes[4:])
	if crc != crcInDisk {
		return ErrBucketCrcInvalid
	}

	// parse the payload
	id := binary.LittleEndian.Uint64(bytes[BucketMetaSize : BucketMetaSize+IdSize])
	ds := binary.LittleEndian.Uint16(bytes[BucketMetaSize+IdSize : BucketMetaSize+IdSize+DsSize])
	name := bytes[BucketMetaSize+IdSize+DsSize:]
	b.Id = id
	b.Name = string(name)
	b.Ds = Ds(ds)
	return nil
}

func (b *Bucket) GetEntrySize() int {
	return int(BucketMetaSize) + b.GetPayloadSize()
}

func (b *Bucket) GetPayloadSize() int {
	return IdSize + DsSize + len(b.Name)
}
