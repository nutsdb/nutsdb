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
	BucketInsertOperation BucketOperation = 1
	BucketUpdateOperation BucketOperation = 2
	BucketDeleteOperation BucketOperation = 3
)

type BucketStatus = uint8

const BucketStatusExistAlready = 1
const BucketStatusDelete = 2
const BucketStatusNew = 3
const BucketStatusUpdated = 4
const BucketStatusUnknown = 4

var ErrBucketCrcInvalid = errors.New("bucket crc invalid")

func init() {
	BucketMetaSize = getDiskSizeFromSingleObject(BucketMeta{})
}

// BucketMeta stores the Meta info of a Bucket. E.g. the size of bucket it store in disk.
type BucketMeta struct {
	Crc uint32
	// Op: Mark the latest operation (e.g. delete, insert, update) for this bucket.
	Op BucketOperation
	// Size: the size of payload.
	Size uint32
}

// Bucket is the disk structure of bucket
type Bucket struct {
	// Meta: the metadata for this bucket
	Meta *BucketMeta
	// Id: is the marker for this bucket, every bucket creation activity will generate a new Id for it.
	// for example. If you have a bucket called "bucket_1", and you just delete bucket and create it again.
	// the last bucket will have a different Id from the previous one.
	Id BucketId
	// Ds: the data structure for this bucket. (List, Set, SortSet, String)
	Ds Ds
	// Name: the name of this bucket.
	Name string
}

// decode : CRC | op | size
func (meta *BucketMeta) decode(bytes []byte) {
	_ = bytes[BucketMetaSize-1]
	crc := binary.LittleEndian.Uint32(bytes[:4])
	op := binary.LittleEndian.Uint16(bytes[4:6])
	size := binary.LittleEndian.Uint32(bytes[6:10])
	meta.Crc = crc
	meta.Size = size
	meta.Op = BucketOperation(op)
}

// encode : Meta | BucketId | Ds | BucketName
func (b *Bucket) encode() []byte {
	entrySize := b.getEntrySize()
	buf := make([]byte, entrySize)
	b.Meta.Size = uint32(b.getPayloadSize())
	binary.LittleEndian.PutUint16(buf[4:6], uint16(b.Meta.Op))
	binary.LittleEndian.PutUint32(buf[6:10], b.Meta.Size)
	binary.LittleEndian.PutUint64(buf[BucketMetaSize:BucketMetaSize+IdSize], uint64(b.Id))
	binary.LittleEndian.PutUint16(buf[BucketMetaSize+IdSize:BucketMetaSize+IdSize+DsSize], uint16(b.Ds))
	copy(buf[BucketMetaSize+IdSize+DsSize:], b.Name)
	c32 := crc32.ChecksumIEEE(buf[4:])
	b.Meta.Crc = c32
	binary.LittleEndian.PutUint32(buf[0:4], c32)

	return buf
}

// decode : Meta | BucketId | Ds | BucketName
func (b *Bucket) decode(bytes []byte) error {
	// parse the payload
	id := binary.LittleEndian.Uint64(bytes[:IdSize])
	ds := binary.LittleEndian.Uint16(bytes[IdSize : IdSize+DsSize])
	name := bytes[IdSize+DsSize:]
	b.Id = id
	b.Name = string(name)
	b.Ds = ds
	return nil
}

func (b *Bucket) getEntrySize() int {
	return int(BucketMetaSize) + b.getPayloadSize()
}

func (b *Bucket) getCRC(headerBuf []byte, dataBuf []byte) uint32 {
	crc := crc32.ChecksumIEEE(headerBuf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, dataBuf)
	return crc
}

func (b *Bucket) getPayloadSize() int {
	return IdSize + DsSize + len(b.Name)
}
