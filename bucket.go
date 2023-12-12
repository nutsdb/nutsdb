package nutsdb

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var bucketMetaSize int64

const (
	idSize = 8
	dsSize = 2
)

type bucketOperation uint16

const (
	bucketInsertOperation bucketOperation = 1
	bucketUpdateOperation bucketOperation = 2
	bucketDeleteOperation bucketOperation = 3
)

type bucketStatus = uint8

const bucketStatusExistAlready = 1
const bucketStatusDelete = 2
const bucketStatusNew = 3
const bucketStatusUpdated = 4
const bucketStatusUnknown = 4

var ErrBucketCrcInvalid = errors.New("bucket crc invalid")

func init() {
	bucketMetaSize = getDiskSizeFromSingleObject(bucketMeta{})
}

// bucketMeta stores the Meta info of a bucket. E.g. the size of bucket it store in disk.
type bucketMeta struct {
	Crc uint32
	// Op: Mark the latest operation (e.g. delete, insert, update) for this bucket.
	Op bucketOperation
	// Size: the size of payload.
	Size uint32
}

// bucket is the disk structure of bucket
type bucket struct {
	// Meta: the metadata for this bucket
	Meta *bucketMeta
	// Id: is the marker for this bucket, every bucket creation activity will generate a new Id for it.
	// for example. If you have a bucket called "bucket_1", and you just delete bucket and create it again.
	// the last bucket will have a different Id from the previous one.
	Id bucketId
	// Ds: the data structure for this bucket. (list, set, SortSet, String)
	Ds ds
	// Name: the name of this bucket.
	Name string
}

// decode : CRC | op | size
func (meta *bucketMeta) decode(bytes []byte) {
	_ = bytes[bucketMetaSize-1]
	crc := binary.LittleEndian.Uint32(bytes[:4])
	op := binary.LittleEndian.Uint16(bytes[4:6])
	size := binary.LittleEndian.Uint32(bytes[6:10])
	meta.Crc = crc
	meta.Size = size
	meta.Op = bucketOperation(op)
}

// encode : Meta | bucketId | ds | bucketName
func (b *bucket) encode() []byte {
	entrySize := b.getEntrySize()
	buf := make([]byte, entrySize)
	b.Meta.Size = uint32(b.getPayloadSize())
	binary.LittleEndian.PutUint16(buf[4:6], uint16(b.Meta.Op))
	binary.LittleEndian.PutUint32(buf[6:10], b.Meta.Size)
	binary.LittleEndian.PutUint64(buf[bucketMetaSize:bucketMetaSize+idSize], uint64(b.Id))
	binary.LittleEndian.PutUint16(buf[bucketMetaSize+idSize:bucketMetaSize+idSize+dsSize], uint16(b.Ds))
	copy(buf[bucketMetaSize+idSize+dsSize:], b.Name)
	c32 := crc32.ChecksumIEEE(buf[4:])
	b.Meta.Crc = c32
	binary.LittleEndian.PutUint32(buf[0:4], c32)

	return buf
}

// decode : Meta | bucketId | ds | bucketName
func (b *bucket) decode(bytes []byte) error {
	// parse the payload
	id := binary.LittleEndian.Uint64(bytes[:idSize])
	ds := binary.LittleEndian.Uint16(bytes[idSize : idSize+dsSize])
	name := bytes[idSize+dsSize:]
	b.Id = id
	b.Name = string(name)
	b.Ds = ds
	return nil
}

func (b *bucket) getEntrySize() int {
	return int(bucketMetaSize) + b.getPayloadSize()
}

func (b *bucket) getCRC(headerBuf []byte, dataBuf []byte) uint32 {
	crc := crc32.ChecksumIEEE(headerBuf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, dataBuf)
	return crc
}

func (b *bucket) getPayloadSize() int {
	return idSize + dsSize + len(b.Name)
}
