package nutsdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBucket_DecodeAndDecode(t *testing.T) {
	bucket := &Bucket{
		Meta: &BucketMeta{
			Op: BucketInsertOperation,
		},
		Id:   1,
		Ds:   DataStructureBTree,
		Name: "bucket_1",
	}
	bytes := bucket.encode()

	bucketMeta := &BucketMeta{}
	bucketMeta.decode(bytes[:BucketMetaSize])
	assert.Equal(t, bucketMeta.Op, BucketInsertOperation)
	assert.Equal(t, int64(8+2+8), int64(bucketMeta.Size))
	decodeBucket := &Bucket{Meta: bucketMeta}

	err := decodeBucket.decode(bytes[BucketMetaSize:])
	assert.Nil(t, err)
	assert.Equal(t, BucketId(1), decodeBucket.Id)
	assert.Equal(t, decodeBucket.Name, "bucket_1")

	crc := decodeBucket.getCRC(bytes[:BucketMetaSize], bytes[BucketMetaSize:])
	assert.Equal(t, decodeBucket.Meta.Crc, crc)
}
