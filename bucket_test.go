package nutsdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBucket_DecodeAndDecode(t *testing.T) {
	bucket1 := &bucket{
		Meta: &bucketMeta{
			Op: bucketInsertOperation,
		},
		Id:   1,
		Ds:   DataStructureBTree,
		Name: "bucket_1",
	}
	bytes := bucket1.encode()

	bucketMeta := &bucketMeta{}
	bucketMeta.decode(bytes[:bucketMetaSize])
	assert.Equal(t, bucketMeta.Op, bucketInsertOperation)
	assert.Equal(t, int64(8+2+8), int64(bucketMeta.Size))
	decodeBucket := &bucket{Meta: bucketMeta}

	err := decodeBucket.decode(bytes[bucketMetaSize:])
	assert.Nil(t, err)
	assert.Equal(t, bucketId(1), decodeBucket.Id)
	assert.Equal(t, decodeBucket.Name, "bucket_1")

	crc := decodeBucket.getCRC(bytes[:bucketMetaSize], bytes[bucketMetaSize:])
	assert.Equal(t, decodeBucket.Meta.Crc, crc)
}
