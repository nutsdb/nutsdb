package nutsdb

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func Test_readEntry(t *testing.T) {
	path := "/tmp/test_read_entry"

	fd, err := os.OpenFile(path, os.O_TRUNC|os.O_CREATE|os.O_RDWR, os.ModePerm)
	require.NoError(t, err)
	meta := NewMetaData().WithKeySize(uint32(len("key"))).
		WithValueSize(uint32(len("val"))).WithTimeStamp(1547707905).
		WithTTL(Persistent).WithFlag(DataSetFlag).WithBucketId(1)

	expect := NewEntry().WithKey([]byte("key")).WithMeta(meta).WithValue([]byte("val"))

	_, err = fd.Write(expect.Encode())
	require.NoError(t, err)

	f, err := newFileRecovery(path, 4096)
	require.NoError(t, err)

	entry, err := f.readEntry(0)
	require.NoError(t, err)

	assert.Equal(t, expect.Encode(), entry.Encode())

	err = fd.Close()
	require.NoError(t, err)

}

func Test_fileRecovery_readBucket(t *testing.T) {
	filePath := "bucket_test_data"
	bucket := &Bucket{
		Meta: &BucketMeta{
			Op: BucketInsertOperation,
		},
		Id:   1,
		Ds:   Ds(DataStructureBTree),
		Name: "bucket_1",
	}
	bytes := bucket.Encode()

	fd, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, os.ModePerm)
	defer func() {
		err = fd.Close()
		assert.Nil(t, err)
		err = os.Remove(filePath)
		assert.Nil(t, nil)
	}()
	assert.Nil(t, err)
	_, err = fd.Write(bytes)
	assert.Nil(t, err)

	fr, err := newFileRecovery(filePath, 4*MB)
	assert.Nil(t, err)
	readBucket, err := fr.readBucket()
	assert.Nil(t, err)
	assert.Equal(t, readBucket.Meta.Op, BucketInsertOperation)
	assert.Equal(t, int64(8+2+8), int64(readBucket.Meta.Size))
	assert.Equal(t, BucketId(1), readBucket.Id)
	assert.Equal(t, readBucket.Name, "bucket_1")
}
