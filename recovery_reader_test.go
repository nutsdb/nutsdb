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

	expect := &Entry{
		Key:   []byte("key"),
		Value: []byte("val"),
		Meta: &MetaData{
			KeySize:    uint32(len("key")),
			ValueSize:  uint32(len("val")),
			Timestamp:  1547707905,
			TTL:        Persistent,
			Bucket:     []byte("Test_readEntry"),
			BucketSize: uint32(len("Test_readEntry")),
			Flag:       DataSetFlag,
		},
		position: 0,
	}
	_, err = fd.Write(expect.Encode())
	require.NoError(t, err)

	f, err := newFileRecovery(path, 4096)
	require.NoError(t, err)

	get, err := f.readEntry()
	require.NoError(t, err)

	assert.Equal(t, expect.Encode(), get.Encode())

	err = fd.Close()
	require.NoError(t, err)

}
