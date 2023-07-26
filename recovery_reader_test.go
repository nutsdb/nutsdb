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
		WithValueSize(uint32(len("val"))).WithTimeStamp(1547707905).WithTTL(Persistent).
		WithBucketSize(uint32(len("Test_readEntry"))).WithFlag(DataSetFlag)

	expect := NewEntry().WithKey([]byte("key")).WithMeta(meta).WithValue([]byte("val")).WithBucket([]byte("Test_readEntry"))

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
