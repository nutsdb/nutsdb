package fileio_test

import (
	"path"
	"testing"

	"github.com/nutsdb/nutsdb"
	"github.com/nutsdb/nutsdb/internal/fileio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRWManager_FileIO_All(t *testing.T) {
	filePath := path.Join(t.TempDir(), "foo_rw_fileio")
	maxFdNums := 20
	cleanThreshold := 0.5
	var fdm *fileio.FdManager

	t.Run("test write read", func(t *testing.T) {
		fdm = fileio.NewFdm(maxFdNums, cleanThreshold)
		fd, err := fdm.GetFd(filePath)
		if err != nil {
			require.NoError(t, err)
		}

		rwManager := &fileio.FileIORWManager{fd, filePath, fdm, 256 * nutsdb.MB}
		b := []byte("hello")
		off := int64(3)
		_, err = rwManager.WriteAt(b, off)
		if err != nil {
			require.NoError(t, err)
		}

		bucketBufLen := len(b)
		bucketBuf := make([]byte, bucketBufLen)
		n, err := rwManager.ReadAt(bucketBuf, off)
		if err != nil {
			require.NoError(t, err)
		}

		assert.Equal(t, bucketBufLen, n)
		assert.Equal(t, b, bucketBuf)
	})
}
