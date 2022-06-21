package nutsdb

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRWManager_FileIO_All(t *testing.T) {

	filePath := "/tmp/foo_rw_fileio"
	maxFdNums := 20
	cleanThreshold := 0.5
	var fdm *fdManager

	t.Run("test write read", func(t *testing.T) {
		fdm = newFdm(maxFdNums, cleanThreshold)
		fd, err := fdm.getFd(filePath)
		if err != nil {
			require.NoError(t, err)
		}

		rwManager := &FileIORWManager{fd, filePath, fdm}
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
