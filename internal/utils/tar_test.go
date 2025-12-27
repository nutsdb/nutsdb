package utils_test

import (
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/nutsdb/nutsdb/internal/utils"
	"github.com/stretchr/testify/require"
)

func TestTarGZCompress(t *testing.T) {
	backupFile := filepath.Join(t.TempDir(), "backupFile.tar.gz")
	realPath := filepath.Join(t.TempDir(), "x", "realpath.txt")
	_ = os.MkdirAll(path.Dir(realPath), os.ModePerm)
	f, err := os.Create(realPath)
	require.NoError(t, err)
	f.Close()
	f, err = os.Create(backupFile)
	require.NoError(t, err)
	require.NoError(t, utils.TarCompress(f, realPath))
	defer f.Close()
}
