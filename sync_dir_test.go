package nutsdb

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRotateActiveFileWithSyncEnabled(t *testing.T) {
	opt := DefaultOptions
	opt.Dir = t.TempDir()
	opt.SyncEnable = true

	db, err := Open(opt)
	require.NoError(t, err)
	defer db.Close()

	oldFileID := db.ActiveFile.fileID
	require.NoError(t, db.rotateActiveFile())
	require.Greater(t, db.ActiveFile.fileID, oldFileID)
}

func TestSyncDirReturnsOpenError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows persists directory entries without opening or syncing the directory")
	}

	err := syncDir(t.TempDir() + "/missing")
	require.Error(t, err)
}
