package nutsdb

import (
	"errors"
	"os"
	"testing"

	"github.com/nutsdb/nutsdb/internal/fileio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failSyncRWManager wraps a RWManager and fails Sync while failSync is set.
type failSyncRWManager struct {
	fileio.RWManager
	failSync bool
}

func (m *failSyncRWManager) Sync() error {
	if m.failSync {
		return errors.New("injected fsync failure")
	}
	return m.RWManager.Sync()
}

// A commit whose fsync fails is not acknowledged, so the active file's offsets
// must be rewound; otherwise the next commit leaves a hole in the log that
// recovery treats as the end of the file.
func TestTx_FailedSyncRewindsActiveFileOffset(t *testing.T) {
	dir, err := os.MkdirTemp("", "nutsdb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := DefaultOptions
	opt.Dir = dir
	opt.SegmentSize = 8 * 1024

	db, err := Open(opt)
	require.NoError(t, err)

	txCreateBucket(t, db, DataStructureBTree, "bucket1", nil)
	txPut(t, db, "bucket1", []byte("k1"), []byte("v1"), Persistent, nil, nil)

	sizeBefore := db.ActiveFile.ActualSize
	wrapper := &failSyncRWManager{RWManager: db.ActiveFile.rwManager, failSync: true}
	db.ActiveFile.rwManager = wrapper

	err = db.Update(func(tx *Tx) error {
		return tx.Put("bucket1", []byte("k2"), []byte("v2"), Persistent)
	})
	require.Error(t, err)

	assert.Equal(t, sizeBefore, db.ActiveFile.ActualSize,
		"ActualSize after failed fsync = %d, want %d", db.ActiveFile.ActualSize, sizeBefore)
	assert.Equal(t, sizeBefore, db.ActiveFile.writeOff,
		"writeOff after failed fsync = %d, want %d", db.ActiveFile.writeOff, sizeBefore)

	wrapper.failSync = false
	txPut(t, db, "bucket1", []byte("k3"), []byte("v3"), Persistent, nil, nil)
	txGet(t, db, "bucket1", []byte("k3"), []byte("v3"), nil)
	require.NoError(t, db.Close())

	// Reopen: recovery must see k1 and k3 and must not stop at a hole.
	db, err = Open(opt)
	require.NoError(t, err)
	defer db.Close()

	txGet(t, db, "bucket1", []byte("k1"), []byte("v1"), nil)
	txGet(t, db, "bucket1", []byte("k3"), []byte("v3"), nil)
	txGet(t, db, "bucket1", []byte("k2"), nil, ErrNotFoundKey)
}
