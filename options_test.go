package nutsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithNodeNum(t *testing.T) {
	InitOpt("", true)
	db, err := Open(
		opt,
		WithNodeNum(1011),
	)
	defer db.Close()
	assert.NoError(t, err)
	assert.Equal(t, int64(1011), db.opt.NodeNum)
}

func TestWithRWMode(t *testing.T) {
	db, err = Open(DefaultOptions,
		WithDir("/tmp/nutsdb"),
		WithRWMode(MMap),
	)
	defer db.Close()
	assert.NoError(t, err)
	assert.Equal(t, db.opt.RWMode, MMap)
}

func TestWithSyncEnable(t *testing.T) {
	db, err = Open(DefaultOptions,
		WithDir("/tmp/nutsdb"),
		WithSyncEnable(false),
	)

	assert.NoError(t, err)
	assert.False(t, db.opt.SyncEnable)

	defer db.Close()
}

func TestWithMaxFdNumsInCache(t *testing.T) {
	db, err = Open(DefaultOptions,
		WithDir("/tmp/nutsdb"),
		WithMaxFdNumsInCache(100),
	)

	assert.NoError(t, err)
	assert.Equal(t, db.opt.MaxFdNumsInCache, 100)

	defer db.Close()
}

func TestWithCleanFdsCacheThreshold(t *testing.T) {
	db, err = Open(DefaultOptions,
		WithDir("/tmp/nutsdb"),
		WithCleanFdsCacheThreshold(0.5),
	)

	assert.NoError(t, err)
	assert.Equal(t, db.opt.CleanFdsCacheThreshold, 0.5)

	defer db.Close()
}
