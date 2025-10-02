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
	assert.NoError(t, err)
	assert.Equal(t, int64(1011), db.opt.NodeNum)
	err = db.Close()
	assert.NoError(t, err)
}

func TestWithMaxBatchCount(t *testing.T) {
	InitOpt("", true)
	db, err := Open(
		opt,
		WithMaxBatchCount(10),
	)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), db.getMaxBatchCount())
	err = db.Close()
	assert.NoError(t, err)
}

func TestWithMaxBatchSize(t *testing.T) {
	InitOpt("", true)
	db, err := Open(
		opt,
		WithMaxBatchSize(100),
	)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), db.getMaxBatchSize())
	err = db.Close()
	assert.NoError(t, err)
}

func TestWithHintKeyAndRAMIdxCacheSize(t *testing.T) {
	InitOpt("", true)
	db, err := Open(
		opt,
		WithHintKeyAndRAMIdxCacheSize(100),
	)
	assert.NoError(t, err)
	assert.Equal(t, 100, db.getHintKeyAndRAMIdxCacheSize())
	err = db.Close()
	assert.NoError(t, err)
}

func TestWithMaxWriteRecordCount(t *testing.T) {
	InitOpt("", true)
	db, err := Open(
		opt,
		WithMaxWriteRecordCount(100),
	)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), db.getMaxWriteRecordCount())
	err = db.Close()
	assert.NoError(t, err)
}

func TestWithRWMode(t *testing.T) {
	db, err = Open(DefaultOptions,
		WithDir("/tmp/nutsdb"),
		WithRWMode(MMap),
	)
	assert.NoError(t, err)
	assert.Equal(t, db.opt.RWMode, MMap)
	err = db.Close()
	assert.NoError(t, err)
}

func TestWithSyncEnable(t *testing.T) {
	db, err = Open(DefaultOptions,
		WithDir("/tmp/nutsdb"),
		WithSyncEnable(false),
	)

	assert.NoError(t, err)
	assert.False(t, db.opt.SyncEnable)

	err = db.Close()
	assert.NoError(t, err)
}

func TestWithMaxFdNumsInCache(t *testing.T) {
	db, err = Open(DefaultOptions,
		WithDir("/tmp/nutsdb"),
		WithMaxFdNumsInCache(100),
	)

	assert.NoError(t, err)
	assert.Equal(t, db.opt.MaxFdNumsInCache, 100)

	err = db.Close()
	assert.NoError(t, err)
}

func TestWithCleanFdsCacheThreshold(t *testing.T) {
	db, err = Open(DefaultOptions,
		WithDir("/tmp/nutsdb"),
		WithCleanFdsCacheThreshold(0.5),
	)

	assert.NoError(t, err)
	assert.Equal(t, db.opt.CleanFdsCacheThreshold, 0.5)

	err = db.Close()
	assert.NoError(t, err)
}

func TestWithErrorHandler(t *testing.T) {
	db, err = Open(DefaultOptions,
		WithDir("/tmp/nutsdb"),
		WithErrorHandler(ErrorHandlerFunc(func(err error) {
		})),
	)
	assert.NoError(t, err)
	assert.NotNil(t, db.opt.ErrorHandler)

	err = db.Close()
	assert.NoError(t, err)
}

func TestWithLessFunc(t *testing.T) {
	db, err = Open(DefaultOptions,
		WithDir("/tmp/nutsdb"),
		WithLessFunc(func(l, r string) bool {
			return len(l) < len(r)
		}),
	)
	assert.NoError(t, err)
	assert.NotNil(t, db.opt.LessFunc)

	err = db.Close()
	assert.NoError(t, err)
}

func TestWithEnableHintFile(t *testing.T) {
	// Test default value
	db, err := Open(DefaultOptions,
		WithDir("/tmp/nutsdb"),
	)
	assert.NoError(t, err)
	assert.True(t, db.opt.EnableHintFile)
	err = db.Close()
	assert.NoError(t, err)

	// Test setting to false
	db, err = Open(DefaultOptions,
		WithDir("/tmp/nutsdb"),
		WithEnableHintFile(false),
	)
	assert.NoError(t, err)
	assert.False(t, db.opt.EnableHintFile)
	err = db.Close()
	assert.NoError(t, err)

	// Test setting to true explicitly
	db, err = Open(DefaultOptions,
		WithDir("/tmp/nutsdb"),
		WithEnableHintFile(true),
	)
	assert.NoError(t, err)
	assert.True(t, db.opt.EnableHintFile)
	err = db.Close()
	assert.NoError(t, err)
}
