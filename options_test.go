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

func TestWithMaxBatchCount(t *testing.T) {
    InitOpt("", true)
    db, err := Open(
        opt,
        WithMaxBatchCount(10),
    )
    defer db.Close()
    assert.NoError(t, err)
    assert.Equal(t, int64(10), db.getMaxBatchCount())
}

func TestWithMaxBatchSize(t *testing.T) {
    InitOpt("", true)
    db, err := Open(
        opt,
        WithMaxBatchSize(100),
    )
    defer db.Close()
    assert.NoError(t, err)
    assert.Equal(t, int64(100), db.getMaxBatchSize())
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

func TestWithErrorHandler(t *testing.T) {
    db, err = Open(DefaultOptions,
        WithDir("/tmp/nutsdb"),
        WithErrorHandler(ErrorHandlerFunc(func(err error) {
        })),
    )
    assert.NoError(t, err)
    assert.NotNil(t, db.opt.ErrorHandler)

    defer db.Close()
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

    defer db.Close()
}
