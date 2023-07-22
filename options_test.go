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
}

func TestWithRWMode(t *testing.T) {
	db, err = Open(DefaultOptions,
		WithDir("/tmp/nutsdb"),
		WithRWMode(MMap),
	)
	assert.NoError(t, err)
	assert.Equal(t, db.opt.RWMode, MMap)
}
