package nutsdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
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
