package core_test

import (
	"testing"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/xujiajun/utils/strconv2"
)

func Test_EntryCreation(t *testing.T) {
	k := []byte("k")
	v := []byte("v")
	ttl := 12
	txId := 121212
	entry := core.NewEntry().
		WithKey(k).
		WithValue(v).
		WithMeta(
			core.NewMetaData().
				WithTTL(uint32(ttl)).
				WithTxID(uint64(txId)).
				WithDs(core.DataStructureBTree),
		)

	assert.Equal(
		t,
		[]byte(strconv2.Int64ToStr(int64(txId))),
		entry.GetTxIDBytes())
	assert.Equal(
		t,
		uint32(ttl),
		entry.Meta.TTL,
	)
	assert.Equal(t, entry.Key, k)
	assert.Equal(t, entry.Value, v)
	assert.True(t, entry.IsBelongsToBTree())
}
