// Copyright 2019 The nutsdb Author. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core_test

import (
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				WithDs(core.DataStructureBTree).
				WithKeySize(1).WithValueSize(1),
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
	assert.False(t, entry.IsZero())
}

func Test_EntryMostFunctions(t *testing.T) {
	r := require.New(t)
	k := []byte("key")
	v := []byte("value")
	ttl := 12
	txId := 121212
	bucketId := uint64(9)
	entry := core.NewEntry().
		WithKey(k).
		WithValue(v).
		WithMeta(
			core.NewMetaData().
				WithTTL(uint32(ttl)).
				WithTxID(uint64(txId)).
				WithDs(core.DataStructureBTree).
				WithBucketId(bucketId).
				WithKeySize(3).
				WithValueSize(5).
				WithFlag(core.DataBTreeBucketDeleteFlag),
		)

	r.Equal(entry.Key, k)
	data := entry.Encode()
	// test encode
	checkFuncs := []func(buf []byte) []byte{
		func(buf []byte) []byte {
			// crc32 check
			t.Log("crc32 check")
			length := 4
			crc := binary.LittleEndian.Uint32(buf[0:length])
			r.Equal(crc, crc32.ChecksumIEEE(buf[length:]))
			return buf[length:]
		},
		func(buf []byte) []byte {
			// timestamp check
			t.Log("timestamp check")
			ts, length := binary.Uvarint(buf[0:])
			r.Equal(entry.Meta.Timestamp, ts)
			return buf[length:]
		},
		func(buf []byte) []byte {
			// key size check
			t.Log("keySize check")
			ks, length := binary.Uvarint(buf[0:])
			r.Equal(uint64(entry.Meta.KeySize), ks)
			return buf[length:]
		},
		func(buf []byte) []byte {
			t.Log("valueSize check")
			vs, length := binary.Uvarint(buf[0:])
			r.Equal(uint64(entry.Meta.ValueSize), vs)
			return buf[length:]
		},
		func(buf []byte) []byte {
			t.Log("Flag check")
			flag, length := binary.Uvarint(buf[0:])
			r.Equal(uint64(entry.Meta.Flag), flag)
			return buf[length:]
		},
		func(buf []byte) []byte {
			t.Log("ttl check")
			ttl, length := binary.Uvarint(buf[0:])
			r.Equal(uint64(entry.Meta.TTL), ttl)
			return buf[length:]
		},
		func(buf []byte) []byte {
			t.Log("status check")
			status, length := binary.Uvarint(buf[0:])
			r.Equal(uint64(entry.Meta.Status), status)
			return buf[length:]
		},
		func(buf []byte) []byte {
			t.Log("ds check")
			ds, length := binary.Uvarint(buf[0:])
			r.Equal(uint64(entry.Meta.Ds), ds)
			return buf[length:]
		},
		func(buf []byte) []byte {
			t.Log("txId check")
			txId, length := binary.Uvarint(buf[0:])
			r.Equal(uint64(entry.Meta.TxID), txId)
			return buf[length:]
		},
		func(buf []byte) []byte {
			t.Log("bucketId check")
			txId, length := binary.Uvarint(buf[0:])
			r.Equal(uint64(entry.Meta.BucketId), txId)
			return buf[length:]
		},
	}

	for _, f := range checkFuncs {
		data = f(data)
	}
	r.Equal("keyvalue", string(data))

	// test GetRawKey
	key, err := entry.GetRawKey()
	r.Nil(err)
	r.Equal(k, key)

	// test is belong to btree
	r.True(entry.IsBelongsToBTree())

	// test Entry valid
	r.Nil(entry.Valid())

	// test isfilter
	r.False(entry.IsFilter())
}

func Test_EntrySizeAndIsZero(t *testing.T) {
	meta := core.NewMetaData().
		WithKeySize(3).
		WithValueSize(5)
	e := core.NewEntry().
		WithKey([]byte("key")).
		WithValue([]byte("value")).
		WithMeta(meta)

	// Size = meta.Size() + keySize + valueSize
	expectedSize := meta.Size() + int64(meta.KeySize+meta.ValueSize)
	assert.Equal(t, expectedSize, e.Size())

	// Zero entry.
	zeroEntry := core.NewEntry().WithMeta(core.NewMetaData())
	assert.True(t, zeroEntry.IsZero())
}

func Test_EntryGetCrc(t *testing.T) {
	k := []byte("key")
	v := []byte("value")

	entry := core.NewEntry().
		WithKey(k).
		WithValue(v).
		WithMeta(
			core.NewMetaData().
				WithKeySize(uint32(len(k))).
				WithValueSize(uint32(len(v))),
		)

	encoded := entry.Encode()

	// Build header buffer as it would be when computing crc before payload is written.
	headerLen := int(entry.Meta.Size())
	headerBuf := make([]byte, headerLen)
	copy(headerBuf, encoded[:headerLen])

	// Use a fresh entry, GetCrc should match crc in encoded data.
	e2 := core.NewEntry().
		WithKey(k).
		WithValue(v)
	crcFromMethod := e2.GetCrc(headerBuf)
	crcEncoded := binary.LittleEndian.Uint32(encoded[:4])

	assert.Equal(t, crcEncoded, crcFromMethod)
}

func Test_EntryParsePayloadAndCheckPayloadSize(t *testing.T) {
	r := require.New(t)

	k := []byte("abc")
	v := []byte("defgh")

	meta := core.NewMetaData().
		WithKeySize(uint32(len(k))).
		WithValueSize(uint32(len(v))).
		WithBucketSize(2)

	e := core.NewEntry().WithMeta(meta)
	payload := append(k, v...)

	// ParsePayload should split key/value according to meta sizes.
	r.NoError(e.ParsePayload(payload))
	r.Equal(k, e.Key)
	r.Equal(v, e.Value)

	// CheckPayloadSize ok.
	payloadSize := meta.PayloadSize()
	r.NoError(e.CheckPayloadSize(payloadSize))

	// CheckPayloadSize mismatch.
	err := e.CheckPayloadSize(payloadSize + 1)
	r.Error(err)
	r.Equal(core.ErrPayLoadSizeMismatch, err)
}

func Test_EntryParseMeta(t *testing.T) {
	r := require.New(t)

	k := []byte("k")
	v := []byte("v")

	orig := core.NewEntry().
		WithKey(k).
		WithValue(v).
		WithMeta(
			core.NewMetaData().
				WithKeySize(uint32(len(k))).
				WithValueSize(uint32(len(v))).
				WithTTL(123).
				WithFlag(core.DataSetFlag).
				WithStatus(core.Committed).
				WithDs(core.DataStructureList).
				WithTxID(42).
				WithBucketId(7),
		)
	e := core.NewEntry()
	n, err := e.ParseMeta(orig.Encode())
	r.NoError(err)
	r.True(n > 0)

	// Meta should match original entry's meta.
	r.Equal(orig.Meta.Timestamp, e.Meta.Timestamp)
	r.Equal(orig.Meta.KeySize, e.Meta.KeySize)
	r.Equal(orig.Meta.ValueSize, e.Meta.ValueSize)
	r.Equal(orig.Meta.Flag, e.Meta.Flag)
	r.Equal(orig.Meta.TTL, e.Meta.TTL)
	r.Equal(orig.Meta.Status, e.Meta.Status)
	r.Equal(orig.Meta.Ds, e.Meta.Ds)
	r.Equal(orig.Meta.TxID, e.Meta.TxID)
	r.Equal(orig.Meta.BucketId, e.Meta.BucketId)

	// Too small header.
	short := make([]byte, core.MinEntryHeaderSize-1)
	e2 := core.NewEntry()
	n, err = e2.ParseMeta(short)
	r.Equal(int64(0), n)
	r.Equal(core.ErrHeaderSizeOutOfBounds, err)
}

func Test_EntryIsFilterFlags(t *testing.T) {
	tests := []struct {
		name string
		flag core.DataFlag
	}{
		{"delete", core.DataDeleteFlag},
		{"rpop", core.DataRPopFlag},
		{"lpop", core.DataLPopFlag},
		{"lrem", core.DataLRemFlag},
		{"ltrim", core.DataLTrimFlag},
		{"zrem", core.DataZRemFlag},
		{"zremRangeByRank", core.DataZRemRangeByRankFlag},
		{"zpopMax", core.DataZPopMaxFlag},
		{"zpopMin", core.DataZPopMinFlag},
		{"lremByIndex", core.DataLRemByIndex},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := core.NewEntry().WithMeta(core.NewMetaData().WithFlag(uint16(tc.flag)))
			assert.True(t, e.IsFilter())
		})
	}
}

func Test_EntryValidErrors(t *testing.T) {
	// Empty key.
	e := core.NewEntry().
		WithKey(nil).
		WithValue([]byte("v"))

	err := e.Valid()
	assert.Equal(t, core.ErrKeyEmpty, err)
}

func Test_EntryBelongsToHelpers(t *testing.T) {
	setEntry := core.NewEntry().WithMeta(
		core.NewMetaData().WithDs(uint16(core.DataStructureSet)),
	)
	assert.True(t, setEntry.IsBelongsToSet())

	listEntry := core.NewEntry().WithMeta(
		core.NewMetaData().WithDs(uint16(core.DataStructureList)),
	)
	assert.True(t, listEntry.IsBelongsToList())

	zsetEntry := core.NewEntry().WithMeta(
		core.NewMetaData().WithDs(uint16(core.DataStructureSortedSet)),
	)
	assert.True(t, zsetEntry.IsBelongsToSortSet())

	// B+Tree helper is alias of BTree.
	btreeEntry := core.NewEntry().WithMeta(
		core.NewMetaData().WithDs(uint16(core.DataStructureBTree)),
	)
	assert.True(t, btreeEntry.IsBelongsToBPlusTree())
}

func Test_DataInTxMethods(t *testing.T) {
	dt := &core.DataInTx{
		Es:       []*core.EntryWhenRecovery{},
		TxId:     100,
		StartOff: 10,
	}

	e1 := &core.EntryWhenRecovery{
		Fid: 1,
		Off: 20,
	}
	e1.Meta = core.NewMetaData().WithTxID(100)

	e2 := &core.EntryWhenRecovery{
		Fid: 1,
		Off: 40,
	}
	e2.Meta = core.NewMetaData().WithTxID(200)

	assert.True(t, dt.IsSameTx(e1))
	assert.False(t, dt.IsSameTx(e2))

	dt.AppendEntry(e1)
	dt.AppendEntry(e2)
	assert.Len(t, dt.Es, 2)

	dt.Reset()
	assert.Len(t, dt.Es, 0)
	assert.Equal(t, uint64(0), dt.TxId)
}

func Test_EntryGetRawKey_Various(t *testing.T) {
	r := require.New(t)

	// List with LPush flag, valid prefix.
	seq := make([]byte, 8)
	copy(seq, []byte("12345678"))
	userKey := []byte("list-key")
	listKey := append(seq, userKey...)

	listEntry := core.NewEntry().WithKey(listKey).WithMeta(
		core.NewMetaData().
			WithDs(uint16(core.DataStructureList)).
			WithFlag(uint16(core.DataLPushFlag)),
	)

	rawKey, err := listEntry.GetRawKey()
	r.NoError(err)
	r.Equal(userKey, rawKey)

	// List with short key should return error.
	shortList := core.NewEntry().WithKey([]byte("short")).WithMeta(
		core.NewMetaData().
			WithDs(uint16(core.DataStructureList)).
			WithFlag(uint16(core.DataLPushFlag)),
	)
	rawKey, err = shortList.GetRawKey()
	r.Equal(core.ErrInvalidKey, err)
	r.Equal([]byte("short"), rawKey)

	// List with non-push flag returns original key.
	otherList := core.NewEntry().WithKey([]byte("plain")).WithMeta(
		core.NewMetaData().
			WithDs(uint16(core.DataStructureList)).
			WithFlag(uint16(core.DataSetFlag)),
	)
	rawKey, err = otherList.GetRawKey()
	r.NoError(err)
	r.Equal([]byte("plain"), rawKey)

	// Sorted set ZAdd with separator.
	zsetKey := []byte("user" + core.SeparatorForZSetKey + "score")
	zsetEntry := core.NewEntry().WithKey(zsetKey).WithMeta(
		core.NewMetaData().
			WithDs(uint16(core.DataStructureSortedSet)).
			WithFlag(uint16(core.DataZAddFlag)),
	)
	rawKey, err = zsetEntry.GetRawKey()
	r.NoError(err)
	r.Equal([]byte("user"), rawKey)

	// Sorted set with invalid key.
	badZsetKey := []byte("invalid-key-without-sep")
	badZsetEntry := core.NewEntry().WithKey(badZsetKey).WithMeta(
		core.NewMetaData().
			WithDs(uint16(core.DataStructureSortedSet)).
			WithFlag(uint16(core.DataZAddFlag)),
	)
	rawKey, err = badZsetEntry.GetRawKey()
	r.Equal(core.ErrInvalidKey, err)
	r.Equal(badZsetKey, rawKey)

	// Set and BTree should return original key.
	setKey := []byte("set-key")
	setEntry := core.NewEntry().WithKey(setKey).WithMeta(
		core.NewMetaData().
			WithDs(uint16(core.DataStructureSet)),
	)
	rawKey, err = setEntry.GetRawKey()
	r.NoError(err)
	r.Equal(setKey, rawKey)

	btreeKey := []byte("btree-key")
	btreeEntry := core.NewEntry().WithKey(btreeKey).WithMeta(
		core.NewMetaData().
			WithDs(uint16(core.DataStructureBTree)),
	)
	rawKey, err = btreeEntry.GetRawKey()
	r.NoError(err)
	r.Equal(btreeKey, rawKey)

	// Unsupported data structure.
	unsupportedEntry := core.NewEntry().WithKey([]byte("k")).WithMeta(
		core.NewMetaData().
			WithDs(100),
	)
	rawKey, err = unsupportedEntry.GetRawKey()
	r.Equal(core.ErrDataStructureNotSupported, err)
	r.Equal([]byte("k"), rawKey)
}
