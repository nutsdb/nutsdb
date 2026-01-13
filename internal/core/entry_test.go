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
