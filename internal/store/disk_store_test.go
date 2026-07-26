// Copyright 2026 The nutsdb Author. All rights reserved.
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

package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/fileio"
	"github.com/stretchr/testify/require"
)

func testDiskOpts(dir string) DiskStoreOptions {
	opts := DefaultDiskStoreOptions(dir)
	opts.SegmentSize = fileio.HeaderSize + fileio.FooterSize + (64 << 10)
	opts.MaxRecordSize = 4 << 10
	opts.WriteBufferSize = 4 << 10
	opts.SyncMode = fileio.SyncEveryWrite
	return opts
}

func TestEntry_PutDeleteRoundTrip(t *testing.T) {
	rec := core.NewRecord().WithKey([]byte("k")).WithValue([]byte("v")).WithTTL(10)
	rec.Timestamp = 100
	raw, err := encodePutPayload([]byte("k"), rec)
	require.NoError(t, err)

	key, got, err := decodePutPayload(raw)
	require.NoError(t, err)
	require.Equal(t, []byte("k"), key)
	require.Equal(t, []byte("v"), got.Value)
	require.Equal(t, uint64(100), got.Timestamp)
	require.Equal(t, uint32(10), got.TTL)

	dk, err := decodeKey(raw, fileio.RecordPut)
	require.NoError(t, err)
	require.Equal(t, []byte("k"), dk)

	del, err := encodeDeletePayload([]byte("k"))
	require.NoError(t, err)
	dkey, err := decodeDeletePayload(del)
	require.NoError(t, err)
	require.Equal(t, []byte("k"), dkey)

	_, err = decodeKey(nil, fileio.RecordMeta)
	require.ErrorIs(t, err, fileio.ErrHintSkipMeta)
}

func TestDiskStore_PutGetDelete(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenDiskStore(testDiskOpts(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()

	ctx := context.Background()
	rec := core.NewRecord().WithValue([]byte("hello"))
	require.NoError(t, st.Put(ctx, []byte("a"), rec))

	got, err := st.Get(ctx, []byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), got.Value)
	require.Equal(t, []byte("a"), got.Key)
	require.NotZero(t, got.Timestamp)

	require.NoError(t, st.Delete(ctx, []byte("a")))
	_, err = st.Get(ctx, []byte("a"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	require.NoError(t, st.Delete(ctx, []byte("missing")))
}

func TestDiskStore_PersistAndReopen(t *testing.T) {
	dir := t.TempDir()
	opts := testDiskOpts(dir)

	st1, err := OpenDiskStore(opts)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, st1.Put(ctx, []byte("k1"), core.NewRecord().WithValue([]byte("v1"))))
	require.NoError(t, st1.Put(ctx, []byte("k2"), core.NewRecord().WithValue([]byte("v2"))))
	require.NoError(t, st1.Delete(ctx, []byte("k1")))
	require.NoError(t, st1.Close())

	st2, err := OpenDiskStore(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, st2.Close()) }()

	_, err = st2.Get(ctx, []byte("k1"))
	require.ErrorIs(t, err, ErrKeyNotFound)
	got, err := st2.Get(ctx, []byte("k2"))
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), got.Value)
}

func TestDiskStore_IterateOrder(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenDiskStore(testDiskOpts(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()

	ctx := context.Background()
	require.NoError(t, st.Put(ctx, []byte("c"), core.NewRecord().WithValue([]byte("3"))))
	require.NoError(t, st.Put(ctx, []byte("a"), core.NewRecord().WithValue([]byte("1"))))
	require.NoError(t, st.Put(ctx, []byte("b"), core.NewRecord().WithValue([]byte("2"))))

	var keys []string
	require.NoError(t, st.Iterate(ctx, func(key []byte, value *core.Record) bool {
		keys = append(keys, string(key))
		return true
	}))
	require.Equal(t, []string{"a", "b", "c"}, keys)
}

func TestDiskStore_Batch(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenDiskStore(testDiskOpts(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()

	ctx := context.Background()
	require.NoError(t, st.BatchPut(ctx, []struct {
		Key   []byte
		Value *core.Record
	}{
		{Key: []byte("x"), Value: core.NewRecord().WithValue([]byte("1"))},
		{Key: []byte("y"), Value: core.NewRecord().WithValue([]byte("2"))},
	}))

	got, err := st.BatchGet(ctx, [][]byte{[]byte("x"), []byte("missing"), []byte("y")})
	require.NoError(t, err)
	require.Len(t, got, 3)
	require.Equal(t, []byte("1"), got[0].Value.Value)
	require.Nil(t, got[1].Value)
	require.Equal(t, []byte("2"), got[2].Value.Value)

	require.NoError(t, st.BatchDelete(ctx, [][]byte{[]byte("x")}))
	_, err = st.Get(ctx, []byte("x"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestDiskStore_TTLExpired(t *testing.T) {
	dir := t.TempDir()
	opts := testDiskOpts(dir)
	opts.EnableReadTTL = true
	opts.AutoFillTimestamp = false
	st, err := OpenDiskStore(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()

	ctx := context.Background()
	rec := core.NewRecord().WithValue([]byte("old"))
	rec.Timestamp = uint64(time.Now().Unix()) - 100
	rec.TTL = 1
	require.NoError(t, st.Put(ctx, []byte("exp"), rec))

	_, err = st.Get(ctx, []byte("exp"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestDiskStore_HintAfterSeal(t *testing.T) {
	dir := t.TempDir()
	opts := testDiskOpts(dir)
	opts.SegmentSize = fileio.HeaderSize + fileio.FooterSize + 512
	opts.MaxRecordSize = 128
	opts.WriteBufferSize = 64

	st, err := OpenDiskStore(opts)
	require.NoError(t, err)

	ctx := context.Background()
	var sealed bool
	for i := 0; i < 40; i++ {
		key := []byte{byte('a' + i%26), byte(i)}
		val := make([]byte, 40)
		require.NoError(t, st.Put(ctx, key, core.NewRecord().WithValue(val)))
		matches, _ := filepath.Glob(filepath.Join(dir, "*.hint"))
		if len(matches) > 0 {
			sealed = true
			break
		}
	}
	require.True(t, sealed, "expected hint file after segment seal")
	require.NoError(t, st.Close())

	st2, err := OpenDiskStore(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, st2.Close()) }()

	// Store should reopen and serve data.
	var n int
	require.NoError(t, st2.Iterate(ctx, func(key []byte, value *core.Record) bool {
		n++
		return true
	}))
	require.Greater(t, n, 0)
}

func TestDiskStore_EmptyKey(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenDiskStore(testDiskOpts(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()

	ctx := context.Background()
	require.ErrorIs(t, st.Put(ctx, nil, core.NewRecord().WithValue([]byte("v"))), ErrKeyEmpty)
	_, err = st.Get(ctx, nil)
	require.ErrorIs(t, err, ErrKeyEmpty)
}
