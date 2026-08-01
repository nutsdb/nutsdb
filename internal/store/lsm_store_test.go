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
	"fmt"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/fileio"
	"github.com/stretchr/testify/require"
)

func testLSMOpts(dir string) LSMOptions {
	opts := DefaultLSMOptions(dir)
	opts.WALSyncMode = fileio.SyncEveryWrite
	opts.MemTableSize = 1 << 20 // 1MiB
	opts.ValueInlineThreshold = 64
	opts.L0FileNumCompactionTrigger = 2
	opts.LevelBaseSize = 1 << 20
	opts.ValueLog.SyncMode = fileio.SyncEveryWrite
	opts.ValueLog.SegmentSize = 4 << 20
	return opts
}

func TestLSM_PutGetDelete(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStoreManager(testLSMOpts(dir))
	require.NoError(t, err)
	defer st.Close()

	ctx := context.Background()
	require.NoError(t, st.Put(ctx, []byte("a"), core.NewRecord().WithValue([]byte("1"))))
	rec, err := st.Get(ctx, []byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("1"), rec.Value)

	require.NoError(t, st.Delete(ctx, []byte("a")))
	_, err = st.Get(ctx, []byte("a"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestLSM_InlineAndLocation(t *testing.T) {
	dir := t.TempDir()
	opts := testLSMOpts(dir)
	opts.ValueInlineThreshold = 8
	st, err := OpenStoreManager(opts)
	require.NoError(t, err)
	defer st.Close()
	ctx := context.Background()

	require.NoError(t, st.Put(ctx, []byte("small"), core.NewRecord().WithValue([]byte("abc"))))
	big := make([]byte, 64)
	for i := range big {
		big[i] = 'x'
	}
	require.NoError(t, st.Put(ctx, []byte("big"), core.NewRecord().WithValue(big)))

	rec, err := st.Get(ctx, []byte("small"))
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), rec.Value)
	rec, err = st.Get(ctx, []byte("big"))
	require.NoError(t, err)
	require.Equal(t, big, rec.Value)
}

func TestLSM_PersistAndReopen(t *testing.T) {
	dir := t.TempDir()
	opts := testLSMOpts(dir)
	ctx := context.Background()

	st1, err := OpenStoreManager(opts)
	require.NoError(t, err)
	require.NoError(t, st1.Put(ctx, []byte("k"), core.NewRecord().WithValue([]byte("v"))))
	require.NoError(t, st1.Close())

	st2, err := OpenStoreManager(opts)
	require.NoError(t, err)
	defer st2.Close()
	rec, err := st2.Get(ctx, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), rec.Value)
}

func TestLSM_FlushAndReopen(t *testing.T) {
	dir := t.TempDir()
	opts := testLSMOpts(dir)
	opts.MemTableSize = 512 // force flush quickly
	ctx := context.Background()

	st1, err := OpenStoreManager(opts)
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		v := []byte(fmt.Sprintf("val-%04d", i))
		require.NoError(t, st1.Put(ctx, k, core.NewRecord().WithValue(v)))
	}
	require.NoError(t, st1.Close())

	st2, err := OpenStoreManager(opts)
	require.NoError(t, err)
	defer st2.Close()
	rec, err := st2.Get(ctx, []byte("key-0042"))
	require.NoError(t, err)
	require.Equal(t, []byte("val-0042"), rec.Value)
}

func TestLSM_IterateOrderAndDelete(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStoreManager(testLSMOpts(dir))
	require.NoError(t, err)
	defer st.Close()
	ctx := context.Background()

	require.NoError(t, st.Put(ctx, []byte("c"), core.NewRecord().WithValue([]byte("3"))))
	require.NoError(t, st.Put(ctx, []byte("a"), core.NewRecord().WithValue([]byte("1"))))
	require.NoError(t, st.Put(ctx, []byte("b"), core.NewRecord().WithValue([]byte("2"))))
	require.NoError(t, st.Delete(ctx, []byte("b")))

	var keys []string
	require.NoError(t, st.Iterate(ctx, func(key []byte, _ *core.Record) bool {
		keys = append(keys, string(key))
		return true
	}))
	require.Equal(t, []string{"a", "c"}, keys)
}

func TestLSM_Batch(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStoreManager(testLSMOpts(dir))
	require.NoError(t, err)
	defer st.Close()
	ctx := context.Background()

	require.NoError(t, st.BatchPut(ctx, []struct {
		Key   []byte
		Value *core.Record
	}{
		{Key: []byte("x"), Value: core.NewRecord().WithValue([]byte("1"))},
		{Key: []byte("y"), Value: core.NewRecord().WithValue([]byte("2"))},
	}))
	got, err := st.BatchGet(ctx, [][]byte{[]byte("x"), []byte("z"), []byte("y")})
	require.NoError(t, err)
	require.Equal(t, []byte("1"), got[0].Value.Value)
	require.Nil(t, got[1].Value)
	require.Equal(t, []byte("2"), got[2].Value.Value)
	require.NoError(t, st.BatchDelete(ctx, [][]byte{[]byte("x")}))
	_, err = st.Get(ctx, []byte("x"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestLSM_TTLExpired(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStoreManager(testLSMOpts(dir))
	require.NoError(t, err)
	defer st.Close()
	ctx := context.Background()

	rec := core.NewRecord().WithValue([]byte("v"))
	rec.Timestamp = uint64(time.Now().Unix()) - 10
	rec.TTL = 1
	require.NoError(t, st.Put(ctx, []byte("old"), rec))
	_, err = st.Get(ctx, []byte("old"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestLSM_UpdateOverwrite(t *testing.T) {
	dir := t.TempDir()
	opts := testLSMOpts(dir)
	opts.MemTableSize = 256
	opts.L0FileNumCompactionTrigger = 2
	st, err := OpenStoreManager(opts)
	require.NoError(t, err)
	ctx := context.Background()

	for i := 0; i < 30; i++ {
		require.NoError(t, st.Put(ctx, []byte("k"), core.NewRecord().WithValue([]byte(fmt.Sprintf("v%d", i)))))
		// also write fillers to flush
		require.NoError(t, st.Put(ctx, []byte(fmt.Sprintf("f%04d", i)), core.NewRecord().WithValue([]byte("pad"))))
	}
	rec, err := st.Get(ctx, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v29"), rec.Value)
	require.NoError(t, st.Close())

	st2, err := OpenStoreManager(opts)
	require.NoError(t, err)
	defer st2.Close()
	rec, err = st2.Get(ctx, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v29"), rec.Value)
}

func TestEncodeDecodePutPayload(t *testing.T) {
	key := []byte("k1")
	rec := core.NewRecord().WithValue([]byte("v1"))
	rec.Timestamp = 100
	raw, err := encodePutPayload(key, rec)
	require.NoError(t, err)
	gotKey, gotRec, err := decodePutPayload(raw)
	require.NoError(t, err)
	require.Equal(t, key, gotKey)
	require.Equal(t, rec.Value, gotRec.Value)
}

func TestValueRefRoundTrip(t *testing.T) {
	refs := []ValueRef{
		{Kind: ValueKindTombstone, Timestamp: 1},
		{Kind: ValueKindInline, Inline: []byte("hi"), Timestamp: 2, TTL: 3},
		{Kind: ValueKindLocation, Loc: fileio.Location{FileID: 1, Offset: 100, Length: 20}, Timestamp: 4},
	}
	for _, ref := range refs {
		raw := encodeValueRef(nil, ref)
		got, n, err := decodeValueRef(raw)
		require.NoError(t, err)
		require.Equal(t, len(raw), n)
		require.Equal(t, ref.Kind, got.Kind)
		require.Equal(t, ref.Timestamp, got.Timestamp)
		require.Equal(t, ref.TTL, got.TTL)
		require.Equal(t, ref.Inline, got.Inline)
		require.Equal(t, ref.Loc, got.Loc)
	}
}
