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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemStore_PutGet(t *testing.T) {
	ms := NewMemStore()
	key := []byte("key")
	val := []byte("value")

	require.NoError(t, ms.Put(key, val))

	got, err := ms.Get(key)
	require.NoError(t, err)
	require.Equal(t, val, got)
}

func TestMemStore_PutOverwrite(t *testing.T) {
	ms := NewMemStore()
	key := []byte("key")

	require.NoError(t, ms.Put(key, []byte("v1")))
	require.NoError(t, ms.Put(key, []byte("v2")))

	got, err := ms.Get(key)
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), got)
}

func TestMemStore_GetNotFound(t *testing.T) {
	ms := NewMemStore()

	_, err := ms.Get([]byte("missing"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestMemStore_DeleteFound(t *testing.T) {
	ms := NewMemStore()
	key := []byte("key")
	val := []byte("value")

	require.NoError(t, ms.Put(key, val))

	got, ok := ms.Delete(key)
	require.True(t, ok)
	require.Equal(t, val, got)

	_, err := ms.Get(key)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestMemStore_DeleteNotFound(t *testing.T) {
	ms := NewMemStore()

	got, ok := ms.Delete([]byte("missing"))
	require.False(t, ok)
	require.Nil(t, got)
}

func TestMemStore_DeleteIdempotent(t *testing.T) {
	ms := NewMemStore()
	key := []byte("key")

	require.NoError(t, ms.Put(key, []byte("value")))

	_, ok := ms.Delete(key)
	require.True(t, ok)

	got, ok := ms.Delete(key)
	require.False(t, ok)
	require.Nil(t, got)
}

func TestMemStore_IterateSortedOrder(t *testing.T) {
	ms := NewMemStore()
	entries := [][2][]byte{
		{[]byte("c"), []byte("3")},
		{[]byte("a"), []byte("1")},
		{[]byte("b"), []byte("2")},
	}
	for _, e := range entries {
		require.NoError(t, ms.Put(e[0], e[1]))
	}

	var keys [][]byte
	ms.Iterate(func(key, value []byte) bool {
		keys = append(keys, append([]byte(nil), key...))
		require.NotNil(t, value)
		return true
	})
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, keys)
}

func TestMemStore_IterateEarlyStop(t *testing.T) {
	ms := NewMemStore()
	for _, k := range [][]byte{[]byte("a"), []byte("b"), []byte("c")} {
		require.NoError(t, ms.Put(k, k))
	}

	var count int
	ms.Iterate(func(key, value []byte) bool {
		count++
		return false
	})
	require.Equal(t, 1, count)
}

func TestMemStore_BinaryKey(t *testing.T) {
	ms := NewMemStore()
	original := []byte{0x00, 0xff, 0x00, 'k'}
	other := []byte{0x01, 0xff, 0x00, 'k'}

	require.NoError(t, ms.Put(original, []byte("binary")))

	_, err := ms.Get(other)
	require.ErrorIs(t, err, ErrKeyNotFound)

	got, err := ms.Get([]byte{0x00, 0xff, 0x00, 'k'})
	require.NoError(t, err)
	require.Equal(t, []byte("binary"), got)
}

func TestMemStore_EmptyStoreIterate(t *testing.T) {
	ms := NewMemStore()

	var called bool
	ms.Iterate(func(key, value []byte) bool {
		called = true
		return true
	})
	require.False(t, called)
}
