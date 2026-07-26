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
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/nutsdb/nutsdb/internal/fileio"
	"github.com/stretchr/testify/require"
)

func testLoc(fileID uint32, offset uint64, length uint32) fileio.Location {
	return fileio.Location{FileID: fileID, Offset: offset, Length: length}
}

func requireLocationEqual(t *testing.T, expect, got fileio.Location) {
	t.Helper()
	require.Equal(t, expect, got)
}

func assertMemStoreState(t *testing.T, ms MemStore, want map[string]fileio.Location) {
	t.Helper()

	var prev []byte
	var count int
	ms.Iterate(func(key []byte, value fileio.Location) bool {
		count++
		if len(prev) > 0 {
			require.Less(t, bytes.Compare(prev, key), 0)
		}
		prev = append(prev[:0], key...)

		expect, ok := want[string(key)]
		require.True(t, ok, "unexpected key %q", key)
		requireLocationEqual(t, expect, value)
		return true
	})
	require.Equal(t, len(want), count)
}

func TestMemStore_PutGet(t *testing.T) {
	ms := NewMemStore()
	key := []byte("key")
	loc := testLoc(1, fileio.HeaderSize, 32)

	require.NoError(t, ms.Put(key, loc))

	got, err := ms.Get(key)
	require.NoError(t, err)
	requireLocationEqual(t, loc, got)
}

func TestMemStore_PutOverwrite(t *testing.T) {
	ms := NewMemStore()
	key := []byte("key")

	require.NoError(t, ms.Put(key, testLoc(1, fileio.HeaderSize, 16)))
	locV2 := testLoc(2, fileio.HeaderSize+16, 24)
	require.NoError(t, ms.Put(key, locV2))

	got, err := ms.Get(key)
	require.NoError(t, err)
	requireLocationEqual(t, locV2, got)
}

func TestMemStore_GetNotFound(t *testing.T) {
	ms := NewMemStore()

	_, err := ms.Get([]byte("missing"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestMemStore_DeleteFound(t *testing.T) {
	ms := NewMemStore()
	key := []byte("key")
	loc := testLoc(1, fileio.HeaderSize, 32)

	require.NoError(t, ms.Put(key, loc))

	got, ok := ms.Delete(key)
	require.True(t, ok)
	requireLocationEqual(t, loc, got)

	_, err := ms.Get(key)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestMemStore_DeleteNotFound(t *testing.T) {
	ms := NewMemStore()

	got, ok := ms.Delete([]byte("missing"))
	require.False(t, ok)
	require.Equal(t, fileio.Location{}, got)
}

func TestMemStore_DeleteIdempotent(t *testing.T) {
	ms := NewMemStore()
	key := []byte("key")

	require.NoError(t, ms.Put(key, testLoc(1, fileio.HeaderSize, 16)))

	_, ok := ms.Delete(key)
	require.True(t, ok)

	got, ok := ms.Delete(key)
	require.False(t, ok)
	require.Equal(t, fileio.Location{}, got)
}

func TestMemStore_IterateSortedOrder(t *testing.T) {
	ms := NewMemStore()
	entries := []struct {
		key []byte
		loc fileio.Location
	}{
		{[]byte("c"), testLoc(1, 100, 10)},
		{[]byte("a"), testLoc(1, 200, 10)},
		{[]byte("b"), testLoc(1, 300, 10)},
	}
	for _, e := range entries {
		require.NoError(t, ms.Put(e.key, e.loc))
	}

	var keys [][]byte
	ms.Iterate(func(key []byte, value fileio.Location) bool {
		keys = append(keys, append([]byte(nil), key...))
		require.NotEqual(t, fileio.Location{}, value)
		return true
	})
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, keys)
}

func TestMemStore_IterateEarlyStop(t *testing.T) {
	ms := NewMemStore()
	for i, k := range [][]byte{[]byte("a"), []byte("b"), []byte("c")} {
		require.NoError(t, ms.Put(k, testLoc(1, uint64(fileio.HeaderSize+i*16), 16)))
	}

	var count int
	ms.Iterate(func(key []byte, value fileio.Location) bool {
		count++
		return false
	})
	require.Equal(t, 1, count)
}

func TestMemStore_BinaryKey(t *testing.T) {
	ms := NewMemStore()
	original := []byte{0x00, 0xff, 0x00, 'k'}
	other := []byte{0x01, 0xff, 0x00, 'k'}
	loc := testLoc(3, fileio.HeaderSize, 40)

	require.NoError(t, ms.Put(original, loc))

	_, err := ms.Get(other)
	require.ErrorIs(t, err, ErrKeyNotFound)

	got, err := ms.Get([]byte{0x00, 0xff, 0x00, 'k'})
	require.NoError(t, err)
	requireLocationEqual(t, loc, got)
}

func TestMemStore_EmptyStoreIterate(t *testing.T) {
	ms := NewMemStore()

	var called bool
	ms.Iterate(func(key []byte, value fileio.Location) bool {
		called = true
		return true
	})
	require.False(t, called)
}

func TestMemStore_RandomInsert(t *testing.T) {
	const n = 500
	ms := NewMemStore()
	entries := make([]struct {
		key []byte
		loc fileio.Location
	}, n)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		entries[i] = struct {
			key []byte
			loc fileio.Location
		}{
			key: key,
			loc: testLoc(1, uint64(fileio.HeaderSize+i*32), 32),
		}
	}

	rng := rand.New(rand.NewSource(42))
	for _, idx := range rng.Perm(n) {
		entry := entries[idx]
		require.NoError(t, ms.Put(entry.key, entry.loc))
	}

	for _, entry := range entries {
		got, err := ms.Get(entry.key)
		require.NoError(t, err)
		requireLocationEqual(t, entry.loc, got)
	}

	var keys [][]byte
	ms.Iterate(func(key []byte, value fileio.Location) bool {
		if len(keys) > 0 {
			require.Less(t, bytes.Compare(keys[len(keys)-1], key), 0)
		}
		keys = append(keys, append([]byte(nil), key...))
		return true
	})
	require.Len(t, keys, n)
}

func TestMemStore_RandomInsertAndDelete(t *testing.T) {
	const n = 1000
	ms := NewMemStore()
	entries := make([]struct {
		key []byte
		loc fileio.Location
	}, n)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		entries[i] = struct {
			key []byte
			loc fileio.Location
		}{
			key: key,
			loc: testLoc(1, uint64(fileio.HeaderSize+i*32), 32),
		}
	}

	rng := rand.New(rand.NewSource(99))
	for _, idx := range rng.Perm(n) {
		entry := entries[idx]
		require.NoError(t, ms.Put(entry.key, entry.loc))
	}

	remaining := make(map[string]fileio.Location, n)
	for _, entry := range entries {
		remaining[string(entry.key)] = entry.loc
	}

	for _, idx := range rng.Perm(n) {
		entry := entries[idx]
		got, ok := ms.Delete(entry.key)
		require.True(t, ok)
		requireLocationEqual(t, entry.loc, got)

		delete(remaining, string(entry.key))
		assertMemStoreState(t, ms, remaining)
	}

	assertMemStoreState(t, ms, map[string]fileio.Location{})
}

func TestMemStore_DeleteStructuralCases(t *testing.T) {
	t.Run("sole_root", func(t *testing.T) {
		ms := NewMemStore()
		key := []byte("root")
		loc := testLoc(1, fileio.HeaderSize, 16)

		require.NoError(t, ms.Put(key, loc))
		got, ok := ms.Delete(key)
		require.True(t, ok)
		requireLocationEqual(t, loc, got)
		assertMemStoreState(t, ms, map[string]fileio.Location{})
	})

	t.Run("delete_root_with_children", func(t *testing.T) {
		ms := NewMemStore()
		keys := []string{"key-002", "key-000", "key-001", "key-003", "key-004"}
		want := make(map[string]fileio.Location, len(keys))
		for i, key := range keys {
			loc := testLoc(1, uint64(fileio.HeaderSize+i*16), 16)
			require.NoError(t, ms.Put([]byte(key), loc))
			want[key] = loc
		}

		got, ok := ms.Delete([]byte("key-002"))
		require.True(t, ok)
		requireLocationEqual(t, want["key-002"], got)
		delete(want, "key-002")
		assertMemStoreState(t, ms, want)
	})

	t.Run("delete_internal_then_leaves", func(t *testing.T) {
		ms := NewMemStore()
		keys := []string{
			"key-004", "key-002", "key-006", "key-001", "key-003",
			"key-005", "key-007", "key-000", "key-008", "key-009",
		}
		want := make(map[string]fileio.Location, len(keys))
		for i, key := range keys {
			loc := testLoc(1, uint64(fileio.HeaderSize+i*16), 16)
			require.NoError(t, ms.Put([]byte(key), loc))
			want[key] = loc
		}

		deleteOrder := []string{
			"key-004", "key-001", "key-006", "key-000", "key-008",
			"key-002", "key-005", "key-009", "key-003", "key-007",
		}
		for _, key := range deleteOrder {
			got, ok := ms.Delete([]byte(key))
			require.True(t, ok)
			requireLocationEqual(t, want[key], got)
			delete(want, key)
			assertMemStoreState(t, ms, want)
		}
	})

	t.Run("delete_in_sorted_order", func(t *testing.T) {
		ms := NewMemStore()
		want := make(map[string]fileio.Location)
		for i := 0; i < 32; i++ {
			key := fmt.Sprintf("sorted-%02d", i)
			loc := testLoc(1, uint64(fileio.HeaderSize+i*16), 16)
			require.NoError(t, ms.Put([]byte(key), loc))
			want[key] = loc
		}

		for i := 0; i < 32; i++ {
			key := fmt.Sprintf("sorted-%02d", i)
			got, ok := ms.Delete([]byte(key))
			require.True(t, ok)
			requireLocationEqual(t, want[key], got)
			delete(want, key)
			assertMemStoreState(t, ms, want)
		}
	})

	t.Run("delete_in_reverse_sorted_order", func(t *testing.T) {
		ms := NewMemStore()
		want := make(map[string]fileio.Location)
		for i := 0; i < 32; i++ {
			key := fmt.Sprintf("rev-%02d", i)
			loc := testLoc(1, uint64(fileio.HeaderSize+i*16), 16)
			require.NoError(t, ms.Put([]byte(key), loc))
			want[key] = loc
		}

		for i := 31; i >= 0; i-- {
			key := fmt.Sprintf("rev-%02d", i)
			got, ok := ms.Delete([]byte(key))
			require.True(t, ok)
			requireLocationEqual(t, want[key], got)
			delete(want, key)
			assertMemStoreState(t, ms, want)
		}
	})
}
