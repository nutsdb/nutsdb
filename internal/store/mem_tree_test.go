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

	"github.com/stretchr/testify/require"
)

// testVal is a dedicated value type for MemTree tests (not production types).
type testVal struct {
	ID int
}

func tv(id int) testVal { return testVal{ID: id} }

func assertMemTreeState(t *testing.T, tree MemTree[testVal], want map[string]testVal) {
	t.Helper()

	var prev []byte
	var count int
	tree.Iterate(func(key []byte, value testVal) bool {
		count++
		if len(prev) > 0 {
			require.Less(t, bytes.Compare(prev, key), 0)
		}
		prev = append(prev[:0], key...)

		expect, ok := want[string(key)]
		require.True(t, ok, "unexpected key %q", key)
		require.Equal(t, expect, value)
		return true
	})
	require.Equal(t, len(want), count)
	require.Equal(t, len(want), tree.Size())
}

func TestMemTree_PutGet(t *testing.T) {
	tree := newMemTree[testVal]()
	key := []byte("key")
	val := tv(1)

	require.NoError(t, tree.Put(key, val))

	got, err := tree.Get(key)
	require.NoError(t, err)
	require.Equal(t, val, got)
}

func TestMemTree_PutOverwrite(t *testing.T) {
	tree := newMemTree[testVal]()
	key := []byte("key")

	require.NoError(t, tree.Put(key, tv(1)))
	require.NoError(t, tree.Put(key, tv(2)))

	got, err := tree.Get(key)
	require.NoError(t, err)
	require.Equal(t, tv(2), got)
	require.Equal(t, 1, tree.Size())
}

func TestMemTree_GetNotFound(t *testing.T) {
	tree := newMemTree[testVal]()

	_, err := tree.Get([]byte("missing"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestMemTree_DeleteFound(t *testing.T) {
	tree := newMemTree[testVal]()
	key := []byte("key")
	val := tv(32)

	require.NoError(t, tree.Put(key, val))

	got, ok := tree.Delete(key)
	require.True(t, ok)
	require.Equal(t, val, got)

	_, err := tree.Get(key)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestMemTree_DeleteNotFound(t *testing.T) {
	tree := newMemTree[testVal]()

	got, ok := tree.Delete([]byte("missing"))
	require.False(t, ok)
	require.Equal(t, testVal{}, got)
}

func TestMemTree_DeleteIdempotent(t *testing.T) {
	tree := newMemTree[testVal]()
	key := []byte("key")

	require.NoError(t, tree.Put(key, tv(16)))

	_, ok := tree.Delete(key)
	require.True(t, ok)

	got, ok := tree.Delete(key)
	require.False(t, ok)
	require.Equal(t, testVal{}, got)
}

func TestMemTree_IterateSortedOrder(t *testing.T) {
	tree := newMemTree[testVal]()
	entries := []struct {
		key []byte
		val testVal
	}{
		{[]byte("c"), tv(100)},
		{[]byte("a"), tv(200)},
		{[]byte("b"), tv(300)},
	}
	for _, e := range entries {
		require.NoError(t, tree.Put(e.key, e.val))
	}

	var keys [][]byte
	tree.Iterate(func(key []byte, value testVal) bool {
		keys = append(keys, append([]byte(nil), key...))
		require.NotEqual(t, testVal{}, value)
		return true
	})
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, keys)
}

func TestMemTree_IterateEarlyStop(t *testing.T) {
	tree := newMemTree[testVal]()
	for i, k := range [][]byte{[]byte("a"), []byte("b"), []byte("c")} {
		require.NoError(t, tree.Put(k, tv(i)))
	}

	var count int
	tree.Iterate(func(key []byte, value testVal) bool {
		count++
		return false
	})
	require.Equal(t, 1, count)
}

func TestMemTree_BinaryKey(t *testing.T) {
	tree := newMemTree[testVal]()
	original := []byte{0x00, 0xff, 0x00, 'k'}
	other := []byte{0x01, 0xff, 0x00, 'k'}
	val := tv(40)

	require.NoError(t, tree.Put(original, val))

	_, err := tree.Get(other)
	require.ErrorIs(t, err, ErrKeyNotFound)

	got, err := tree.Get([]byte{0x00, 0xff, 0x00, 'k'})
	require.NoError(t, err)
	require.Equal(t, val, got)
}

func TestMemTree_EmptyIterate(t *testing.T) {
	tree := newMemTree[testVal]()

	var called bool
	tree.Iterate(func(key []byte, value testVal) bool {
		called = true
		return true
	})
	require.False(t, called)
}

func TestMemTree_RandomInsert(t *testing.T) {
	const n = 500
	tree := newMemTree[testVal]()
	entries := make([]struct {
		key []byte
		val testVal
	}, n)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		entries[i] = struct {
			key []byte
			val testVal
		}{
			key: key,
			val: tv(i),
		}
	}

	rng := rand.New(rand.NewSource(42))
	for _, idx := range rng.Perm(n) {
		entry := entries[idx]
		require.NoError(t, tree.Put(entry.key, entry.val))
	}

	for _, entry := range entries {
		got, err := tree.Get(entry.key)
		require.NoError(t, err)
		require.Equal(t, entry.val, got)
	}

	var keys [][]byte
	tree.Iterate(func(key []byte, value testVal) bool {
		if len(keys) > 0 {
			require.Less(t, bytes.Compare(keys[len(keys)-1], key), 0)
		}
		keys = append(keys, append([]byte(nil), key...))
		return true
	})
	require.Len(t, keys, n)
	require.Equal(t, n, tree.Size())
}

func TestMemTree_RandomInsertAndDelete(t *testing.T) {
	const n = 1000
	tree := newMemTree[testVal]()
	entries := make([]struct {
		key []byte
		val testVal
	}, n)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		entries[i] = struct {
			key []byte
			val testVal
		}{
			key: key,
			val: tv(i),
		}
	}

	rng := rand.New(rand.NewSource(99))
	for _, idx := range rng.Perm(n) {
		entry := entries[idx]
		require.NoError(t, tree.Put(entry.key, entry.val))
	}

	remaining := make(map[string]testVal, n)
	for _, entry := range entries {
		remaining[string(entry.key)] = entry.val
	}

	for _, idx := range rng.Perm(n) {
		entry := entries[idx]
		got, ok := tree.Delete(entry.key)
		require.True(t, ok)
		require.Equal(t, entry.val, got)

		delete(remaining, string(entry.key))
		assertMemTreeState(t, tree, remaining)
	}

	assertMemTreeState(t, tree, map[string]testVal{})
}

func TestMemTree_DeleteStructuralCases(t *testing.T) {
	t.Run("sole_root", func(t *testing.T) {
		tree := newMemTree[testVal]()
		key := []byte("root")
		val := tv(16)

		require.NoError(t, tree.Put(key, val))
		got, ok := tree.Delete(key)
		require.True(t, ok)
		require.Equal(t, val, got)
		assertMemTreeState(t, tree, map[string]testVal{})
	})

	t.Run("delete_root_with_children", func(t *testing.T) {
		tree := newMemTree[testVal]()
		keys := []string{"key-002", "key-000", "key-001", "key-003", "key-004"}
		want := make(map[string]testVal, len(keys))
		for i, key := range keys {
			require.NoError(t, tree.Put([]byte(key), tv(i)))
			want[key] = tv(i)
		}

		got, ok := tree.Delete([]byte("key-002"))
		require.True(t, ok)
		require.Equal(t, want["key-002"], got)
		delete(want, "key-002")
		assertMemTreeState(t, tree, want)
	})

	t.Run("delete_internal_then_leaves", func(t *testing.T) {
		tree := newMemTree[testVal]()
		keys := []string{
			"key-004", "key-002", "key-006", "key-001", "key-003",
			"key-005", "key-007", "key-000", "key-008", "key-009",
		}
		want := make(map[string]testVal, len(keys))
		for i, key := range keys {
			require.NoError(t, tree.Put([]byte(key), tv(i)))
			want[key] = tv(i)
		}

		deleteOrder := []string{
			"key-004", "key-001", "key-006", "key-000", "key-008",
			"key-002", "key-005", "key-009", "key-003", "key-007",
		}
		for _, key := range deleteOrder {
			got, ok := tree.Delete([]byte(key))
			require.True(t, ok)
			require.Equal(t, want[key], got)
			delete(want, key)
			assertMemTreeState(t, tree, want)
		}
	})

	t.Run("delete_in_sorted_order", func(t *testing.T) {
		tree := newMemTree[testVal]()
		want := make(map[string]testVal)
		for i := 0; i < 32; i++ {
			key := fmt.Sprintf("sorted-%02d", i)
			require.NoError(t, tree.Put([]byte(key), tv(i)))
			want[key] = tv(i)
		}

		for i := 0; i < 32; i++ {
			key := fmt.Sprintf("sorted-%02d", i)
			got, ok := tree.Delete([]byte(key))
			require.True(t, ok)
			require.Equal(t, want[key], got)
			delete(want, key)
			assertMemTreeState(t, tree, want)
		}
	})

	t.Run("delete_in_reverse_sorted_order", func(t *testing.T) {
		tree := newMemTree[testVal]()
		want := make(map[string]testVal)
		for i := 0; i < 32; i++ {
			key := fmt.Sprintf("rev-%02d", i)
			require.NoError(t, tree.Put([]byte(key), tv(i)))
			want[key] = tv(i)
		}

		for i := 31; i >= 0; i-- {
			key := fmt.Sprintf("rev-%02d", i)
			got, ok := tree.Delete([]byte(key))
			require.True(t, ok)
			require.Equal(t, want[key], got)
			delete(want, key)
			assertMemTreeState(t, tree, want)
		}
	})
}

func TestMemTree_AddDuplicate(t *testing.T) {
	tree := newMemTree[testVal]()
	key := []byte("dup")
	require.NoError(t, tree.Add(key, tv(1)))
	err := tree.Add(key, tv(2))
	require.ErrorIs(t, err, ErrRBTreeSameRBNode)
}
