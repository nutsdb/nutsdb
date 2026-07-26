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
	"errors"
	"testing"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/fileio"
	"github.com/stretchr/testify/require"
)

// failPutMem wraps MemStore and fails the Nth Put call (1-based).
type failPutMem struct {
	MemStore
	failOn int
	puts   int
}

func (f *failPutMem) Put(key []byte, value fileio.Location) error {
	f.puts++
	if f.failOn > 0 && f.puts == f.failOn {
		return errors.New("injected mem put failure")
	}
	return f.MemStore.Put(key, value)
}

func TestStoreMgr_MemRevertOnPutFailure(t *testing.T) {
	dir := t.TempDir()
	opts := testDiskOpts(dir)

	base := NewMemStore()
	// Recovery may Put into mem; fail on the first user Put after open.
	// Open with normal mem first, then we need inject after recover.
	// Use failOn high enough to skip recovery puts: empty dir → 0 recovery puts.
	wrapped := &failPutMem{MemStore: base, failOn: 1}

	st, err := openStoreManagerWithMem(opts, wrapped)
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()

	ctx := context.Background()
	err = st.Put(ctx, []byte("k"), core.NewRecord().WithValue([]byte("v")))
	require.Error(t, err)
	require.Contains(t, err.Error(), "injected mem put failure")

	// Mem must not contain the key (reverted / never committed).
	mgr := st.(*storeMgr)
	_, err = mgr.mem.Get([]byte("k"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestStoreMgr_MemRevertKeepsOldLocation(t *testing.T) {
	dir := t.TempDir()
	opts := testDiskOpts(dir)

	st1, err := OpenStoreManager(opts)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, st1.Put(ctx, []byte("k"), core.NewRecord().WithValue([]byte("v1"))))
	require.NoError(t, st1.Close())

	base := NewMemStore()
	// Reopen via withMem: recover will Put once for "k". failOn=2 → next Put (overwrite) fails.
	wrapped := &failPutMem{MemStore: base, failOn: 2}
	st2, err := openStoreManagerWithMem(opts, wrapped)
	require.NoError(t, err)
	defer func() { require.NoError(t, st2.Close()) }()

	mgr := st2.(*storeMgr)
	oldLoc, err := mgr.mem.Get([]byte("k"))
	require.NoError(t, err)

	err = st2.Put(ctx, []byte("k"), core.NewRecord().WithValue([]byte("v2")))
	require.Error(t, err)

	gotLoc, err := mgr.mem.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, oldLoc, gotLoc, "mem must revert to previous Location")

	// Readable value still v1 via Get.
	rec, err := st2.Get(ctx, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), rec.Value)
}

func TestStoreMgr_DiskAppendFailureLeavesMemUnchanged(t *testing.T) {
	dir := t.TempDir()
	opts := testDiskOpts(dir)
	opts.MaxRecordSize = 32 // tiny: force ErrRecordTooLarge

	st, err := OpenStoreManager(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()

	ctx := context.Background()
	big := make([]byte, 64)
	err = st.Put(ctx, []byte("k"), core.NewRecord().WithValue(big))
	require.Error(t, err)

	mgr := st.(*storeMgr)
	_, err = mgr.mem.Get([]byte("k"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestOpenDiskStore_AliasesStoreManager(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenDiskStore(testDiskOpts(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()
	_, ok := st.(*storeMgr)
	require.True(t, ok)
}
