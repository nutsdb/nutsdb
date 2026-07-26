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

package fileio

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegment_MapReadonlyRefuseUnsealed(t *testing.T) {
	dir := t.TempDir()
	seg, err := createSegment(dir, 1, HeaderSize+FooterSize+64)
	require.NoError(t, err)
	defer func() { require.NoError(t, seg.close()) }()

	require.False(t, seg.sealed)
	require.Error(t, seg.mapReadonly())
	require.Nil(t, seg.mmapData)
}

func TestStore_MMapSealedReadAndReopen(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SegmentSize = HeaderSize + FooterSize + 256
	opts.MaxRecordSize = 64
	opts.WriteBufferSize = 32
	opts.SyncMode = SyncEveryWrite
	opts.ReadBackend = ReadBackendMMapSealed

	stIface, err := Open(opts)
	require.NoError(t, err)
	st := stIface.(*store)

	loc, err := st.Append(bytes.Repeat([]byte("m"), 40), RecordPut)
	require.NoError(t, err)
	firstID := loc.FileID

	var sealedID uint32
	for i := 0; i < 20; i++ {
		next, err := st.Append(bytes.Repeat([]byte("n"), 40), RecordPut)
		require.NoError(t, err)
		if next.FileID != firstID {
			sealedID = firstID
			break
		}
	}
	require.NotZero(t, sealedID)

	sealed := st.segments[sealedID]
	require.NotNil(t, sealed)
	require.True(t, sealed.sealed)
	require.NotNil(t, sealed.mmapData, "sealed segment should be mmap'd")

	got, typ, err := st.Read(loc)
	require.NoError(t, err)
	require.Equal(t, RecordPut, typ)
	require.Equal(t, bytes.Repeat([]byte("m"), 40), got)

	require.NoError(t, st.Close())

	st2Iface, err := Open(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, st2Iface.Close()) }()
	st2 := st2Iface.(*store)

	reopened := st2.segments[sealedID]
	require.NotNil(t, reopened)
	require.True(t, reopened.sealed)
	require.NotNil(t, reopened.mmapData)

	got, typ, err = st2.Read(loc)
	require.NoError(t, err)
	require.Equal(t, RecordPut, typ)
	require.Equal(t, bytes.Repeat([]byte("m"), 40), got)

	var iterated int
	err = st2.Iterate(sealedID, func(_ Location, _ RecordType, _ []byte) error {
		iterated++
		return nil
	})
	require.NoError(t, err)
	require.Greater(t, iterated, 0)
}

func TestStore_MMapSealedEvictUnmaps(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SegmentSize = HeaderSize + FooterSize + 128
	opts.MaxRecordSize = 40
	opts.WriteBufferSize = 32
	opts.SyncMode = SyncEveryWrite
	opts.ReadBackend = ReadBackendMMapSealed
	opts.MaxOpenSegments = 2

	stIface, err := Open(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, stIface.Close()) }()
	st := stIface.(*store)

	var locs []Location
	for i := 0; i < 40; i++ {
		loc, err := st.Append(bytes.Repeat([]byte{byte(i)}, 20), RecordPut)
		require.NoError(t, err)
		locs = append(locs, loc)
	}
	require.NoError(t, st.Sync())

	// recover/create may leave more than MaxOpenSegments until eviction runs.
	st.evictLRULocked()
	require.LessOrEqual(t, len(st.segments), opts.MaxOpenSegments)

	for i, loc := range locs {
		got, _, err := st.Read(loc)
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte{byte(i)}, 20), got)
	}
	require.LessOrEqual(t, len(st.segments), opts.MaxOpenSegments)
}
