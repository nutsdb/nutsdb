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

package fileio_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/nutsdb/nutsdb/internal/fileio"
	"github.com/stretchr/testify/require"
)

func testOptions(dir string) fileio.Options {
	opts := fileio.DefaultOptions(dir)
	// Keep tests fast: small segment, still larger than header+footer+records.
	opts.SegmentSize = 64 << 10 // 64KiB
	opts.WriteBufferSize = 4 << 10
	opts.MaxRecordSize = 8 << 10
	opts.SyncMode = fileio.SyncNoSync
	return opts
}

func TestStore_AppendRead(t *testing.T) {
	dir := t.TempDir()
	st, err := fileio.Open(testOptions(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()

	payload := []byte("hello-store")
	loc, err := st.Append(payload, fileio.RecordPut)
	require.NoError(t, err)
	require.Equal(t, uint32(1), loc.FileID)
	require.Equal(t, uint64(fileio.HeaderSize), loc.Offset)
	require.Equal(t, uint32(fileio.RecordHeaderSize+len(payload)), loc.Length)

	got, typ, err := st.Read(loc)
	require.NoError(t, err)
	require.Equal(t, fileio.RecordPut, typ)
	require.Equal(t, payload, got)
}

func TestStore_AppendEmptyPayload(t *testing.T) {
	dir := t.TempDir()
	st, err := fileio.Open(testOptions(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()

	loc, err := st.Append(nil, fileio.RecordDelete)
	require.NoError(t, err)

	got, typ, err := st.Read(loc)
	require.NoError(t, err)
	require.Equal(t, fileio.RecordDelete, typ)
	require.Empty(t, got)
}

func TestStore_AppendMultipleAndIterate(t *testing.T) {
	dir := t.TempDir()
	st, err := fileio.Open(testOptions(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()

	records := [][]byte{
		[]byte("a"),
		[]byte("bb"),
		{},
		[]byte("cccc"),
	}
	locs := make([]fileio.Location, 0, len(records))
	for _, r := range records {
		loc, err := st.Append(r, fileio.RecordPut)
		require.NoError(t, err)
		locs = append(locs, loc)
	}
	require.NoError(t, st.Sync())

	for i, loc := range locs {
		got, typ, err := st.Read(loc)
		require.NoError(t, err)
		require.Equal(t, fileio.RecordPut, typ)
		require.Equal(t, records[i], got)
	}

	var iterated [][]byte
	err = st.Iterate(locs[0].FileID, func(loc fileio.Location, typ fileio.RecordType, payload []byte) error {
		cp := make([]byte, len(payload))
		copy(cp, payload)
		iterated = append(iterated, cp)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, records, iterated)
}

func TestStore_LargeRecordBypassBuffer(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(dir)
	opts.WriteBufferSize = 64
	opts.MaxRecordSize = 512
	st, err := fileio.Open(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()

	payload := bytes.Repeat([]byte("x"), 200)
	loc, err := st.Append(payload, fileio.RecordPut)
	require.NoError(t, err)

	got, _, err := st.Read(loc)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestStore_RecordTooLarge(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(dir)
	opts.MaxRecordSize = 32
	st, err := fileio.Open(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()

	payload := bytes.Repeat([]byte("y"), 64)
	_, err = st.Append(payload, fileio.RecordPut)
	require.ErrorIs(t, err, fileio.ErrRecordTooLarge)
}

func TestStore_RotateSegment(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(dir)
	opts.SegmentSize = fileio.HeaderSize + fileio.FooterSize + 128
	opts.MaxRecordSize = 64
	opts.WriteBufferSize = 32
	st, err := fileio.Open(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()

	var locs []fileio.Location
	for i := 0; i < 10; i++ {
		payload := bytes.Repeat([]byte{byte(i)}, 20)
		loc, err := st.Append(payload, fileio.RecordPut)
		require.NoError(t, err)
		locs = append(locs, loc)
	}
	require.NoError(t, st.Sync())

	fileIDs := map[uint32]struct{}{}
	for i, loc := range locs {
		fileIDs[loc.FileID] = struct{}{}
		got, _, err := st.Read(loc)
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte{byte(i)}, 20), got)
	}
	require.GreaterOrEqual(t, len(fileIDs), 2)
}

func TestStore_PersistAndReopen(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(dir)
	opts.SyncMode = fileio.SyncEveryWrite

	st1, err := fileio.Open(opts)
	require.NoError(t, err)

	loc1, err := st1.Append([]byte("first"), fileio.RecordPut)
	require.NoError(t, err)
	loc2, err := st1.Append([]byte("second"), fileio.RecordMeta)
	require.NoError(t, err)
	require.NoError(t, st1.Close())

	st2, err := fileio.Open(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, st2.Close()) }()

	got, typ, err := st2.Read(loc1)
	require.NoError(t, err)
	require.Equal(t, fileio.RecordPut, typ)
	require.Equal(t, []byte("first"), got)

	got, typ, err = st2.Read(loc2)
	require.NoError(t, err)
	require.Equal(t, fileio.RecordMeta, typ)
	require.Equal(t, []byte("second"), got)

	loc3, err := st2.Append([]byte("third"), fileio.RecordPut)
	require.NoError(t, err)
	require.Equal(t, loc2.FileID, loc3.FileID)
	require.Greater(t, loc3.Offset, loc2.Offset)
}

func TestStore_SealPersistsFooter(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(dir)
	opts.SegmentSize = fileio.HeaderSize + fileio.FooterSize + 256
	opts.MaxRecordSize = 64
	opts.SyncMode = fileio.SyncEveryWrite

	st, err := fileio.Open(opts)
	require.NoError(t, err)

	loc, err := st.Append(bytes.Repeat([]byte("a"), 40), fileio.RecordPut)
	require.NoError(t, err)

	// Keep appending until a new segment is created (old one sealed).
	firstID := loc.FileID
	for i := 0; i < 20; i++ {
		next, err := st.Append(bytes.Repeat([]byte("b"), 40), fileio.RecordPut)
		require.NoError(t, err)
		if next.FileID != firstID {
			break
		}
	}
	require.NoError(t, st.Close())

	st2, err := fileio.Open(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, st2.Close()) }()

	got, _, err := st2.Read(loc)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte("a"), 40), got)

	entries, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	require.NoError(t, err)
	require.NotEmpty(t, entries)
}

func TestStore_InvalidLocation(t *testing.T) {
	dir := t.TempDir()
	st, err := fileio.Open(testOptions(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()

	_, _, err = st.Read(fileio.Location{})
	require.ErrorIs(t, err, fileio.ErrInvalidLocation)

	_, _, err = st.Read(fileio.Location{FileID: 1, Offset: 0, Length: 12})
	require.ErrorIs(t, err, fileio.ErrInvalidLocation)
}

func TestStore_ReadInto(t *testing.T) {
	dir := t.TempDir()
	st, err := fileio.Open(testOptions(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()

	payload := []byte("read-into")
	loc, err := st.Append(payload, fileio.RecordPut)
	require.NoError(t, err)
	require.NoError(t, st.Sync())

	buf := make([]byte, 64)
	got, typ, err := st.ReadInto(loc, buf)
	require.NoError(t, err)
	require.Equal(t, fileio.RecordPut, typ)
	require.Equal(t, payload, got)
	require.Equal(t, &buf[0], &got[0])
}

func TestStore_DeleteSegment(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(dir)
	opts.SegmentSize = fileio.HeaderSize + fileio.FooterSize + 128
	opts.MaxRecordSize = 40
	opts.SyncMode = fileio.SyncEveryWrite
	st, err := fileio.Open(opts)
	require.NoError(t, err)

	loc, err := st.Append(bytes.Repeat([]byte("z"), 20), fileio.RecordPut)
	require.NoError(t, err)

	rotated := false
	for i := 0; i < 30; i++ {
		next, err := st.Append(bytes.Repeat([]byte("z"), 20), fileio.RecordPut)
		require.NoError(t, err)
		if next.FileID != loc.FileID {
			rotated = true
			break
		}
	}
	require.True(t, rotated)
	require.NoError(t, st.Close())

	st2, err := fileio.Open(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, st2.Close()) }()

	require.NoError(t, st2.DeleteSegment(loc.FileID))
	_, _, err = st2.Read(loc)
	require.ErrorIs(t, err, fileio.ErrSegmentNotFound)
}
