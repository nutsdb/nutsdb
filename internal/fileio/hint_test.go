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
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// testDecodeKey treats the entire payload as the key for Put/Delete;
// Meta records are skipped.
func testDecodeKey(payload []byte, typ RecordType) ([]byte, error) {
	if typ == RecordMeta {
		return nil, ErrHintSkipMeta
	}
	if len(payload) == 0 {
		return nil, ErrHintInvalidEntry
	}
	return payload, nil
}

func TestEncodeDecodeHintEntry(t *testing.T) {
	e := HintEntry{
		Key:  []byte("hello"),
		Loc:  Location{FileID: 1, Offset: HeaderSize, Length: RecordHeaderSize + 5},
		Type: RecordPut,
	}
	raw, err := encodeHintEntry(nil, e)
	require.NoError(t, err)
	require.Equal(t, HintEntryHeaderSize+len(e.Key), len(raw))

	got, n, err := decodeHintEntry(raw, 1)
	require.NoError(t, err)
	require.Equal(t, len(raw), n)
	require.Equal(t, e.Key, got.Key)
	require.Equal(t, e.Loc, got.Loc)
	require.Equal(t, e.Type, got.Type)

	raw[0] ^= 0xff
	_, _, err = decodeHintEntry(raw, 1)
	require.ErrorIs(t, err, ErrHintCorrupt)
}

func TestHintWriterReaderRoundTrip(t *testing.T) {
	dir := t.TempDir()
	hm := NewHintManager(dir)

	w, err := hm.CreateWriter(1)
	require.NoError(t, err)

	entries := []HintEntry{
		{Key: []byte("a"), Loc: Location{FileID: 1, Offset: HeaderSize, Length: 13}, Type: RecordPut},
		{Key: []byte("bb"), Loc: Location{FileID: 1, Offset: HeaderSize + 13, Length: 14}, Type: RecordPut},
		{Key: []byte("a"), Loc: Location{FileID: 1, Offset: HeaderSize + 27, Length: 13}, Type: RecordDelete},
	}
	for _, e := range entries {
		require.NoError(t, w.Append(e))
	}
	require.NoError(t, w.Finish())

	require.FileExists(t, hintPath(dir, 1))
	_, err = os.Stat(hintTmpPath(dir, 1))
	require.True(t, os.IsNotExist(err))

	rd, err := hm.OpenReader(1)
	require.NoError(t, err)
	defer func() { require.NoError(t, rd.Close()) }()

	var got []HintEntry
	require.NoError(t, rd.Iterate(func(e HintEntry) error {
		got = append(got, HintEntry{
			Key:  append([]byte(nil), e.Key...),
			Loc:  e.Loc,
			Type: e.Type,
		})
		return nil
	}))
	require.Equal(t, entries, got)
}

func TestHintManager_BuildFromSegmentAndVerify(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SegmentSize = HeaderSize + FooterSize + 256
	opts.MaxRecordSize = 64
	opts.WriteBufferSize = 32
	opts.SyncMode = SyncEveryWrite

	st, err := Open(opts)
	require.NoError(t, err)

	loc1, err := st.Append([]byte("key1"), RecordPut)
	require.NoError(t, err)
	_, err = st.Append([]byte("meta"), RecordMeta)
	require.NoError(t, err)
	loc2, err := st.Append([]byte("key2"), RecordPut)
	require.NoError(t, err)
	loc3, err := st.Append([]byte("key1"), RecordDelete)
	require.NoError(t, err)

	// Force seal of file 1 by filling until rotate.
	firstID := loc1.FileID
	for i := 0; i < 30; i++ {
		next, err := st.Append(bytes.Repeat([]byte("x"), 20), RecordPut)
		require.NoError(t, err)
		if next.FileID != firstID {
			break
		}
	}
	require.NoError(t, st.Sync())

	hm := NewHintManager(dir)
	require.NoError(t, hm.BuildFromSegment(st, firstID, testDecodeKey))
	require.NoError(t, hm.VerifyHintMatchesSegment(st, firstID, testDecodeKey))

	rd, err := hm.OpenReader(firstID)
	require.NoError(t, err)
	defer func() { require.NoError(t, rd.Close()) }()

	var keys []string
	var locs []Location
	var types []RecordType
	require.NoError(t, rd.Iterate(func(e HintEntry) error {
		keys = append(keys, string(e.Key))
		locs = append(locs, e.Loc)
		types = append(types, e.Type)
		return nil
	}))
	// Meta skipped; first three KV records from file 1 before rotate filler.
	require.Equal(t, []string{"key1", "key2", "key1"}, keys[:3])
	require.Equal(t, []Location{loc1, loc2, loc3}, locs[:3])
	require.Equal(t, []RecordType{RecordPut, RecordPut, RecordDelete}, types[:3])

	require.NoError(t, st.Close())
}

func TestHintManager_LoadAllOrderAndDelete(t *testing.T) {
	dir := t.TempDir()
	hm := NewHintManager(dir)

	for _, id := range []uint32{2, 1} {
		w, err := hm.CreateWriter(id)
		require.NoError(t, err)
		require.NoError(t, w.Append(HintEntry{
			Key:  []byte{byte('0' + id)},
			Loc:  Location{FileID: id, Offset: HeaderSize, Length: 13},
			Type: RecordPut,
		}))
		require.NoError(t, w.Finish())
	}

	var order []uint32
	require.NoError(t, hm.LoadAll(func(e HintEntry) error {
		order = append(order, e.Loc.FileID)
		return nil
	}))
	require.Equal(t, []uint32{1, 2}, order)

	require.NoError(t, hm.Delete(1))
	_, err := hm.OpenReader(1)
	require.ErrorIs(t, err, ErrHintNotFound)
}

func TestHintManager_VerifyMismatch(t *testing.T) {
	dir := t.TempDir()
	hm := NewHintManager(dir)

	w, err := hm.CreateWriter(1)
	require.NoError(t, err)
	require.NoError(t, w.Append(HintEntry{
		Key:  []byte("only-in-hint"),
		Loc:  Location{FileID: 1, Offset: HeaderSize, Length: 20},
		Type: RecordPut,
	}))
	require.NoError(t, w.Finish())

	opts := DefaultOptions(dir)
	opts.SegmentSize = 64 << 10
	opts.SyncMode = SyncEveryWrite
	st, err := Open(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()

	_, err = st.Append([]byte("seg-key"), RecordPut)
	require.NoError(t, err)

	// Segment fileID is 1 after Open creates first active.
	err = hm.VerifyHintMatchesSegment(st, 1, testDecodeKey)
	require.ErrorIs(t, err, ErrHintSegKeyMismatch)
}

func TestHintIncompleteRejected(t *testing.T) {
	dir := t.TempDir()
	hm := NewHintManager(dir)
	w, err := hm.CreateWriter(1)
	require.NoError(t, err)
	require.NoError(t, w.Append(HintEntry{
		Key:  []byte("x"),
		Loc:  Location{FileID: 1, Offset: HeaderSize, Length: 13},
		Type: RecordPut,
	}))
	require.NoError(t, w.Close()) // abort without Finish

	_, err = hm.OpenReader(1)
	require.True(t, errors.Is(err, ErrHintNotFound) || errors.Is(err, ErrHintIncomplete))

	// Corrupt finished file: truncate footer.
	w2, err := hm.CreateWriter(2)
	require.NoError(t, err)
	require.NoError(t, w2.Append(HintEntry{
		Key:  []byte("y"),
		Loc:  Location{FileID: 2, Offset: HeaderSize, Length: 13},
		Type: RecordPut,
	}))
	require.NoError(t, w2.Finish())

	path := hintPath(dir, 2)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw[:len(raw)-8], 0o644))

	_, err = hm.OpenReader(2)
	require.Error(t, err)
}

func TestHintPathPadding(t *testing.T) {
	dir := t.TempDir()
	p := hintPath(dir, 1)
	require.Equal(t, filepath.Join(dir, "0000000001.hint"), p)

	// Ensure header file_id round-trips in writer.
	hm := NewHintManager(dir)
	w, err := hm.CreateWriter(42)
	require.NoError(t, err)
	require.NoError(t, w.Finish())

	hdr, err := os.ReadFile(hintPath(dir, 42))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(hdr), HintHeaderSize)
	require.Equal(t, uint32(42), binary.LittleEndian.Uint32(hdr[8:12]))
}
