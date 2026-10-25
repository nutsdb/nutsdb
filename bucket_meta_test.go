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

package nutsdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBucketMeta_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	m := newBucketManager(dir)
	require.NoError(t, m.load())

	id1 := m.allocID()
	id2 := m.allocID()
	require.NoError(t, m.applyCommit(map[bucketKey]uint64{
		{ds: DataStructureBTree, name: "a"}: id1,
		{ds: DataStructureBTree, name: "b"}: id2,
	}, nil))

	m2 := newBucketManager(dir)
	require.NoError(t, m2.load())
	got1, ok := m2.getID(DataStructureBTree, "a")
	require.True(t, ok)
	require.Equal(t, id1, got1)
	got2, ok := m2.getID(DataStructureBTree, "b")
	require.True(t, ok)
	require.Equal(t, id2, got2)
	require.Equal(t, []string{"a", "b"}, m2.list(DataStructureBTree))

	require.NoError(t, m2.applyCommit(nil, map[bucketKey]struct{}{
		{ds: DataStructureBTree, name: "a"}: {},
	}))

	m3 := newBucketManager(dir)
	require.NoError(t, m3.load())
	_, ok = m3.getID(DataStructureBTree, "a")
	require.False(t, ok)
	_, ok = m3.getID(DataStructureBTree, "b")
	require.True(t, ok)
}

func TestBucketMeta_CorruptRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, bucketsMetaFileName)
	require.NoError(t, os.WriteFile(path, []byte("xxxx"), bucketsMetaFilePerm))
	m := newBucketManager(dir)
	require.Error(t, m.load())
}

func TestEncodeEngineKey(t *testing.T) {
	ek := encodeEngineKey(1, []byte("abc"))
	require.Len(t, ek, 8+3)
	require.Equal(t, []byte("abc"), ek[8:])
	require.Equal(t, encodeBucketPrefix(1), ek[:8])
}
