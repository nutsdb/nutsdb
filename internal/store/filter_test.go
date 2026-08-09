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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBloomFilter_MayContain(t *testing.T) {
	p := NewBloomFilterPolicy(10)
	b := p.NewBuilder()
	for i := 0; i < 1000; i++ {
		b.Add(fmt.Appendf(nil, "key-%04d", i))
	}
	payload := b.Finish()
	f, err := p.Open(payload)
	require.NoError(t, err)

	for i := 0; i < 1000; i++ {
		require.True(t, f.MayContain(fmt.Appendf(nil, "key-%04d", i)), "false negative at %d", i)
	}

	// Missing keys should usually be rejected.
	fp := 0
	for i := 0; i < 1000; i++ {
		if f.MayContain(fmt.Appendf(nil, "miss-%04d", i)) {
			fp++
		}
	}
	require.Less(t, fp, 50, "false positive rate too high: %d/1000", fp)
}

func TestBloomFilter_EncodeRoundTrip(t *testing.T) {
	p := NewBloomFilterPolicy(10)
	b := p.NewBuilder()
	b.Add([]byte("a"))
	b.Add([]byte("b"))
	block := encodeFilterBlock(p, b.Finish())

	f, err := decodeFilterBlock(block)
	require.NoError(t, err)
	require.True(t, f.MayContain([]byte("a")))
	require.True(t, f.MayContain([]byte("b")))
	require.False(t, f.MayContain([]byte("zzz-not-present-xyz")))
}

func TestFilter_UnknownPolicyFallsBack(t *testing.T) {
	// Craft a block with an unregistered name.
	raw := append([]byte{3, 0}, []byte("nope")...)
	raw = append(raw, []byte("payload")...)
	f, err := decodeFilterBlock(raw)
	require.NoError(t, err)
	require.True(t, f.MayContain([]byte("anything")))
}

func TestSST_BloomFilterGet(t *testing.T) {
	dir := t.TempDir()
	w, err := newSSTWriter(dir, 1)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("k%04d", i))
		ref := ValueRef{Kind: ValueKindInline, Inline: []byte("v"), Timestamp: 1}
		require.NoError(t, w.Add(k, ref, uint64(i+1)))
	}
	meta, err := w.Finish()
	require.NoError(t, err)
	require.Equal(t, uint64(1), meta.FileNumber)

	rd, err := openSSTReader(dir, 1)
	require.NoError(t, err)
	require.NotNil(t, rd.filter)

	ref, seq, ok, err := rd.Get([]byte("k0042"))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(43), seq)
	require.Equal(t, []byte("v"), ref.Inline)

	_, _, ok, err = rd.Get([]byte("missing-key-should-miss-bloom"))
	require.NoError(t, err)
	require.False(t, ok)
}

func TestSST_NoFilterPolicy(t *testing.T) {
	dir := t.TempDir()
	w, err := newSSTWriterWithFilter(dir, 2, nil)
	require.NoError(t, err)
	ref := ValueRef{Kind: ValueKindInline, Inline: []byte("x"), Timestamp: 1}
	require.NoError(t, w.Add([]byte("only"), ref, 1))
	_, err = w.Finish()
	require.NoError(t, err)

	rd, err := openSSTReader(dir, 2)
	require.NoError(t, err)
	require.Nil(t, rd.filter)
	_, _, ok, err := rd.Get([]byte("only"))
	require.NoError(t, err)
	require.True(t, ok)
}
