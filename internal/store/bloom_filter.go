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
	"encoding/binary"
	"hash/fnv"
)

const (
	bloomPolicyName         = "nutsdb.BuiltinBloomV1"
	defaultBloomBitsPerKey  = 10 // ~1% false-positive rate
	bloomPayloadVersion     = 1
	bloomPayloadHeaderSize  = 1 + 1 + 1 + 4 // ver | bits_per_key | probes | nbytes
	bloomMinBits            = 64
)

// bloomFilterPolicy is a simple Bloom FilterPolicy.
type bloomFilterPolicy struct {
	bitsPerKey int
}

// NewBloomFilterPolicy returns a Bloom FilterPolicy.
// bitsPerKey ≈ 10 targets about 1% false positives (LevelDB-style).
func NewBloomFilterPolicy(bitsPerKey int) FilterPolicy {
	if bitsPerKey < 1 {
		bitsPerKey = defaultBloomBitsPerKey
	}
	return &bloomFilterPolicy{bitsPerKey: bitsPerKey}
}

func (p *bloomFilterPolicy) Name() string { return bloomPolicyName }

func (p *bloomFilterPolicy) NewBuilder() FilterBuilder {
	return &bloomBuilder{bitsPerKey: p.bitsPerKey}
}

func (p *bloomFilterPolicy) Open(payload []byte) (Filter, error) {
	return openBloomFilter(payload)
}

type bloomBuilder struct {
	bitsPerKey int
	keys       [][]byte
}

func (b *bloomBuilder) Add(key []byte) {
	b.keys = append(b.keys, append([]byte(nil), key...))
}

func (b *bloomBuilder) Finish() []byte {
	n := len(b.keys)
	bitsPerKey := b.bitsPerKey
	if bitsPerKey < 1 {
		bitsPerKey = defaultBloomBitsPerKey
	}
	// probes ≈ bits_per_key * ln(2)
	probes := uint8(float64(bitsPerKey) * 0.69)
	if probes < 1 {
		probes = 1
	}
	if probes > 30 {
		probes = 30
	}

	nbits := n * bitsPerKey
	if nbits < bloomMinBits {
		nbits = bloomMinBits
	}
	nbytes := (nbits + 7) / 8
	bitset := make([]byte, nbytes)

	for _, key := range b.keys {
		addBloomHash(bitset, bloomHash(key), probes)
	}

	out := make([]byte, bloomPayloadHeaderSize+nbytes)
	out[0] = bloomPayloadVersion
	out[1] = uint8(bitsPerKey)
	out[2] = probes
	binary.LittleEndian.PutUint32(out[3:7], uint32(nbytes))
	copy(out[bloomPayloadHeaderSize:], bitset)
	return out
}

type bloomFilter struct {
	probes uint8
	bitset []byte
}

func openBloomFilter(payload []byte) (Filter, error) {
	if len(payload) < bloomPayloadHeaderSize {
		return nil, ErrCorruptEntry
	}
	if payload[0] != bloomPayloadVersion {
		return nil, ErrCorruptEntry
	}
	probes := payload[2]
	if probes == 0 || probes > 30 {
		return nil, ErrCorruptEntry
	}
	nbytes := int(binary.LittleEndian.Uint32(payload[3:7]))
	if nbytes < 0 || bloomPayloadHeaderSize+nbytes != len(payload) {
		return nil, ErrCorruptEntry
	}
	return &bloomFilter{
		probes: probes,
		bitset: payload[bloomPayloadHeaderSize:],
	}, nil
}

func (f *bloomFilter) MayContain(key []byte) bool {
	if f == nil || len(f.bitset) == 0 {
		return true
	}
	return mayContainBloomHash(f.bitset, bloomHash(key), f.probes)
}

func bloomHash(key []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(key)
	return h.Sum64()
}

// Kirsch–Mitzenmacher double hashing: h_i = h1 + i*h2.
func bloomHashes(h uint64) (h1, h2 uint64) {
	h1 = h
	h2 = (h >> 17) | (h << 47)
	if h2&1 == 0 {
		h2++
	}
	return h1, h2
}

func addBloomHash(bitset []byte, h uint64, probes uint8) {
	nbits := uint64(len(bitset) * 8)
	h1, h2 := bloomHashes(h)
	for i := uint8(0); i < probes; i++ {
		bit := (h1 + uint64(i)*h2) % nbits
		bitset[bit/8] |= 1 << (bit % 8)
	}
}

func mayContainBloomHash(bitset []byte, h uint64, probes uint8) bool {
	nbits := uint64(len(bitset) * 8)
	h1, h2 := bloomHashes(h)
	for i := uint8(0); i < probes; i++ {
		bit := (h1 + uint64(i)*h2) % nbits
		if bitset[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}
