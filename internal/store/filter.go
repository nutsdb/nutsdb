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
	"sync"
)

// Filter answers approximate membership queries for keys in one SST.
// False negatives are not allowed; false positives are.
type Filter interface {
	MayContain(key []byte) bool
}

// FilterBuilder accumulates keys while writing an SST, then encodes a Filter.
type FilterBuilder interface {
	Add(key []byte)
	// Finish returns the policy-specific payload (without the name header).
	Finish() []byte
}

// FilterPolicy is a pluggable filter implementation (Bloom, etc.).
// SST persists Name() with the payload so readers can reopen the right type.
type FilterPolicy interface {
	Name() string
	NewBuilder() FilterBuilder
	// Open reconstructs a Filter from a Finish() payload.
	Open(payload []byte) (Filter, error)
}

const (
	filterNameLenSize = 2

	sstFlagHasFilter uint16 = 1 << 0
)

var (
	filterPoliciesMu sync.RWMutex
	filterPolicies   = map[string]FilterPolicy{}
)

func init() {
	RegisterFilterPolicy(NewBloomFilterPolicy(defaultBloomBitsPerKey))
}

// RegisterFilterPolicy registers a FilterPolicy by Name() for SST decode.
// Later registrations with the same name replace the previous one.
func RegisterFilterPolicy(p FilterPolicy) {
	if p == nil {
		return
	}
	filterPoliciesMu.Lock()
	filterPolicies[p.Name()] = p
	filterPoliciesMu.Unlock()
}

// LookupFilterPolicy returns a registered policy by name.
func LookupFilterPolicy(name string) (FilterPolicy, bool) {
	filterPoliciesMu.RLock()
	p, ok := filterPolicies[name]
	filterPoliciesMu.RUnlock()
	return p, ok
}

// defaultFilterPolicy is used by SST writers when none is specified.
var defaultFilterPolicy FilterPolicy = NewBloomFilterPolicy(defaultBloomBitsPerKey)

// encodeFilterBlock packs policy name + payload for the SST Filter Block.
func encodeFilterBlock(policy FilterPolicy, payload []byte) []byte {
	name := policy.Name()
	n := filterNameLenSize + len(name) + len(payload)
	out := make([]byte, n)
	binary.LittleEndian.PutUint16(out[0:2], uint16(len(name)))
	copy(out[2:], name)
	copy(out[2+len(name):], payload)
	return out
}

// decodeFilterBlock parses a Filter Block into a Filter.
// Empty data yields a nil Filter (treated as "no filter").
func decodeFilterBlock(data []byte) (Filter, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < filterNameLenSize {
		return nil, ErrCorruptEntry
	}
	nameLen := int(binary.LittleEndian.Uint16(data[0:2]))
	if nameLen <= 0 || filterNameLenSize+nameLen > len(data) {
		return nil, ErrCorruptEntry
	}
	name := string(data[filterNameLenSize : filterNameLenSize+nameLen])
	payload := data[filterNameLenSize+nameLen:]
	p, ok := LookupFilterPolicy(name)
	if !ok {
		// Unknown policy: keep correctness by not filtering.
		return alwaysContainFilter{}, nil
	}
	return p.Open(payload)
}

// alwaysContainFilter is used when the on-disk policy is unknown.
type alwaysContainFilter struct{}

func (alwaysContainFilter) MayContain([]byte) bool { return true }
