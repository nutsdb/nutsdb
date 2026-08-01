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

// memValue is stored in MemTable: ValueRef plus write sequence.
type memValue struct {
	Ref ValueRef
	Seq uint64
}

// MemTable is an ordered mutable table of key → memValue.
type MemTable struct {
	tree   MemTree[memValue]
	approx int64
	frozen bool
}

func newMemTable() *MemTable {
	return &MemTable{tree: newMemTree[memValue]()}
}

func (m *MemTable) ApproxBytes() int64 {
	if m == nil {
		return 0
	}
	return m.approx
}

func (m *MemTable) Size() int {
	if m == nil || m.tree == nil {
		return 0
	}
	return m.tree.Size()
}

func (m *MemTable) Put(key []byte, ref ValueRef, seq uint64) error {
	if m == nil || m.frozen {
		return ErrStoreClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	old, err := m.tree.Get(key)
	had := err == nil
	v := memValue{Ref: cloneValueRef(ref), Seq: seq}
	keyCopy := append([]byte(nil), key...)
	if had {
		m.approx -= int64(len(key)) + valueRefApproxSize(old.Ref)
	}
	if err := m.tree.Put(keyCopy, v); err != nil {
		return err
	}
	m.approx += int64(len(key)) + valueRefApproxSize(ref)
	return nil
}

func (m *MemTable) Get(key []byte) (memValue, bool) {
	if m == nil || m.tree == nil {
		return memValue{}, false
	}
	v, err := m.tree.Get(key)
	if err != nil {
		return memValue{}, false
	}
	return v, true
}

func (m *MemTable) Iterate(cb func(key []byte, v memValue) bool) {
	if m == nil || m.tree == nil {
		return
	}
	m.tree.Iterate(cb)
}

func (m *MemTable) Freeze() {
	if m != nil {
		m.frozen = true
	}
}

func cloneValueRef(ref ValueRef) ValueRef {
	out := ref
	if len(ref.Inline) > 0 {
		out.Inline = append([]byte(nil), ref.Inline...)
	}
	return out
}
