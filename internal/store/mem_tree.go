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

// MemTree is an ordered key→value map used by MemTable.
// RBTree is the default implementation.
type MemTree[V any] interface {
	// Size returns the number of keys.
	Size() int
	// Put upserts a key→value pair.
	Put(key []byte, value V) error
	// Add inserts a new key; returns ErrRBTreeSameRBNode if the key already exists.
	Add(key []byte, value V) error
	// Get returns the value for key, or ErrKeyNotFound.
	Get(key []byte) (V, error)
	// Delete removes key and returns the previous value if present.
	Delete(key []byte) (V, bool)
	// Iterate walks keys in ascending order; stop when callback returns false.
	Iterate(cb func(key []byte, value V) bool)
}

// newMemTree returns the default MemTree implementation (RBTree).
func newMemTree[V any]() MemTree[V] {
	return newRBTree[V]()
}

var _ MemTree[struct{}] = (*RBTree[struct{}])(nil)
