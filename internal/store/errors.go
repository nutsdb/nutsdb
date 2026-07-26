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

import "errors"

var (
	// ErrMemStoreClosed is returned when operating on a closed mem store.
	ErrMemStoreClosed = errors.New("mem store closed")

	// ErrKeyNotFound is returned when a key is not found or has expired.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyEmpty is returned when the key is empty.
	ErrKeyEmpty = errors.New("key empty")

	// ErrRBTreeSameRBNode is returned when a duplicate key is added to the RBTree.
	ErrRBTreeSameRBNode = errors.New("RBTree cannot add duplicate key")

	// ErrDiskStoreClosed is returned when operating on a closed disk store.
	ErrDiskStoreClosed = errors.New("disk store closed")

	// ErrInvalidRecord is returned when a Put record is nil or invalid.
	ErrInvalidRecord = errors.New("invalid record")

	// ErrKeyMismatch is returned when Record.Key does not match the API key.
	ErrKeyMismatch = errors.New("record key mismatch")

	// ErrCorruptEntry is returned when an on-disk entry payload cannot be decoded.
	ErrCorruptEntry = errors.New("corrupt entry")
)
