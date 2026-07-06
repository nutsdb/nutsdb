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
	"context"

	"github.com/nutsdb/nutsdb/internal/core"
)

// StoreManager is the interface for all operation for
// memory storage and disk storage.
type StoreManager interface {
	// Get returns the record for the given key.
	Get(ctx context.Context, key []byte) (*core.Record, error)
	// Put puts the record for the given key.
	Put(ctx context.Context, key []byte, value *core.Record) error
	// Delete deletes the record for the given key.
	Delete(ctx context.Context, key []byte) error
	// Iterate iterates over the records in the store.
	Iterate(ctx context.Context, callback func(key []byte, value *core.Record) bool) error
	// Close closes the store.
	Close() error

	BatchAPI
}

type BatchAPI interface {
	// BatchPut puts the records for the given keys.
	BatchPut(ctx context.Context, records []struct {
		Key   []byte
		Value *core.Record
	}) error
	// BatchDelete deletes the records for the given keys.
	BatchDelete(ctx context.Context, keys [][]byte) error
	// BatchGet gets the records for the given keys.
	BatchGet(ctx context.Context, keys [][]byte) ([]struct {
		Key   []byte
		Value *core.Record
	}, error)
}
