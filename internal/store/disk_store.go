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

	"github.com/nutsdb/nutsdb/internal/fileio"
)

// DiskStoreOptions configures OpenDiskStore / OpenStoreManager.
type DiskStoreOptions struct {
	Dir string

	SegmentSize     uint64
	WriteBufferSize int
	MaxRecordSize   uint64
	SyncMode        fileio.SyncMode
	MaxOpenSegments int
	ReadBackend     fileio.ReadBackend

	EnableReadTTL                  bool
	DeleteWritesTombstoneIfMissing bool
	AutoFillTimestamp              bool
}

// DefaultDiskStoreOptions returns defaults aligned with DISKSTORE_DESIGN.md.
func DefaultDiskStoreOptions(dir string) DiskStoreOptions {
	fio := fileio.DefaultOptions(dir)
	return DiskStoreOptions{
		Dir:                            dir,
		SegmentSize:                    fio.SegmentSize,
		WriteBufferSize:                fio.WriteBufferSize,
		MaxRecordSize:                  fio.MaxRecordSize,
		SyncMode:                       fio.SyncMode,
		MaxOpenSegments:                fio.MaxOpenSegments,
		ReadBackend:                    fio.ReadBackend,
		EnableReadTTL:                  true,
		DeleteWritesTombstoneIfMissing: false,
		AutoFillTimestamp:              true,
	}
}

// OpenDiskStore opens a durable StoreManager.
// It is an alias of OpenStoreManager for compatibility with DISKSTORE_DESIGN.md.
func OpenDiskStore(opts DiskStoreOptions) (StoreManager, error) {
	return OpenStoreManager(opts)
}

func checkCtx(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
