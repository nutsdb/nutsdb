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
	"github.com/nutsdb/nutsdb/internal/fileio"
)

// LSMOptions configures the LSM + ValueLog StoreManager.
// See docs/design/store/LSM_VALUELOG_DESIGN.md.
type LSMOptions struct {
	Dir string

	// ValueLog configures the fileio.Store used as ValueLog (typically Dir/vlog).
	ValueLog fileio.Options

	MemTableSize               int64
	ValueInlineThreshold       int
	L0FileNumCompactionTrigger int
	LevelSizeMultiplier        int
	LevelBaseSize              int64
	WALSyncMode                fileio.SyncMode
}

// DefaultLSMOptions returns defaults aligned with docs/design/store/LSM_VALUELOG_DESIGN.md.
func DefaultLSMOptions(dir string) LSMOptions {
	fio := fileio.DefaultOptions(defaultVLogDir(dir))
	return LSMOptions{
		Dir:                        dir,
		ValueLog:                   fio,
		MemTableSize:               64 << 20,
		ValueInlineThreshold:       256,
		L0FileNumCompactionTrigger: 4,
		LevelSizeMultiplier:        10,
		LevelBaseSize:              10 << 20,
		WALSyncMode:                fileio.SyncBatch,
	}
}

// OpenStoreManager opens the LSM + ValueLog StoreManager.
func OpenStoreManager(opts LSMOptions) (StoreManager, error) {
	return openLSMStoreManager(opts)
}
