// Copyright 2019 The nutsdb Authors. All rights reserved.
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

// EntryIdxMode represents entry index mode
type EntryIdxMode int

const (
	// HintKeyValAndRAMIdxMode represents ram index (key and value) mode
	HintKeyValAndRAMIdxMode EntryIdxMode = iota

	// HintKeyAndRAMIdxMode represents represents ram index (only key) mode
	HintKeyAndRAMIdxMode
)

// Options records params for creating DB object.
type Options struct {
	Dir                  string
	EntryIdxMode         EntryIdxMode
	RWMode               RWMode
	SegmentSize          int64
	NodeNum              int64
	SyncEnable           bool
	StartFileLoadingMode RWMode
}

var defaultSegmentSize int64 = 8 * 1024 * 1024

// DefaultOptions represents the default options
var DefaultOptions = Options{
	EntryIdxMode:         HintKeyValAndRAMIdxMode,
	SegmentSize:          defaultSegmentSize,
	NodeNum:              1,
	RWMode:               FileIO,
	SyncEnable:           true,
	StartFileLoadingMode: MMap,
}
