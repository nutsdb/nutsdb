// Copyright 2019 The nutsdb Author. All rights reserved.
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

import "github.com/xujiajun/nutsdb/consts"

// Options records params for creating DB object.
type Options struct {
	// Dir represents Open the database located in which dir.
	Dir string

	// EntryIdxMode represents using which mode to index the entries.
	EntryIdxMode consts.EntryIdxMode

	// RWMode represents the read and write mode.
	// RWMode includes two options: FileIO and MMap.
	// FileIO represents the read and write mode using standard I/O.
	// MMap represents the read and write mode using mmap.
	RWMode      consts.RWMode
	SegmentSize int64

	// NodeNum represents the node number.
	// Default NodeNum is 1. NodeNum range [1,1023].
	NodeNum int64

	// SyncEnable represents if call Sync() function.
	// if SyncEnable is false, high write performance but potential data loss likely.
	// if SyncEnable is true, slower but persistent.
	SyncEnable bool

	// StartFileLoadingMode represents when open a database which RWMode to load files.
	StartFileLoadingMode consts.RWMode

	// MaxFdNumsInCache represents the max numbers of fd in cache.
	MaxFdNumsInCache int

	// CleanFdsCacheThreshold represents the maximum threshold for recycling fd, it should be between 0 and 1.
	CleanFdsCacheThreshold float64
}

// DefaultOptions represents the default options.
var DefaultOptions = func() Options {
	return Options{
		EntryIdxMode:         consts.HintKeyValAndRAMIdxMode,
		SegmentSize:          consts.DefaultSegmentSize,
		NodeNum:              1,
		RWMode:               consts.FileIO,
		SyncEnable:           true,
		StartFileLoadingMode: consts.MMap,
	}
}()

type Option func(*Options)

func WithDir(dir string) Option {
	return func(opt *Options) {
		opt.Dir = dir
	}
}

func WithEntryIdxMode(entryIdxMode consts.EntryIdxMode) Option {
	return func(opt *Options) {
		opt.EntryIdxMode = entryIdxMode
	}
}

func WithRWMode(rwMode consts.RWMode) Option {
	return func(opt *Options) {
		opt.RWMode = rwMode
	}
}

func WithSegmentSize(size int64) Option {
	return func(opt *Options) {
		opt.SegmentSize = size
	}
}

func WithNodeNum(num int64) Option {
	return func(opt *Options) {
		opt.NodeNum = num
	}
}

func WithSyncEnable(enable bool) Option {
	return func(opt *Options) {
		opt.SyncEnable = enable
	}
}

func WithStartFileLoadingMode(rwMode consts.RWMode) Option {
	return func(opt *Options) {
		opt.StartFileLoadingMode = rwMode
	}
}

func WithMaxFdNumsInCache(num int) Option {
	return func(opt *Options) {
		opt.MaxFdNumsInCache = num
	}
}

func WithCleanFdsCacheThreshold(threshold float64) Option {
	return func(opt *Options) {
		opt.CleanFdsCacheThreshold = threshold
	}
}
