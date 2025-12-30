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

import (
	"time"

	"github.com/nutsdb/nutsdb/internal/data"
	"github.com/nutsdb/nutsdb/internal/fileio"
	"github.com/nutsdb/nutsdb/internal/ttl"
)

// EntryIdxMode represents entry index mode.
type EntryIdxMode int

const (
	// HintKeyValAndRAMIdxMode represents ram index (key and value) mode.
	HintKeyValAndRAMIdxMode EntryIdxMode = iota

	// HintKeyAndRAMIdxMode represents ram index (only key) mode.
	HintKeyAndRAMIdxMode
)

// TTLConfig is an alias for ttl.Config for convenience.
type TTLConfig = ttl.Config

// ListImplementationType defines the implementation type for List data structure.
type ListImplementationType data.ListImplementationType

func (impl ListImplementationType) toInternal() data.ListImplementationType {
	return data.ListImplementationType(impl)
}

const (
	// ListImplDoublyLinkedList uses doubly linked list implementation (default).
	// Advantages: O(1) head/tail operations, lower memory overhead
	// Best for: High-frequency LPush/RPush/LPop/RPop operations
	ListImplDoublyLinkedList = iota

	// ListImplBTree uses BTree implementation.
	// Advantages: O(log n + k) range queries, efficient random access
	// Best for: Frequent range queries or indexed access patterns
	ListImplBTree
)

// An ErrorHandler handles an error occurred during transaction.
type ErrorHandler interface {
	HandleError(err error)
}

// The ErrorHandlerFunc type is an adapter to ErrorHandler.
type ErrorHandlerFunc func(err error)

func (fn ErrorHandlerFunc) HandleError(err error) {
	fn(err)
}

type LessFunc func(l, r string) bool

// Options records params for creating DB object.
type Options struct {
	// Dir represents Open the database located in which dir.
	Dir string

	// EntryIdxMode represents using which mode to index the entries.
	EntryIdxMode EntryIdxMode

	// RWMode represents the read and write mode.
	// RWMode includes two options: FileIO and MMap.
	// FileIO represents the read and write mode using standard I/O.
	// MMap represents the read and write mode using mmap.
	RWMode      RWMode
	SegmentSize int64

	// NodeNum represents the node number.
	// Default NodeNum is 1. NodeNum range [1,1023].
	NodeNum int64

	// SyncEnable represents if call Sync() function.
	// if SyncEnable is false, high write performance but potential data loss likely.
	// if SyncEnable is true, slower but persistent.
	SyncEnable bool

	// MaxFdNumsInCache represents the max numbers of fd in cache.
	MaxFdNumsInCache int

	// CleanFdsCacheThreshold represents the maximum threshold for recycling fd, it should be between 0 and 1.
	CleanFdsCacheThreshold float64

	// BufferSizeOfRecovery represents the buffer size of recoveryReader buffer Size
	BufferSizeOfRecovery int

	// GcWhenClose represent initiative GC when calling db.Close()
	GCWhenClose bool

	// CommitBufferSize represent allocated memory for tx
	CommitBufferSize int64

	// ErrorHandler handles an error occurred during transaction.
	// Example:
	//     func triggerAlertError(err error) {
	//     	   if errors.Is(err, targetErr) {
	//         		alertManager.TriggerAlert()
	//     	   }
	//     })
	ErrorHandler ErrorHandler

	// LessFunc is a function that sorts keys.
	LessFunc LessFunc

	// MergeInterval represent the interval for automatic merges, with 0 meaning automatic merging is disabled.
	MergeInterval time.Duration

	// MaxBatchCount represents max entries in batch
	MaxBatchCount int64

	// MaxBatchSize represents max batch size in bytes
	MaxBatchSize int64

	// max write record num
	MaxWriteRecordCount int64

	// cache size for HintKeyAndRAMIdxMode
	HintKeyAndRAMIdxCacheSize int

	// TTLConfig contains configuration for TTL expiration handling.
	// If not set, DefaultTTLConfig() will be used.
	TTLConfig TTLConfig

	// EnableHintFile represents if enable hint file feature.
	// If EnableHintFile is true, hint files will be created and used for faster database startup.
	// If EnableHintFile is false, hint files will not be created or used.
	EnableHintFile bool

	// EnableMergeV2 toggles the redesigned merge pipeline with deterministic merge files and manifest support.
	// When disabled, NutsDB falls back to the legacy merge logic.
	EnableMergeV2 bool

	// ListImpl specifies the implementation type for List data structure.
	// Default: ListImplDoublyLinkedList (maintains backward compatibility)
	ListImpl ListImplementationType

	// EnableWatch toggles the watch feature.
	// If EnableWatch is true, the watch feature will be enabled. The watch feature will be disabled by default.
	EnableWatch bool
}

const (
	B  = fileio.B
	KB = fileio.KB
	MB = fileio.MB
	GB = fileio.GB
)

// defaultSegmentSize is default data file size.
var defaultSegmentSize int64 = 256 * MB

// DefaultOptions represents the default options.
var DefaultOptions = func() Options {
	return Options{
		EntryIdxMode:              HintKeyValAndRAMIdxMode,
		SegmentSize:               defaultSegmentSize,
		NodeNum:                   1,
		RWMode:                    FileIO,
		SyncEnable:                true,
		CommitBufferSize:          4 * MB,
		MergeInterval:             2 * time.Hour,
		MaxBatchSize:              (15 * defaultSegmentSize / 4) / 100,
		MaxBatchCount:             (15 * defaultSegmentSize / 4) / 100 / 100,
		HintKeyAndRAMIdxCacheSize: 0,
		TTLConfig:                 ttl.DefaultConfig(),
		EnableHintFile:            false,
		EnableMergeV2:             false,
		ListImpl:                  ListImplementationType(ListImplBTree),
		EnableWatch:               false,
	}
}()

type Option func(*Options)

func WithDir(dir string) Option {
	return func(opt *Options) {
		opt.Dir = dir
	}
}

func WithEntryIdxMode(entryIdxMode EntryIdxMode) Option {
	return func(opt *Options) {
		opt.EntryIdxMode = entryIdxMode
	}
}

func WithRWMode(rwMode RWMode) Option {
	return func(opt *Options) {
		opt.RWMode = rwMode
	}
}

func WithSegmentSize(size int64) Option {
	return func(opt *Options) {
		opt.SegmentSize = size
	}
}

func WithMaxBatchCount(count int64) Option {
	return func(opt *Options) {
		opt.MaxBatchCount = count
	}
}

func WithHintKeyAndRAMIdxCacheSize(size int) Option {
	return func(opt *Options) {
		opt.HintKeyAndRAMIdxCacheSize = size
	}
}

func WithMaxBatchSize(size int64) Option {
	return func(opt *Options) {
		opt.MaxBatchSize = size
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

func WithBufferSizeOfRecovery(size int) Option {
	return func(opt *Options) {
		opt.BufferSizeOfRecovery = size
	}
}

func WithGCWhenClose(enable bool) Option {
	return func(opt *Options) {
		opt.GCWhenClose = enable
	}
}

func WithErrorHandler(errorHandler ErrorHandler) Option {
	return func(opt *Options) {
		opt.ErrorHandler = errorHandler
	}
}

func WithCommitBufferSize(commitBufferSize int64) Option {
	return func(opt *Options) {
		opt.CommitBufferSize = commitBufferSize
	}
}

func WithLessFunc(lessFunc LessFunc) Option {
	return func(opt *Options) {
		opt.LessFunc = lessFunc
	}
}

func WithMaxWriteRecordCount(maxWriteRecordCount int64) Option {
	return func(opt *Options) {
		opt.MaxWriteRecordCount = maxWriteRecordCount
	}
}

func WithEnableHintFile(enable bool) Option {
	return func(opt *Options) {
		opt.EnableHintFile = enable
	}
}

func WithEnableMergeV2(enable bool) Option {
	return func(opt *Options) {
		opt.EnableMergeV2 = enable
	}
}

func WithListImpl(implType ListImplementationType) Option {
	return func(opt *Options) {
		opt.ListImpl = implType
	}
}

// WithTTLConfig sets the TTL configuration.
func WithTTLConfig(config TTLConfig) Option {
	return func(opt *Options) {
		opt.TTLConfig = config
	}
}

// DefaultTTLConfig returns the default TTL configuration.
// This is a convenience function that wraps ttl.DefaultConfig().
func DefaultTTLConfig() TTLConfig {
	return ttl.DefaultConfig()
}

func WithEnableWatch(enable bool) Option {
	return func(opt *Options) {
		opt.EnableWatch = enable
	}
}
