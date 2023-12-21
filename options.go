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

import "time"

// EntryIdxMode represents entry index mode.
type EntryIdxMode int

const (
	// HintKeyValAndRAMIdxMode represents ram index (key and value) mode.
	HintKeyValAndRAMIdxMode EntryIdxMode = iota

	// HintKeyAndRAMIdxMode represents ram index (only key) mode.
	HintKeyAndRAMIdxMode
)

type ExpiredDeleteType uint8

const (
	// TimeWheel represents use time wheel to do expired deletion
	TimeWheel ExpiredDeleteType = iota

	// TimeHeap represents use time heap to do expired deletion
	TimeHeap
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

	// CcWhenClose represent initiative GC when calling db.Close()
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

	// ExpiredDeleteType represents the data structure used for expired deletion
	// TimeWheel means use the time wheel, You can use it when you need high performance or low memory usage
	// TimeHeap means use the time heap, You can use it when you need to delete precisely or memory usage will be high
	ExpiredDeleteType ExpiredDeleteType

	// max write record num
	MaxWriteRecordCount int64

	// cache size for HintKeyAndRAMIdxMode
	HintKeyAndRAMIdxCacheSize int
}

const (
	B = 1

	KB = 1024 * B

	MB = 1024 * KB

	GB = 1024 * MB
)

// defaultSegmentSize is default data file size.
var defaultSegmentSize int64 = 256 * MB

// DefaultOptions represents the default options.
var DefaultOptions = func() Options {
	return Options{
		EntryIdxMode:      HintKeyValAndRAMIdxMode,
		SegmentSize:       defaultSegmentSize,
		NodeNum:           1,
		RWMode:            FileIO,
		SyncEnable:        true,
		CommitBufferSize:  4 * MB,
		MergeInterval:     2 * time.Hour,
		MaxBatchSize:      (15 * defaultSegmentSize / 4) / 100,
		MaxBatchCount:     (15 * defaultSegmentSize / 4) / 100 / 100,
		HintKeyAndRAMIdxCacheSize: 0,
		ExpiredDeleteType: TimeWheel,
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
