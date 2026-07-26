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

package fileio

// Location is the stable address of a record in the storage I/O layer.
type Location struct {
	FileID uint32
	Offset uint64
	Length uint32
}

// RecordType describes the semantic type of a record payload.
type RecordType uint8

const (
	RecordPut RecordType = iota + 1
	RecordDelete
	RecordMeta
)

// SyncMode controls when data is flushed to durable storage.
type SyncMode int

const (
	SyncNoSync SyncMode = iota
	SyncBatch
	SyncEveryWrite
)

// ReadBackend selects how sealed segments are read.
type ReadBackend int

const (
	ReadBackendFileIO ReadBackend = iota
	ReadBackendMMapSealed
)

func (b ReadBackend) String() string {
	switch b {
	case ReadBackendFileIO:
		return "fileio"
	case ReadBackendMMapSealed:
		return "mmap-sealed"
	default:
		return "unknown"
	}
}

// Options configures a Store.
type Options struct {
	Dir             string
	SegmentSize     uint64
	WriteBufferSize int
	MaxRecordSize   uint64
	SyncMode        SyncMode
	MaxOpenSegments int
	ReadBackend     ReadBackend
}

// DefaultOptions returns options with design-document defaults.
func DefaultOptions(dir string) Options {
	return Options{
		Dir:             dir,
		SegmentSize:     DefaultSegmentSize,
		WriteBufferSize: DefaultWriteBufferSize,
		MaxRecordSize:   DefaultMaxRecordSize,
		SyncMode:        SyncBatch,
		MaxOpenSegments: DefaultMaxOpenSegments,
		ReadBackend:     ReadBackendFileIO,
	}
}

func (o Options) withDefaults() (Options, error) {
	if o.Dir == "" {
		return o, ErrInvalidOptions
	}
	if o.SegmentSize == 0 {
		o.SegmentSize = DefaultSegmentSize
	}
	if o.WriteBufferSize <= 0 {
		o.WriteBufferSize = DefaultWriteBufferSize
	}
	if o.MaxOpenSegments <= 0 {
		o.MaxOpenSegments = DefaultMaxOpenSegments
	}
	usable := o.usable()
	if usable <= RecordHeaderSize {
		return o, ErrInvalidOptions
	}
	if o.MaxRecordSize == 0 {
		o.MaxRecordSize = DefaultMaxRecordSize
	}
	if o.MaxRecordSize > usable {
		o.MaxRecordSize = usable
	}
	// Location.Length is uint32; full record size must fit.
	const maxLen = ^uint32(0)
	if o.MaxRecordSize > uint64(maxLen) {
		o.MaxRecordSize = uint64(maxLen)
	}
	return o, nil
}

func (o Options) usable() uint64 {
	if o.SegmentSize <= HeaderSize+FooterSize {
		return 0
	}
	return o.SegmentSize - HeaderSize - FooterSize
}
