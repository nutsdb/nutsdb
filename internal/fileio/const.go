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

import "errors"

const (
	HeaderSize       = 4096
	FooterSize       = 4096
	RecordHeaderSize = 12

	SegmentMagic  uint32 = 0x4E444247 // NDBG
	SealMagic     uint32 = 0x5345414C // SEAL
	FormatVersion        = uint16(1)

	DefaultSegmentSize     = uint64(256 << 20) // 256MiB
	DefaultWriteBufferSize = 1 << 20           // 1MiB
	DefaultMaxRecordSize   = uint64(4 << 20)   // 4MiB
	DefaultMaxOpenSegments = 256

	// FileIDWidth is the zero-padded decimal width used in on-disk filenames
	// (e.g. 0000000001.seg). uint32 max fits in 10 digits.
	FileIDWidth = 10
)

var (
	ErrRecordTooLarge  = errors.New("fileio: record too large")
	ErrInvalidLocation = errors.New("fileio: invalid location")
	ErrCorrupt         = errors.New("fileio: corrupt data")
	ErrSegmentNotFound = errors.New("fileio: segment not found")
	ErrStaleLocation   = errors.New("fileio: stale location")
	ErrStoreClosed     = errors.New("fileio: store closed")
	ErrInvalidOptions  = errors.New("fileio: invalid options")
	ErrSegmentFull     = errors.New("fileio: segment full")
)
