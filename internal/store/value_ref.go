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
	"encoding/binary"

	"github.com/nutsdb/nutsdb/internal/fileio"
)

// ValueKind classifies how a value is stored in the LSM index.
type ValueKind uint8

const (
	ValueKindLocation  ValueKind = 1
	ValueKindInline    ValueKind = 2
	ValueKindTombstone ValueKind = 3
)

// ValueRef is the LSM-side reference to a value (or tombstone).
type ValueRef struct {
	Kind      ValueKind
	Loc       fileio.Location
	Inline    []byte
	Timestamp uint64
	TTL       uint32
}

func encodeValueRef(dst []byte, ref ValueRef) []byte {
	n := 1 + 8 + 4
	switch ref.Kind {
	case ValueKindLocation:
		n += 16
	case ValueKindInline:
		n += 4 + len(ref.Inline)
	case ValueKindTombstone:
	default:
		// encode as tombstone if unknown
		ref.Kind = ValueKindTombstone
	}
	if cap(dst) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
	}
	dst[0] = byte(ref.Kind)
	binary.LittleEndian.PutUint64(dst[1:9], ref.Timestamp)
	binary.LittleEndian.PutUint32(dst[9:13], ref.TTL)
	off := 13
	switch ref.Kind {
	case ValueKindLocation:
		binary.LittleEndian.PutUint32(dst[off:off+4], ref.Loc.FileID)
		binary.LittleEndian.PutUint64(dst[off+4:off+12], ref.Loc.Offset)
		binary.LittleEndian.PutUint32(dst[off+12:off+16], ref.Loc.Length)
	case ValueKindInline:
		binary.LittleEndian.PutUint32(dst[off:off+4], uint32(len(ref.Inline)))
		copy(dst[off+4:], ref.Inline)
	}
	return dst
}

func decodeValueRef(buf []byte) (ValueRef, int, error) {
	if len(buf) < 13 {
		return ValueRef{}, 0, ErrCorruptEntry
	}
	ref := ValueRef{
		Kind:      ValueKind(buf[0]),
		Timestamp: binary.LittleEndian.Uint64(buf[1:9]),
		TTL:       binary.LittleEndian.Uint32(buf[9:13]),
	}
	off := 13
	switch ref.Kind {
	case ValueKindLocation:
		if len(buf) < off+16 {
			return ValueRef{}, 0, ErrCorruptEntry
		}
		ref.Loc = fileio.Location{
			FileID: binary.LittleEndian.Uint32(buf[off : off+4]),
			Offset: binary.LittleEndian.Uint64(buf[off+4 : off+12]),
			Length: binary.LittleEndian.Uint32(buf[off+12 : off+16]),
		}
		return ref, off + 16, nil
	case ValueKindInline:
		if len(buf) < off+4 {
			return ValueRef{}, 0, ErrCorruptEntry
		}
		n := int(binary.LittleEndian.Uint32(buf[off : off+4]))
		off += 4
		if n < 0 || len(buf) < off+n {
			return ValueRef{}, 0, ErrCorruptEntry
		}
		ref.Inline = append([]byte(nil), buf[off:off+n]...)
		return ref, off + n, nil
	case ValueKindTombstone:
		return ref, off, nil
	default:
		return ValueRef{}, 0, ErrCorruptEntry
	}
}

func valueRefApproxSize(ref ValueRef) int64 {
	n := int64(16) // metadata overhead
	switch ref.Kind {
	case ValueKindLocation:
		n += 16
	case ValueKindInline:
		n += int64(len(ref.Inline))
	}
	return n
}
