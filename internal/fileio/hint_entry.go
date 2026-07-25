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

import (
	"encoding/binary"
	"hash/crc32"
)

// HintEntry is one key → Location mapping persisted in a .hint file.
type HintEntry struct {
	Key  []byte
	Loc  Location
	Type RecordType
}

// DecodeKeyFunc extracts a business key from a Store record payload.
// Return ErrHintSkipMeta to omit non-KV RecordMeta records from the hint.
type DecodeKeyFunc func(payload []byte, typ RecordType) (key []byte, err error)

func encodeHintEntry(dst []byte, e HintEntry) ([]byte, error) {
	if len(e.Key) == 0 || len(e.Key) > MaxHintKeySize {
		return nil, ErrHintInvalidEntry
	}
	if e.Loc.Length == 0 || e.Loc.Length < RecordHeaderSize {
		return nil, ErrHintInvalidEntry
	}
	n := HintEntryHeaderSize + len(e.Key)
	if cap(dst) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
	}

	binary.LittleEndian.PutUint32(dst[4:8], uint32(len(e.Key)))
	binary.LittleEndian.PutUint32(dst[8:12], e.Loc.FileID)
	binary.LittleEndian.PutUint64(dst[12:20], e.Loc.Offset)
	binary.LittleEndian.PutUint32(dst[20:24], e.Loc.Length)
	dst[24] = byte(e.Type)
	dst[25], dst[26], dst[27] = 0, 0, 0
	copy(dst[HintEntryHeaderSize:], e.Key)

	crc := crc32.Checksum(dst[4:], crc32cTable)
	binary.LittleEndian.PutUint32(dst[0:4], crc)
	return dst, nil
}

func decodeHintEntry(buf []byte, expectFileID uint32) (HintEntry, int, error) {
	if len(buf) < HintEntryHeaderSize {
		return HintEntry{}, 0, ErrHintCorrupt
	}
	gotCRC := binary.LittleEndian.Uint32(buf[0:4])
	keyLen := binary.LittleEndian.Uint32(buf[4:8])
	if keyLen == 0 || int(keyLen) > MaxHintKeySize {
		return HintEntry{}, 0, ErrHintInvalidEntry
	}
	total := HintEntryHeaderSize + int(keyLen)
	if len(buf) < total {
		return HintEntry{}, 0, ErrHintCorrupt
	}
	wantCRC := crc32.Checksum(buf[4:total], crc32cTable)
	if gotCRC != wantCRC {
		return HintEntry{}, 0, ErrHintCorrupt
	}

	fileID := binary.LittleEndian.Uint32(buf[8:12])
	if fileID != expectFileID {
		return HintEntry{}, 0, ErrHintInvalidEntry
	}
	offset := binary.LittleEndian.Uint64(buf[12:20])
	length := binary.LittleEndian.Uint32(buf[20:24])
	if length == 0 || length < RecordHeaderSize {
		return HintEntry{}, 0, ErrHintInvalidEntry
	}
	typ := RecordType(buf[24])
	key := make([]byte, keyLen)
	copy(key, buf[HintEntryHeaderSize:total])

	return HintEntry{
		Key:  key,
		Loc:  Location{FileID: fileID, Offset: offset, Length: length},
		Type: typ,
	}, total, nil
}
