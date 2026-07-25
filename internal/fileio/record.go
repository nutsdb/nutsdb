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

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// encodeRecord encodes payload into a record buffer.
// Layout: crc32c(4) | payload_len(4) | type(1) | reserved(3) | payload
func encodeRecord(dst []byte, payload []byte, typ RecordType) []byte {
	n := RecordHeaderSize + len(payload)
	if cap(dst) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
	}

	binary.LittleEndian.PutUint32(dst[4:8], uint32(len(payload)))
	dst[8] = byte(typ)
	dst[9], dst[10], dst[11] = 0, 0, 0
	copy(dst[RecordHeaderSize:], payload)

	crc := crc32.Checksum(dst[4:], crc32cTable)
	binary.LittleEndian.PutUint32(dst[0:4], crc)
	return dst
}

func decodeRecord(buf []byte) (payload []byte, typ RecordType, err error) {
	if len(buf) < RecordHeaderSize {
		return nil, 0, ErrCorrupt
	}
	gotCRC := binary.LittleEndian.Uint32(buf[0:4])
	wantCRC := crc32.Checksum(buf[4:], crc32cTable)
	if gotCRC != wantCRC {
		return nil, 0, ErrCorrupt
	}

	payloadLen := binary.LittleEndian.Uint32(buf[4:8])
	if uint64(RecordHeaderSize)+uint64(payloadLen) != uint64(len(buf)) {
		return nil, 0, ErrCorrupt
	}
	typ = RecordType(buf[8])
	if payloadLen == 0 {
		return []byte{}, typ, nil
	}
	payload = make([]byte, payloadLen)
	copy(payload, buf[RecordHeaderSize:])
	return payload, typ, nil
}

func decodeRecordInto(buf []byte, out []byte) (payload []byte, typ RecordType, err error) {
	if len(buf) < RecordHeaderSize {
		return nil, 0, ErrCorrupt
	}
	gotCRC := binary.LittleEndian.Uint32(buf[0:4])
	wantCRC := crc32.Checksum(buf[4:], crc32cTable)
	if gotCRC != wantCRC {
		return nil, 0, ErrCorrupt
	}

	payloadLen := binary.LittleEndian.Uint32(buf[4:8])
	if uint64(RecordHeaderSize)+uint64(payloadLen) != uint64(len(buf)) {
		return nil, 0, ErrCorrupt
	}
	typ = RecordType(buf[8])
	if payloadLen == 0 {
		return []byte{}, typ, nil
	}
	src := buf[RecordHeaderSize:]
	if cap(out) >= int(payloadLen) {
		payload = out[:payloadLen]
	} else {
		payload = make([]byte, payloadLen)
	}
	copy(payload, src)
	return payload, typ, nil
}

func recordLength(payloadLen int) uint32 {
	return uint32(RecordHeaderSize) + uint32(payloadLen)
}
