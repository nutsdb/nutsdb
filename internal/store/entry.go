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
	"bytes"
	"encoding/binary"

	"github.com/nutsdb/nutsdb/internal/core"
)

const (
	putPayloadHeaderSize = 20 // key_len + value_len + timestamp + ttl
)

// encodePutPayload encodes a Put record for ValueLog (fileio.RecordPut payload).
func encodePutPayload(key []byte, rec *core.Record) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyEmpty
	}
	if rec == nil {
		return nil, ErrInvalidRecord
	}
	if len(rec.Key) > 0 && !bytes.Equal(rec.Key, key) {
		return nil, ErrKeyMismatch
	}
	n := putPayloadHeaderSize + len(key) + len(rec.Value)
	buf := make([]byte, n)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(rec.Value)))
	binary.LittleEndian.PutUint64(buf[8:16], rec.Timestamp)
	binary.LittleEndian.PutUint32(buf[16:20], rec.TTL)
	copy(buf[putPayloadHeaderSize:], key)
	copy(buf[putPayloadHeaderSize+len(key):], rec.Value)
	return buf, nil
}

func decodePutPayload(payload []byte) (key []byte, rec *core.Record, err error) {
	if len(payload) < putPayloadHeaderSize {
		return nil, nil, ErrCorruptEntry
	}
	keyLen := binary.LittleEndian.Uint32(payload[0:4])
	valueLen := binary.LittleEndian.Uint32(payload[4:8])
	if keyLen == 0 {
		return nil, nil, ErrCorruptEntry
	}
	need := putPayloadHeaderSize + int(keyLen) + int(valueLen)
	if len(payload) != need {
		return nil, nil, ErrCorruptEntry
	}
	key = make([]byte, keyLen)
	copy(key, payload[putPayloadHeaderSize:putPayloadHeaderSize+int(keyLen)])
	value := make([]byte, valueLen)
	copy(value, payload[putPayloadHeaderSize+int(keyLen):])
	rec = &core.Record{
		Key:       append([]byte(nil), key...),
		Value:     value,
		Timestamp: binary.LittleEndian.Uint64(payload[8:16]),
		TTL:       binary.LittleEndian.Uint32(payload[16:20]),
	}
	return key, rec, nil
}

func isExpired(rec *core.Record, nowUnix uint64) bool {
	if rec == nil || rec.TTL == core.Persistent {
		return false
	}
	return nowUnix > rec.Timestamp+uint64(rec.TTL)
}
