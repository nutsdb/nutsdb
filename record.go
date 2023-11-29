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
)

// Record means item of indexes in memory
type Record struct {
	Key       []byte
	Value     []byte
	FileID    int64
	DataPos   uint64
	ValueSize uint32
	Timestamp uint64
	TTL       uint32
	TxID      uint64
}

// IsExpired returns the record if expired or not.
func (r *Record) IsExpired() bool {
	return IsExpired(r.TTL, r.Timestamp)
}

// IsExpired checks the ttl if expired or not.
func IsExpired(ttl uint32, timestamp uint64) bool {
	if ttl == Persistent {
		return false
	}

	now := time.UnixMilli(time.Now().UnixMilli())
	expireTime := time.UnixMilli(int64(timestamp))
	expireTime = expireTime.Add(time.Duration(ttl) * time.Second)

	return expireTime.Before(now)
}

// NewRecord generate a record Obj
func NewRecord() *Record {
	return new(Record)
}

func (r *Record) WithKey(k []byte) *Record {
	r.Key = k
	return r
}

// WithValue set the Value to Record
func (r *Record) WithValue(v []byte) *Record {
	r.Value = v
	return r
}

// WithFileId set FileID to Record
func (r *Record) WithFileId(fid int64) *Record {
	r.FileID = fid
	return r
}

// WithDataPos set DataPos to Record
func (r *Record) WithDataPos(pos uint64) *Record {
	r.DataPos = pos
	return r
}

func (r *Record) WithValueSize(valueSize uint32) *Record {
	r.ValueSize = valueSize
	return r
}

func (r *Record) WithTimestamp(timestamp uint64) *Record {
	r.Timestamp = timestamp
	return r
}

func (r *Record) WithTTL(ttl uint32) *Record {
	r.TTL = ttl
	return r
}

func (r *Record) WithTxID(txID uint64) *Record {
	r.TxID = txID
	return r
}
