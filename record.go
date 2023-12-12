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

// isExpired returns the record if expired or not.
func (r *Record) isExpired() bool {
	return isExpired(r.TTL, r.Timestamp)
}

// isExpired checks the ttl if expired or not.
func isExpired(ttl uint32, timestamp uint64) bool {
	if ttl == Persistent {
		return false
	}

	now := time.UnixMilli(time.Now().UnixMilli())
	expireTime := time.UnixMilli(int64(timestamp))
	expireTime = expireTime.Add(time.Duration(ttl) * time.Second)

	return expireTime.Before(now)
}

// newRecord generate a record Obj
func newRecord() *Record {
	return new(Record)
}

func (r *Record) withKey(k []byte) *Record {
	r.Key = k
	return r
}

// withValue set the Value to Record
func (r *Record) withValue(v []byte) *Record {
	r.Value = v
	return r
}

// withFileId set FileID to Record
func (r *Record) withFileId(fid int64) *Record {
	r.FileID = fid
	return r
}

// withDataPos set DataPos to Record
func (r *Record) withDataPos(pos uint64) *Record {
	r.DataPos = pos
	return r
}

func (r *Record) withValueSize(valueSize uint32) *Record {
	r.ValueSize = valueSize
	return r
}

func (r *Record) withTimestamp(timestamp uint64) *Record {
	r.Timestamp = timestamp
	return r
}

func (r *Record) withTTL(ttl uint32) *Record {
	r.TTL = ttl
	return r
}

func (r *Record) withTxID(txID uint64) *Record {
	r.TxID = txID
	return r
}
