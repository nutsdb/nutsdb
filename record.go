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

// Record records entry and hint.
type Record struct {
	H      *Hint
	V      []byte
	Bucket string
}

// IsExpired returns the record if expired or not.
func (r *Record) IsExpired() bool {
	return IsExpired(r.H.Meta.TTL, r.H.Meta.Timestamp)
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

// UpdateRecord updates the record.
func (r *Record) UpdateRecord(h *Hint, v []byte) error {
	r.V = v
	r.H = h

	return nil
}

// NewRecord generate a record Obj
func NewRecord() *Record {
	return new(Record)
}

// WithHint set the Hint to Record
func (r *Record) WithHint(hint *Hint) *Record {
	r.H = hint
	return r
}

// WithValue set the Value to Record
func (r *Record) WithValue(v []byte) *Record {
	r.V = v
	return r
}

// WithBucket set the Bucket to Record
func (r *Record) WithBucket(bucket string) *Record {
	r.Bucket = bucket
	return r
}
