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

package core

const Persistent uint32 = 0

// Record means item of indexes in memory
type Record struct {
	Key       []byte
	Value     []byte
	Timestamp uint64
	TTL       uint32
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

func (r *Record) WithTTL(ttl uint32) *Record {
	r.TTL = ttl
	return r
}
