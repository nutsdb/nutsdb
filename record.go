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

import "time"

// Record records entry and hint.
type Record struct {
	H *Hint
	E *Entry
}

// IsExpired returns the record if expired or not.
func (r *Record) IsExpired() bool {
	return IsExpired(r.H.Meta.TTL, r.H.Meta.Timestamp)
}

// IsExpired checks the ttl if expired or not.
func IsExpired(ttl uint32, timestamp uint64) bool {
	now := time.Now().Unix()
	if ttl > 0 && uint64(ttl)+timestamp > uint64(now) || ttl == Persistent {
		return false
	}

	return true
}

// UpdateRecord updates the record.
func (r *Record) UpdateRecord(h *Hint, e *Entry) error {
	r.E = e
	r.H = h

	return nil
}
