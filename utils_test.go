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
	"testing"

	"github.com/xujiajun/utils/strconv2"
)

func TestPrintSortedMap(t *testing.T) {
	entries := make(map[string]*Entry, 10)
	for i := 0; i < 10; i++ {
		k := strconv2.IntToStr(i)
		entries[k] = &Entry{Key: []byte(k)}
	}

	keys, _ := SortedEntryKeys(entries)

	for i := 0; i < 10; i++ {
		k := strconv2.IntToStr(i)
		if k != keys[i] {
			t.Errorf("err TestPrintSortedMap. got %s want %s", keys[i], k)
		}
	}
}
