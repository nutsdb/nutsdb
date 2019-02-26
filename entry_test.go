// Copyright 2019 The nutsdb Authors. All rights reserved.
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
)

func TestEntry_All(t *testing.T) {
	entry := Entry{
		Key:   []byte("key_0001"),
		Value: []byte("val_0001"),
		Meta: &MetaData{
			keySize:    uint32(len("key_0001")),
			valueSize:  uint32(len("val_0001")),
			timestamp:  1547707905,
			TTL:        Persistent,
			bucket:     []byte("test_entry"),
			bucketSize: uint32(len("test_datafile")),
			Flag:       DataSetFlag,
		},
		position: 0,
	}

	expectedEncodeVal := []byte{172, 41, 40, 169, 1, 38, 64, 92, 0, 0, 0, 0, 8, 0, 0, 0, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 116, 101, 115, 116, 95, 101, 110, 116, 114, 121, 0, 0, 0, 107, 101, 121, 95, 48, 48, 48, 49, 118, 97, 108, 95, 48, 48, 48, 49}

	if string(expectedEncodeVal) != string(entry.Encode()) {
		t.Errorf("err TestEntry_Encode got %s want %s", string(entry.Encode()), string(expectedEncodeVal))
	}

	if entry.IsZero() {
		t.Errorf("err entry.IsZero got %v want %v", true, false)
	}

	if entry.GetCrc(entry.Encode()) != 529078050 {
		t.Errorf("err entry.GetCrc got %d want %d", entry.GetCrc(entry.Encode()), 2777557425)
	}
}
