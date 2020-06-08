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
	"os"
	"testing"
)

func TestBPTreeRootIdx_All(t *testing.T) {
	bri1 := &BPTreeRootIdx{
		fID:       0,
		rootOff:   0,
		start:     []byte("key001"),
		end:       []byte("key010"),
		startSize: 6,
		endSize:   6,
	}

	path := "/tmp/bri_test.idx"
	numbers, err := bri1.Persistence(path, 0, true)
	if numbers != 40 || err != nil {
		t.Error("err Persistence")
	}

	fd, _ := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	idx, err := ReadBPTreeRootIdxAt(fd, 0)
	if err != nil {
		t.Error(err)
	}
	if string(idx.start) != "key001" || string(idx.end) != "key010" {
		t.Error("err ReadBPTreeRootIdxAt")
	}

	if idx.IsZero() == true {
		t.Error("err ReadBPTreeRootIdxAt")
	}

	var bptSparseIdxGroup []*BPTreeRootIdx

	bri2 := &BPTreeRootIdx{
		fID:       1,
		rootOff:   0,
		start:     []byte("key011"),
		end:       []byte("key020"),
		startSize: 6,
		endSize:   6,
	}

	bptSparseIdxGroup = append(bptSparseIdxGroup, bri1, bri2)

	SortFID(bptSparseIdxGroup, func(p, q *BPTreeRootIdx) bool {
		return p.fID > q.fID
	})

	if bptSparseIdxGroup[0].fID != 1 {
		t.Error("err sort")
	}

	if bptSparseIdxGroup[1].fID != 0 {
		t.Error("err sort")
	}
}

func TestBPTreeRootIdx_errors(t *testing.T) {
	path := "/tmp/bri_test.idx"
	fd, _ := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	idx, err := ReadBPTreeRootIdxAt(fd, 100)
	if err == nil || idx != nil {
		t.Error("err TestBPTreeRootIdx_errors")
	}
}
