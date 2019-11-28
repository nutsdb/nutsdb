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
)

func TestBucketMeta_All(t *testing.T) {
	bucketMeta := &BucketMeta{
		start:     []byte("key100"),
		end:       []byte("key999"),
		startSize: 6,
		endSize:   6,
	}

	expectedEncodeVal := []byte{51, 34, 113, 225, 6, 0, 0, 0, 6, 0, 0, 0, 107, 101, 121, 49, 48, 48, 107, 101, 121, 57, 57, 57}
	if string(expectedEncodeVal) != string(bucketMeta.Encode()) {
		t.Errorf("err bucketMeta.Encode() got %s want %s", string(entry.Encode()), string(expectedEncodeVal))
	}

	var expectedSize int64 = 24
	if bucketMeta.Size() != expectedSize {
		t.Errorf("err bucketMeta.Size() got %v want %v", bucketMeta.Size(), expectedSize)
	}

	var expectedCrc uint32 = 3410108940
	if bucketMeta.GetCrc(bucketMeta.Encode()) != expectedCrc {
		t.Errorf("err bucketMeta.GetCrc got %v want %v", bucketMeta.Size(), expectedCrc)
	}
}
