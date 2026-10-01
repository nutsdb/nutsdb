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

package utils

import "testing"

func TestOneOfUint16Array(t *testing.T) {
	arr := []uint16{1, 2, 3}
	if !OneOfUint16Array(2, arr) {
		t.Fatal("expected true")
	}
	if OneOfUint16Array(9, arr) {
		t.Fatal("expected false")
	}
}

func TestGetDiskSizeFromSingleObject(t *testing.T) {
	type meta struct {
		A uint8
		B uint16
		C uint32
		D uint64
		E string // ignored
	}
	got := GetDiskSizeFromSingleObject(meta{})
	want := int64(1 + 2 + 4 + 8)
	if got != want {
		t.Fatalf("got %d want %d", got, want)
	}
	if GetDiskSizeFromSingleObject(struct{}{}) != 0 {
		t.Fatal("empty struct should be 0")
	}
}

func TestUvarintSize(t *testing.T) {
	cases := []struct {
		x    uint64
		want int
	}{
		{0, 1},
		{127, 1},
		{128, 2},
		{1<<14 - 1, 2},
		{1 << 14, 3},
	}
	for _, tc := range cases {
		if got := UvarintSize(tc.x); got != tc.want {
			t.Fatalf("UvarintSize(%d)=%d want %d", tc.x, got, tc.want)
		}
	}
}

func TestLruCacheCapZeroAndMiss(t *testing.T) {
	c := NewLruCache(0)
	c.Add("k", "v")
	if c.Len() != 0 {
		t.Fatalf("cap=0 should reject adds, len=%d", c.Len())
	}
	c2 := NewLruCache(2)
	c2.Remove("missing")
	if c2.Get("missing") != nil {
		t.Fatal("missing key should be nil")
	}
}
