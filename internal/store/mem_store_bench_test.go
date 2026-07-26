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

package store

import (
	"fmt"
	"testing"

	"github.com/nutsdb/nutsdb/internal/fileio"
)

func benchKey(i int) []byte {
	return []byte(fmt.Sprintf("memstore-bench-key-%012d", i))
}

func benchLoc(i int) fileio.Location {
	return fileio.Location{
		FileID: 1,
		Offset: uint64(fileio.HeaderSize + i*32),
		Length: 32,
	}
}

func newPopulatedMemStore(b *testing.B, n int) (MemStore, [][]byte) {
	b.Helper()

	ms := NewMemStore()
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		key := benchKey(i)
		keys[i] = key
		if err := ms.Put(key, benchLoc(i)); err != nil {
			b.Fatalf("populate store: %v", err)
		}
	}
	return ms, keys
}

func BenchmarkMemStore_Put(b *testing.B) {
	ms := NewMemStore()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := benchKey(i)
		if err := ms.Put(key, benchLoc(i)); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

func BenchmarkMemStore_PutOverwrite(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("size=%d", n), func(b *testing.B) {
			ms, keys := newPopulatedMemStore(b, n)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := keys[i%n]
				if err := ms.Put(key, benchLoc(i%n)); err != nil {
					b.Fatalf("put overwrite: %v", err)
				}
			}
		})
	}
}

func BenchmarkMemStore_Get(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("size=%d", n), func(b *testing.B) {
			ms, keys := newPopulatedMemStore(b, n)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := ms.Get(keys[i%n]); err != nil {
					b.Fatalf("get: %v", err)
				}
			}
		})
	}
}

func BenchmarkMemStore_GetMiss(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("size=%d", n), func(b *testing.B) {
			ms, _ := newPopulatedMemStore(b, n)
			missing := []byte("memstore-bench-missing-key")

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := ms.Get(missing); err == nil {
					b.Fatal("expected missing key error")
				}
			}
		})
	}
}

func BenchmarkMemStore_Delete(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("size=%d", n), func(b *testing.B) {
			keys := make([][]byte, n)
			for i := 0; i < n; i++ {
				keys[i] = benchKey(i)
			}

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				ms := NewMemStore()
				for j, key := range keys {
					if err := ms.Put(key, benchLoc(j)); err != nil {
						b.Fatalf("populate store: %v", err)
					}
				}
				b.StartTimer()

				for _, key := range keys {
					if _, ok := ms.Delete(key); !ok {
						b.Fatal("delete existing key failed")
					}
				}
			}
		})
	}
}

func BenchmarkMemStore_Iterate(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("size=%d", n), func(b *testing.B) {
			ms, _ := newPopulatedMemStore(b, n)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var count int
				ms.Iterate(func(key []byte, value fileio.Location) bool {
					count++
					return true
				})
				if count != n {
					b.Fatalf("iterate count = %d, want %d", count, n)
				}
			}
		})
	}
}

func BenchmarkMemStore_Mixed(b *testing.B) {
	const storeSize = 10_000
	ms, keys := newPopulatedMemStore(b, storeSize)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch i % 4 {
		case 0:
			key := keys[i%storeSize]
			if err := ms.Put(key, benchLoc(i%storeSize)); err != nil {
				b.Fatalf("mixed put: %v", err)
			}
		case 1:
			if _, err := ms.Get(keys[i%storeSize]); err != nil {
				b.Fatalf("mixed get: %v", err)
			}
		case 2:
			key := keys[i%storeSize]
			if _, ok := ms.Delete(key); !ok {
				if err := ms.Put(key, benchLoc(i%storeSize)); err != nil {
					b.Fatalf("mixed repopulate: %v", err)
				}
			}
		case 3:
			ms.Iterate(func(key []byte, value fileio.Location) bool {
				return i%100 != 0
			})
		}
	}
}
