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
)

func benchKey(i int) []byte {
	return fmt.Appendf(nil, "memtree-bench-key-%012d", i)
}

func newPopulatedMemTree(b *testing.B, n int) (MemTree[testVal], [][]byte) {
	b.Helper()

	tree := newMemTree[testVal]()
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		key := benchKey(i)
		keys[i] = key
		if err := tree.Put(key, tv(i)); err != nil {
			b.Fatalf("populate tree: %v", err)
		}
	}
	return tree, keys
}

func BenchmarkMemTree_Put(b *testing.B) {
	tree := newMemTree[testVal]()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := benchKey(i)
		if err := tree.Put(key, tv(i)); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

func BenchmarkMemTree_PutOverwrite(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("size=%d", n), func(b *testing.B) {
			tree, keys := newPopulatedMemTree(b, n)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := keys[i%n]
				if err := tree.Put(key, tv(i%n)); err != nil {
					b.Fatalf("put overwrite: %v", err)
				}
			}
		})
	}
}

func BenchmarkMemTree_Get(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("size=%d", n), func(b *testing.B) {
			tree, keys := newPopulatedMemTree(b, n)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := tree.Get(keys[i%n]); err != nil {
					b.Fatalf("get: %v", err)
				}
			}
		})
	}
}

func BenchmarkMemTree_GetMiss(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("size=%d", n), func(b *testing.B) {
			tree, _ := newPopulatedMemTree(b, n)
			missing := []byte("memtree-bench-missing-key")

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := tree.Get(missing); err == nil {
					b.Fatal("expected missing key error")
				}
			}
		})
	}
}

func BenchmarkMemTree_Delete(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("size=%d", n), func(b *testing.B) {
			keys := make([][]byte, n)
			for i := 0; i < n; i++ {
				keys[i] = benchKey(i)
			}

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tree := newMemTree[testVal]()
				for j, key := range keys {
					if err := tree.Put(key, tv(j)); err != nil {
						b.Fatalf("populate tree: %v", err)
					}
				}
				b.StartTimer()

				for _, key := range keys {
					if _, ok := tree.Delete(key); !ok {
						b.Fatal("delete existing key failed")
					}
				}
			}
		})
	}
}

func BenchmarkMemTree_Iterate(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("size=%d", n), func(b *testing.B) {
			tree, _ := newPopulatedMemTree(b, n)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var count int
				tree.Iterate(func(key []byte, value testVal) bool {
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

func BenchmarkMemTree_Mixed(b *testing.B) {
	const storeSize = 10_000
	tree, keys := newPopulatedMemTree(b, storeSize)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch i % 4 {
		case 0:
			key := keys[i%storeSize]
			if err := tree.Put(key, tv(i%storeSize)); err != nil {
				b.Fatalf("mixed put: %v", err)
			}
		case 1:
			if _, err := tree.Get(keys[i%storeSize]); err != nil {
				b.Fatalf("mixed get: %v", err)
			}
		case 2:
			key := keys[i%storeSize]
			if _, ok := tree.Delete(key); !ok {
				if err := tree.Put(key, tv(i%storeSize)); err != nil {
					b.Fatalf("mixed repopulate: %v", err)
				}
			}
		case 3:
			tree.Iterate(func(key []byte, value testVal) bool {
				return i%100 != 0
			})
		}
	}
}
