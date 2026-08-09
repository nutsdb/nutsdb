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
	"context"
	"fmt"
	"testing"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/fileio"
)

// StoreManager benchmarks focus on public API throughput under SyncBatch
// (default durability). Run with:
//
//	go test ./internal/store/ -bench=BenchmarkStoreManager -benchmem -count=1
//
// Optional filters:
//
//	go test ./internal/store/ -bench=BenchmarkStoreManager_Get -benchmem
//	go test ./internal/store/ -bench=BenchmarkStoreManager_Put -benchmem

func benchStoreOpts(dir string) LSMOptions {
	opts := DefaultLSMOptions(dir)
	// Match production-ish defaults; SyncEveryWrite would dominate wall time.
	opts.WALSyncMode = fileio.SyncBatch
	opts.ValueLog.SyncMode = fileio.SyncBatch
	return opts
}

func benchStoreKey(i int) []byte {
	return fmt.Appendf(nil, "sm-bench-key-%012d", i)
}

func benchStoreValue(size int, seed byte) []byte {
	v := make([]byte, size)
	for i := range v {
		v[i] = seed + byte(i)
	}
	return v
}

func openBenchStore(b *testing.B) (StoreManager, context.Context) {
	b.Helper()
	st, err := OpenStoreManager(benchStoreOpts(b.TempDir()))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = st.Close() })
	return st, context.Background()
}

func populateStore(b *testing.B, st StoreManager, ctx context.Context, n, valueSize int) [][]byte {
	b.Helper()
	keys := make([][]byte, n)
	val := benchStoreValue(valueSize, 'v')
	for i := 0; i < n; i++ {
		keys[i] = benchStoreKey(i)
		if err := st.Put(ctx, keys[i], core.NewRecord().WithValue(val)); err != nil {
			b.Fatalf("populate put: %v", err)
		}
	}
	return keys
}

func BenchmarkStoreManager_Put(b *testing.B) {
	// valueSize=64  → inline; valueSize=4096 → ValueLog location
	for _, valueSize := range []int{64, 4096} {
		b.Run(fmt.Sprintf("value=%dB", valueSize), func(b *testing.B) {
			st, ctx := openBenchStore(b)
			val := benchStoreValue(valueSize, 'p')
			rec := core.NewRecord().WithValue(val)

			b.SetBytes(int64(len(benchStoreKey(0)) + valueSize))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := st.Put(ctx, benchStoreKey(i), rec); err != nil {
					b.Fatalf("put: %v", err)
				}
			}
		})
	}
}

func BenchmarkStoreManager_PutOverwrite(b *testing.B) {
	const n = 10_000
	const valueSize = 64

	st, ctx := openBenchStore(b)
	keys := populateStore(b, st, ctx, n, valueSize)
	val := benchStoreValue(valueSize, 'o')
	rec := core.NewRecord().WithValue(val)

	b.SetBytes(int64(len(keys[0]) + valueSize))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := st.Put(ctx, keys[i%n], rec); err != nil {
			b.Fatalf("put overwrite: %v", err)
		}
	}
}

func BenchmarkStoreManager_Get(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("keys=%d/value=64B", n), func(b *testing.B) {
			st, ctx := openBenchStore(b)
			keys := populateStore(b, st, ctx, n, 64)

			b.SetBytes(int64(len(keys[0]) + 64))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := st.Get(ctx, keys[i%n]); err != nil {
					b.Fatalf("get: %v", err)
				}
			}
		})
	}
}

func BenchmarkStoreManager_Get_ValueLog(b *testing.B) {
	const n = 10_000
	const valueSize = 4096

	st, ctx := openBenchStore(b)
	keys := populateStore(b, st, ctx, n, valueSize)

	b.SetBytes(int64(len(keys[0]) + valueSize))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := st.Get(ctx, keys[i%n]); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkStoreManager_Delete(b *testing.B) {
	const valueSize = 64

	st, ctx := openBenchStore(b)
	val := benchStoreValue(valueSize, 'd')
	rec := core.NewRecord().WithValue(val)

	b.SetBytes(int64(len(benchStoreKey(0))))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := benchStoreKey(i)
		b.StopTimer()
		if err := st.Put(ctx, key, rec); err != nil {
			b.Fatalf("setup put: %v", err)
		}
		b.StartTimer()
		if err := st.Delete(ctx, key); err != nil {
			b.Fatalf("delete: %v", err)
		}
	}
}

func BenchmarkStoreManager_BatchPut(b *testing.B) {
	const valueSize = 64
	for _, batchSize := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			st, ctx := openBenchStore(b)
			val := benchStoreValue(valueSize, 'b')
			records := make([]struct {
				Key   []byte
				Value *core.Record
			}, batchSize)

			b.SetBytes(int64(batchSize * (len(benchStoreKey(0)) + valueSize)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				base := i * batchSize
				for j := 0; j < batchSize; j++ {
					records[j].Key = benchStoreKey(base + j)
					records[j].Value = core.NewRecord().WithValue(val)
				}
				if err := st.BatchPut(ctx, records); err != nil {
					b.Fatalf("batch put: %v", err)
				}
			}
		})
	}
}

func BenchmarkStoreManager_BatchGet(b *testing.B) {
	const n = 10_000
	const valueSize = 64
	for _, batchSize := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			st, ctx := openBenchStore(b)
			keys := populateStore(b, st, ctx, n, valueSize)
			batch := make([][]byte, batchSize)

			b.SetBytes(int64(batchSize * (len(keys[0]) + valueSize)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				off := (i * batchSize) % n
				for j := 0; j < batchSize; j++ {
					batch[j] = keys[(off+j)%n]
				}
				if _, err := st.BatchGet(ctx, batch); err != nil {
					b.Fatalf("batch get: %v", err)
				}
			}
		})
	}
}

func BenchmarkStoreManager_Iterate(b *testing.B) {
	for _, n := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			st, ctx := openBenchStore(b)
			_ = populateStore(b, st, ctx, n, 64)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				count := 0
				if err := st.Iterate(ctx, func(_ []byte, _ *core.Record) bool {
					count++
					return true
				}); err != nil {
					b.Fatalf("iterate: %v", err)
				}
				if count != n {
					b.Fatalf("iterate count=%d want=%d", count, n)
				}
			}
		})
	}
}

func BenchmarkStoreManager_MixedReadWrite(b *testing.B) {
	// 90% Get / 10% Put — rough OLTP-style mix on a warm key space.
	const n = 10_000
	const valueSize = 64

	st, ctx := openBenchStore(b)
	keys := populateStore(b, st, ctx, n, valueSize)
	val := benchStoreValue(valueSize, 'm')
	rec := core.NewRecord().WithValue(val)

	b.SetBytes(int64(len(keys[0]) + valueSize))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%n]
		if i%10 == 0 {
			if err := st.Put(ctx, key, rec); err != nil {
				b.Fatalf("put: %v", err)
			}
			continue
		}
		if _, err := st.Get(ctx, key); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}
