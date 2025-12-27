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
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"
)

// Benchmark suite for tx_btree.go functions
// This file contains comprehensive benchmarks for profiling CPU and memory usage

func setupBenchmarkDB(b *testing.B, dir string) *DB {
	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 128 * 1024 * 1024 // 128MB
	opts.SyncEnable = false              // Disable sync for better performance

	// Clean up directory
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0755)

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}

	// Create test bucket
	err = db.Update(func(tx *Tx) error {
		return tx.NewKVBucket("test_bucket")
	})
	if err != nil {
		b.Fatalf("Failed to create bucket: %v", err)
	}

	return db
}

func cleanupBenchmarkDB(db *DB, dir string) {
	if db != nil && !db.IsClose() {
		db.Close()
	}
	os.RemoveAll(dir)
}

// Benchmark Put operations
func BenchmarkTx_Put(b *testing.B) {
	dir := b.TempDir()
	db := setupBenchmarkDB(b, dir)
	defer cleanupBenchmarkDB(db, dir)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Update(func(tx *Tx) error {
			key := []byte(fmt.Sprintf("key_%d", i))
			value := []byte(fmt.Sprintf("value_%d", i))
			return tx.Put("test_bucket", key, value, Persistent)
		})
		if err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

// Benchmark Get operations
func BenchmarkTx_Get(b *testing.B) {
	dir := b.TempDir()
	db := setupBenchmarkDB(b, dir)
	defer cleanupBenchmarkDB(db, dir)

	// Pre-populate with data
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		err := db.Update(func(tx *Tx) error {
			key := []byte(fmt.Sprintf("key_%d", i))
			value := []byte(fmt.Sprintf("value_%d", i))
			return tx.Put("test_bucket", key, value, Persistent)
		})
		if err != nil {
			b.Fatalf("Setup failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key_%d", i%numKeys))
		err := db.View(func(tx *Tx) error {
			_, err := tx.Get("test_bucket", key)
			return err
		})
		if err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}
}

// Benchmark RangeScan operations
func BenchmarkTx_RangeScan(b *testing.B) {
	dir := b.TempDir()
	db := setupBenchmarkDB(b, dir)
	defer cleanupBenchmarkDB(db, dir)

	// Pre-populate with sorted data
	numKeys := 10000
	err := db.Update(func(tx *Tx) error {
		for i := 0; i < numKeys; i++ {
			key := []byte(fmt.Sprintf("key_%07d", i))
			value := []byte(fmt.Sprintf("value_%d", i))
			if err := tx.Put("test_bucket", key, value, Persistent); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.View(func(tx *Tx) error {
			start := []byte(fmt.Sprintf("key_%07d", 100))
			end := []byte(fmt.Sprintf("key_%07d", 200))
			_, err := tx.RangeScan("test_bucket", start, end)
			return err
		})
		if err != nil {
			b.Fatalf("RangeScan failed: %v", err)
		}
	}
}

// Benchmark PrefixScan operations
func BenchmarkTx_PrefixScan(b *testing.B) {
	dir := b.TempDir()
	db := setupBenchmarkDB(b, dir)
	defer cleanupBenchmarkDB(db, dir)

	// Pre-populate with data
	numKeys := 10000
	err := db.Update(func(tx *Tx) error {
		for i := 0; i < numKeys; i++ {
			key := []byte(fmt.Sprintf("prefix_%07d", i))
			value := []byte(fmt.Sprintf("value_%d", i))
			if err := tx.Put("test_bucket", key, value, Persistent); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.View(func(tx *Tx) error {
			prefix := []byte("prefix_")
			_, err := tx.PrefixScan("test_bucket", prefix, 0, 100)
			return err
		})
		if err != nil {
			b.Fatalf("PrefixScan failed: %v", err)
		}
	}
}

// Benchmark GetAll operations
func BenchmarkTx_GetAll(b *testing.B) {
	dir := b.TempDir()
	db := setupBenchmarkDB(b, dir)
	defer cleanupBenchmarkDB(db, dir)

	// Pre-populate with data
	numKeys := 5000
	err := db.Update(func(tx *Tx) error {
		for i := 0; i < numKeys; i++ {
			key := []byte(fmt.Sprintf("key_%d", i))
			value := []byte(fmt.Sprintf("value_%d", i))
			if err := tx.Put("test_bucket", key, value, Persistent); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.View(func(tx *Tx) error {
			_, _, err := tx.GetAll("test_bucket")
			return err
		})
		if err != nil {
			b.Fatalf("GetAll failed: %v", err)
		}
	}
}

// Benchmark Delete operations
func BenchmarkTx_Delete(b *testing.B) {
	dir := b.TempDir()
	db := setupBenchmarkDB(b, dir)
	defer cleanupBenchmarkDB(db, dir)

	// Pre-populate with data
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		err := db.Update(func(tx *Tx) error {
			key := []byte(fmt.Sprintf("key_%d", i))
			value := []byte(fmt.Sprintf("value_%d", i))
			return tx.Put("test_bucket", key, value, Persistent)
		})
		if err != nil {
			b.Fatalf("Setup failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key_%d", i%numKeys))
		err := db.Update(func(tx *Tx) error {
			return tx.Delete("test_bucket", key)
		})
		if err != nil {
			b.Fatalf("Delete failed: %v", err)
		}
	}
}

// Benchmark Mixed operations (realistic workload)
func BenchmarkTx_MixedOperations(b *testing.B) {
	dir := b.TempDir()
	db := setupBenchmarkDB(b, dir)
	defer cleanupBenchmarkDB(db, dir)

	// Pre-populate with some data
	for i := 0; i < 1000; i++ {
		err := db.Update(func(tx *Tx) error {
			key := []byte(fmt.Sprintf("key_%d", i))
			value := []byte(fmt.Sprintf("value_%d", i))
			return tx.Put("test_bucket", key, value, Persistent)
		})
		if err != nil {
			b.Fatalf("Setup failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < b.N; i++ {
		operation := rand.Intn(4)
		key := []byte(fmt.Sprintf("key_%d", rand.Intn(1000)))

		switch operation {
		case 0: // Put
			err := db.Update(func(tx *Tx) error {
				value := []byte(fmt.Sprintf("value_%d", i))
				return tx.Put("test_bucket", key, value, Persistent)
			})
			if err != nil {
				b.Fatalf("Put failed: %v", err)
			}

		case 1: // Get
			err := db.View(func(tx *Tx) error {
				_, err := tx.Get("test_bucket", key)
				return err
			})
			if err != nil && err != ErrKeyNotFound {
				b.Fatalf("Get failed: %v", err)
			}

		case 2: // Delete
			err := db.Update(func(tx *Tx) error {
				return tx.Delete("test_bucket", key)
			})
			if err != nil && err != ErrKeyNotFound {
				b.Fatalf("Delete failed: %v", err)
			}

		case 3: // Has
			err := db.View(func(tx *Tx) error {
				_, err := tx.Has("test_bucket", key)
				return err
			})
			if err != nil && err != ErrBucketNotExist {
				b.Fatalf("Has failed: %v", err)
			}
		}
	}
}

// Benchmark concurrent operations
func BenchmarkTx_ConcurrentOperations(b *testing.B) {
	dir := b.TempDir()
	db := setupBenchmarkDB(b, dir)
	defer cleanupBenchmarkDB(db, dir)

	b.ResetTimer()
	b.ReportAllocs()

	// Run concurrent goroutines
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			key := []byte(fmt.Sprintf("key_%d", r.Intn(1000)))
			value := []byte(fmt.Sprintf("value_%d", r.Intn(1000)))

			err := db.Update(func(tx *Tx) error {
				return tx.Put("test_bucket", key, value, Persistent)
			})
			if err != nil {
				b.Errorf("Concurrent Put failed: %v", err)
			}
		}
	})
}

// Benchmark TTL operations
func BenchmarkTx_TTLOperations(b *testing.B) {
	dir := b.TempDir()
	db := setupBenchmarkDB(b, dir)
	defer cleanupBenchmarkDB(db, dir)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))

		err := db.Update(func(tx *Tx) error {
			return tx.Put("test_bucket", key, value, 3600) // 1 hour TTL
		})
		if err != nil {
			b.Fatalf("TTL Put failed: %v", err)
		}

		err = db.View(func(tx *Tx) error {
			_, err := tx.GetTTL("test_bucket", key)
			return err
		})
		if err != nil {
			b.Fatalf("GetTTL failed: %v", err)
		}
	}
}

// Benchmark large value operations
func BenchmarkTx_LargeValues(b *testing.B) {
	dir := b.TempDir()
	db := setupBenchmarkDB(b, dir)
	defer cleanupBenchmarkDB(db, dir)

	// Create a large value (64KB)
	largeValue := make([]byte, 64*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("large_key_%d", i))

		err := db.Update(func(tx *Tx) error {
			return tx.Put("test_bucket", key, largeValue, Persistent)
		})
		if err != nil {
			b.Fatalf("Large value Put failed: %v", err)
		}

		err = db.View(func(tx *Tx) error {
			_, err := tx.Get("test_bucket", key)
			return err
		})
		if err != nil {
			b.Fatalf("Large value Get failed: %v", err)
		}
	}
}

// Benchmark transaction commit performance
func BenchmarkTx_TransactionCommit(b *testing.B) {
	dir := b.TempDir()
	db := setupBenchmarkDB(b, dir)
	defer cleanupBenchmarkDB(db, dir)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Update(func(tx *Tx) error {
			// Multiple operations in a single transaction
			for j := 0; j < 100; j++ {
				key := []byte(fmt.Sprintf("key_%d_%d", i, j))
				value := []byte(fmt.Sprintf("value_%d_%d", i, j))
				if err := tx.Put("test_bucket", key, value, Persistent); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatalf("Transaction commit failed: %v", err)
		}
	}
}

// Memory stress test - allocates and releases many objects
func BenchmarkTx_MemoryStress(b *testing.B) {
	dir := b.TempDir()
	db := setupBenchmarkDB(b, dir)
	defer cleanupBenchmarkDB(db, dir)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Create many keys and values to stress memory
		err := db.Update(func(tx *Tx) error {
			for j := 0; j < 1000; j++ {
				key := make([]byte, 64)
				value := make([]byte, 256)

				// Fill with random data
				for k := range key {
					key[k] = byte(rand.Intn(256))
				}
				for k := range value {
					value[k] = byte(rand.Intn(256))
				}

				if err := tx.Put("test_bucket", key, value, Persistent); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatalf("Memory stress test failed: %v", err)
		}

		// Force GC to get more accurate memory measurements
		runtime.GC()
	}
}
