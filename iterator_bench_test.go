// Copyright 2024 The nutsdb Author. All rights reserved.
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
	"testing"

	"github.com/nutsdb/nutsdb/internal/testutils"
)

// setupIteratorBenchmark prepares a database with test data
func setupIteratorBenchmark(b *testing.B, numKeys int) (*DB, string) {
	dir := b.TempDir()
	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 128 * 1024 * 1024 // 128MB
	opts.SyncEnable = false

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}

	bucket := "benchmark_bucket"
	err = db.Update(func(tx *Tx) error {
		if err := tx.NewKVBucket(bucket); err != nil {
			return err
		}
		// Batch insert for faster setup
		for i := 0; i < numKeys; i++ {
			key := testutils.GetTestBytes(i)
			value := testutils.GetTestBytes(i * 2)
			if err := tx.Put(bucket, key, value, Persistent); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		b.Fatalf("Failed to populate database: %v", err)
	}

	return db, bucket
}

// BenchmarkIterator_Creation tests iterator creation overhead
func BenchmarkIterator_Creation(b *testing.B) {
	db, bucket := setupIteratorBenchmark(b, 10000)
	defer func() { _ = db.Close() }()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			if iterator == nil {
				b.Fatal("Failed to create iterator")
			}
			iterator.Release()
			return nil
		})
		if err != nil {
			b.Fatalf("View failed: %v", err)
		}
	}
}

// BenchmarkIterator_Next tests forward iteration performance
func BenchmarkIterator_Next(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			db, bucket := setupIteratorBenchmark(b, size)
			defer func() { _ = db.Close() }()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				err := db.View(func(tx *Tx) error {
					iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
					defer iterator.Release()

					count := 0
					for iterator.Valid() {
						count++
						if !iterator.Next() {
							break
						}
					}
					return nil
				})
				if err != nil {
					b.Fatalf("View failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkIterator_Prev tests reverse iteration performance
func BenchmarkIterator_Prev(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			db, bucket := setupIteratorBenchmark(b, size)
			defer func() { _ = db.Close() }()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				err := db.View(func(tx *Tx) error {
					iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: true})
					defer iterator.Release()

					count := 0
					for iterator.Valid() {
						count++
						if !iterator.Next() {
							break
						}
					}
					return nil
				})
				if err != nil {
					b.Fatalf("View failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkIterator_KeyAccess tests key access performance
func BenchmarkIterator_KeyAccess(b *testing.B) {
	db, bucket := setupIteratorBenchmark(b, 10000)
	defer func() { _ = db.Close() }()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			defer iterator.Release()

			for iterator.Valid() {
				_ = iterator.Key()
				if !iterator.Next() {
					break
				}
			}
			return nil
		})
		if err != nil {
			b.Fatalf("View failed: %v", err)
		}
	}
}

// BenchmarkIterator_ValueAccess tests value access performance
func BenchmarkIterator_ValueAccess(b *testing.B) {
	db, bucket := setupIteratorBenchmark(b, 10000)
	defer func() { _ = db.Close() }()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			defer iterator.Release()

			for iterator.Valid() {
				_, err := iterator.Value()
				if err != nil {
					return err
				}
				if !iterator.Next() {
					break
				}
			}
			return nil
		})
		if err != nil {
			b.Fatalf("View failed: %v", err)
		}
	}
}

// BenchmarkIterator_KeyValueAccess tests combined key-value access
func BenchmarkIterator_KeyValueAccess(b *testing.B) {
	db, bucket := setupIteratorBenchmark(b, 10000)
	defer func() { _ = db.Close() }()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			defer iterator.Release()

			for iterator.Valid() {
				_ = iterator.Key()
				_, err := iterator.Value()
				if err != nil {
					return err
				}
				if !iterator.Next() {
					break
				}
			}
			return nil
		})
		if err != nil {
			b.Fatalf("View failed: %v", err)
		}
	}
}

// BenchmarkIterator_Seek tests seek operation performance
func BenchmarkIterator_Seek(b *testing.B) {
	db, bucket := setupIteratorBenchmark(b, 100000)
	defer func() { _ = db.Close() }()

	// Pre-generate seek keys
	seekKeys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		seekKeys[i] = testutils.GetTestBytes(i * 100)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			defer iterator.Release()

			seekKey := seekKeys[i%1000]
			if !iterator.Seek(seekKey) {
				return fmt.Errorf("seek failed for key %v", seekKey)
			}
			return nil
		})
		if err != nil {
			b.Fatalf("View failed: %v", err)
		}
	}
}

// BenchmarkIterator_Rewind tests rewind operation performance
func BenchmarkIterator_Rewind(b *testing.B) {
	db, bucket := setupIteratorBenchmark(b, 10000)
	defer func() { _ = db.Close() }()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			defer iterator.Release()

			// Move forward a bit
			for j := 0; j < 100 && iterator.Valid(); j++ {
				iterator.Next()
			}

			// Rewind back to start
			if !iterator.Rewind() {
				return fmt.Errorf("rewind failed")
			}
			return nil
		})
		if err != nil {
			b.Fatalf("View failed: %v", err)
		}
	}
}

// BenchmarkIterator_PartialScan tests scanning a portion of data
func BenchmarkIterator_PartialScan(b *testing.B) {
	db, bucket := setupIteratorBenchmark(b, 100000)
	defer func() { _ = db.Close() }()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			defer iterator.Release()

			// Seek to starting position
			startKey := testutils.GetTestBytes(10000)
			if !iterator.Seek(startKey) {
				return fmt.Errorf("seek failed")
			}

			// Scan 1000 items
			count := 0
			for iterator.Valid() && count < 1000 {
				_ = iterator.Key()
				_, err := iterator.Value()
				if err != nil {
					return err
				}
				count++
				if !iterator.Next() {
					break
				}
			}
			return nil
		})
		if err != nil {
			b.Fatalf("View failed: %v", err)
		}
	}
}

// BenchmarkIterator_ValidCheck tests Valid() method overhead
func BenchmarkIterator_ValidCheck(b *testing.B) {
	db, bucket := setupIteratorBenchmark(b, 1000)
	defer func() { _ = db.Close() }()

	b.ResetTimer()
	b.ReportAllocs()

	err := db.View(func(tx *Tx) error {
		iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
		defer iterator.Release()

		for i := 0; i < b.N; i++ {
			_ = iterator.Valid()
		}
		return nil
	})
	if err != nil {
		b.Fatalf("View failed: %v", err)
	}
}

// BenchmarkIterator_MultipleIterators tests overhead of multiple concurrent iterators
func BenchmarkIterator_MultipleIterators(b *testing.B) {
	db, bucket := setupIteratorBenchmark(b, 10000)
	defer func() { _ = db.Close() }()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.View(func(tx *Tx) error {
			// Create 10 iterators
			iterators := make([]*Iterator, 10)
			for j := 0; j < 10; j++ {
				iterators[j] = NewIterator(tx, bucket, IteratorOptions{Reverse: j%2 == 0})
			}

			// Use all iterators
			for j := 0; j < 10; j++ {
				if iterators[j].Valid() {
					_ = iterators[j].Key()
					iterators[j].Next()
				}
			}

			// Release all iterators
			for j := 0; j < 10; j++ {
				iterators[j].Release()
			}
			return nil
		})
		if err != nil {
			b.Fatalf("View failed: %v", err)
		}
	}
}

// BenchmarkIterator_LargeValues tests iteration with large values
func BenchmarkIterator_LargeValues(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 256 * 1024 * 1024
	opts.SyncEnable = false

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	bucket := "large_bucket"
	largeValue := make([]byte, 64*1024) // 64KB values

	err = db.Update(func(tx *Tx) error {
		if err := tx.NewKVBucket(bucket); err != nil {
			return err
		}
		for i := 0; i < 1000; i++ {
			key := testutils.GetTestBytes(i)
			if err := tx.Put(bucket, key, largeValue, Persistent); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatalf("Failed to populate database: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			defer iterator.Release()

			for iterator.Valid() {
				_, err := iterator.Value()
				if err != nil {
					return err
				}
				if !iterator.Next() {
					break
				}
			}
			return nil
		})
		if err != nil {
			b.Fatalf("View failed: %v", err)
		}
	}
}
