// Copyright 2023 The nutsdb Author. All rights reserved.
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
	"os"
	"testing"
	"time"
)

// BenchmarkHintFileEncode benchmarks the encoding of HintEntry
func BenchmarkHintFileEncode(b *testing.B) {
	entry := &HintEntry{
		BucketId:  1,
		KeySize:   10,
		ValueSize: 100,
		Timestamp: uint64(time.Now().UnixMilli()),
		TTL:       3600,
		Flag:      DataSetFlag,
		Status:    Committed,
		Ds:        DataStructureBTree,
		DataPos:   1000,
		FileID:    1,
		Key:       []byte("testkey123"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = entry.Encode()
	}
}

// BenchmarkHintFileDecode benchmarks the decoding of HintEntry
func BenchmarkHintFileDecode(b *testing.B) {
	entry := &HintEntry{
		BucketId:  1,
		KeySize:   10,
		ValueSize: 100,
		Timestamp: uint64(time.Now().UnixMilli()),
		TTL:       3600,
		Flag:      DataSetFlag,
		Status:    Committed,
		Ds:        DataStructureBTree,
		DataPos:   1000,
		FileID:    1,
		Key:       []byte("testkey123"),
	}

	encoded := entry.Encode()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoded := &HintEntry{}
		_ = decoded.Decode(encoded)
	}
}

// BenchmarkHintFileWrite benchmarks writing hint entries to file
func BenchmarkHintFileWrite(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "hintfile-bench-write")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	hintPath := tmpDir + "/test.hint"

	entries := make([]*HintEntry, 1000)
	for i := 0; i < 1000; i++ {
		entries[i] = &HintEntry{
			BucketId:  1,
			KeySize:   10,
			ValueSize: 100,
			Timestamp: uint64(time.Now().UnixMilli()),
			TTL:       3600,
			Flag:      DataSetFlag,
			Status:    Committed,
			Ds:        DataStructureBTree,
			DataPos:   uint64(i * 10),
			FileID:    1,
			Key:       []byte(fmt.Sprintf("key%d", i)),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer := &HintFileWriter{}
		err := writer.Create(hintPath)
		if err != nil {
			b.Fatalf("Failed to create hint file: %v", err)
		}

		for _, entry := range entries {
			err := writer.Write(entry)
			if err != nil {
				b.Fatalf("Failed to write hint entry: %v", err)
			}
		}

		err = writer.Close()
		if err != nil {
			b.Fatalf("Failed to close hint file: %v", err)
		}

		os.Remove(hintPath)
	}
}

// BenchmarkHintFileRead benchmarks reading hint entries from file
func BenchmarkHintFileRead(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "hintfile-bench-read")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	hintPath := tmpDir + "/test.hint"

	// Create a hint file with 1000 entries
	writer := &HintFileWriter{}
	err = writer.Create(hintPath)
	if err != nil {
		b.Fatalf("Failed to create hint file: %v", err)
	}

	entries := make([]*HintEntry, 1000)
	for i := 0; i < 1000; i++ {
		entries[i] = &HintEntry{
			BucketId:  1,
			KeySize:   10,
			ValueSize: 100,
			Timestamp: uint64(time.Now().UnixMilli()),
			TTL:       3600,
			Flag:      DataSetFlag,
			Status:    Committed,
			Ds:        DataStructureBTree,
			DataPos:   uint64(i * 10),
			FileID:    1,
			Key:       []byte(fmt.Sprintf("key%d", i)),
		}

		err := writer.Write(entries[i])
		if err != nil {
			b.Fatalf("Failed to write hint entry: %v", err)
		}
	}

	err = writer.Close()
	if err != nil {
		b.Fatalf("Failed to close hint file: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := &HintFileReader{}
		err := reader.Open(hintPath)
		if err != nil {
			b.Fatalf("Failed to open hint file: %v", err)
		}

		count := 0
		for {
			_, err := reader.Read()
			if err != nil {
				break
			}
			count++
		}

		if count != 1000 {
			b.Fatalf("Expected to read 1000 entries, got %d", count)
		}

		err = reader.Close()
		if err != nil {
			b.Fatalf("Failed to close hint file: %v", err)
		}
	}
}

// BenchmarkDBStartupWithHintFile benchmarks database startup with hint files
func BenchmarkDBStartupWithHintFile(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "hintfile-bench-startup")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultOptions
	opts.Dir = tmpDir
	opts.SegmentSize = 64 * KB
	opts.EnableHintFile = true
	opts.EntryIdxMode = HintKeyAndRAMIdxMode

	// Prepare database with data and hint files
	start := time.Now()
	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	openTime := time.Since(start)

	bucket := "bucket"
	txCreateBucket(&testing.T{}, db, DataStructureBTree, bucket, nil)

	// Add enough data to trigger merge
	start = time.Now()
	for i := 0; i < 5000000; i++ {
		txPut(&testing.T{}, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
	}
	insertTime := time.Since(start)
	b.Logf("插入阶段耗时: %v", insertTime)

	// Perform merge to create hint files
	start = time.Now()
	err = db.Merge()
	if err != nil {
		b.Fatalf("Failed to merge: %v", err)
	}
	mergeTime := time.Since(start)
	b.Logf("Merge阶段耗时: %v", mergeTime)

	start = time.Now()
	err = db.Close()
	if err != nil {
		b.Fatalf("Failed to close database: %v", err)
	}
	closeTime := time.Since(start)
	b.Logf("初始关闭耗时: %v", closeTime)
	b.Logf("初始打开耗时: %v", openTime)

	b.ResetTimer()
	var totalStartupTime time.Duration
	for i := 0; i < b.N; i++ {
		start = time.Now()
		db, err := Open(opts)
		if err != nil {
			b.Fatalf("Failed to open database: %v", err)
		}
		startupTime := time.Since(start)
		totalStartupTime += startupTime

		err = db.Close()
		if err != nil {
			b.Fatalf("Failed to close database: %v", err)
		}
	}
	avgStartupTime := totalStartupTime / time.Duration(b.N)
	b.Logf("平均重新启动耗时: %v", avgStartupTime)
}

// BenchmarkDBStartupWithoutHintFile benchmarks database startup without hint files
func BenchmarkDBStartupWithoutHintFile(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "hintfile-bench-startup-no-hint")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultOptions
	opts.Dir = tmpDir
	opts.SegmentSize = 64 * KB
	opts.EntryIdxMode = HintKeyAndRAMIdxMode
	opts.EnableHintFile = false

	// Prepare database with data but no hint files
	start := time.Now()
	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	openTime := time.Since(start)

	bucket := "bucket"
	txCreateBucket(&testing.T{}, db, DataStructureBTree, bucket, nil)

	// Add enough data to trigger merge
	start = time.Now()
	for i := 0; i < 5000000; i++ {
		txPut(&testing.T{}, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
	}
	insertTime := time.Since(start)
	b.Logf("插入阶段耗时: %v", insertTime)

	// Perform merge (should not create hint files)
	start = time.Now()
	err = db.Merge()
	if err != nil {
		b.Fatalf("Failed to merge: %v", err)
	}
	mergeTime := time.Since(start)
	b.Logf("Merge阶段耗时: %v", mergeTime)

	start = time.Now()
	err = db.Close()
	if err != nil {
		b.Fatalf("Failed to close database: %v", err)
	}
	closeTime := time.Since(start)
	b.Logf("初始关闭耗时: %v", closeTime)
	b.Logf("初始打开耗时: %v", openTime)

	b.ResetTimer()
	var totalStartupTime time.Duration
	for i := 0; i < b.N; i++ {
		start = time.Now()
		db, err := Open(opts)
		if err != nil {
			b.Fatalf("Failed to open database: %v", err)
		}
		startupTime := time.Since(start)
		totalStartupTime += startupTime

		err = db.Close()
		if err != nil {
			b.Fatalf("Failed to close database: %v", err)
		}
	}
	avgStartupTime := totalStartupTime / time.Duration(b.N)
	b.Logf("平均重新启动耗时: %v", avgStartupTime)
}

// BenchmarkMergeWithHintFile benchmarks merge operation with hint files enabled
func BenchmarkMergeWithHintFile(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "hintfile-bench-merge")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultOptions
	opts.Dir = tmpDir
	opts.SegmentSize = 64 * KB
	opts.EnableHintFile = true

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create fresh database for each iteration
		iterDir := fmt.Sprintf("%s/iter%d", tmpDir, i)
		opts.Dir = iterDir

		db, err := Open(opts)
		if err != nil {
			b.Fatalf("Failed to open database: %v", err)
		}

		bucket := "bucket"
		txCreateBucket(&testing.T{}, db, DataStructureBTree, bucket, nil)

		// Add data and delete some to trigger merge
		for j := 0; j < 2000; j++ {
			txPut(&testing.T{}, db, bucket, GetTestBytes(j), GetTestBytes(j), Persistent, nil, nil)
		}

		for j := 0; j < 500; j++ {
			txDel(&testing.T{}, db, bucket, GetTestBytes(j), nil)
		}

		// Perform merge with hint file creation
		b.StopTimer()
		start := time.Now()
		b.StartTimer()

		err = db.Merge()
		if err != nil {
			b.Fatalf("Failed to merge: %v", err)
		}

		b.StopTimer()
		mergeTime := time.Since(start)
		b.ReportMetric(float64(mergeTime.Nanoseconds())/1e6, "ms")
		b.StartTimer()

		err = db.Close()
		if err != nil {
			b.Fatalf("Failed to close database: %v", err)
		}
	}
}

// BenchmarkMergeWithoutHintFile benchmarks merge operation without hint files enabled
func BenchmarkMergeWithoutHintFile(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "hintfile-bench-merge-no-hint")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultOptions
	opts.Dir = tmpDir
	opts.SegmentSize = 64 * KB
	opts.EnableHintFile = false

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create fresh database for each iteration
		iterDir := fmt.Sprintf("%s/iter%d", tmpDir, i)
		opts.Dir = iterDir

		db, err := Open(opts)
		if err != nil {
			b.Fatalf("Failed to open database: %v", err)
		}

		bucket := "bucket"
		txCreateBucket(&testing.T{}, db, DataStructureBTree, bucket, nil)

		// Add data and delete some to trigger merge
		for j := 0; j < 2000; j++ {
			txPut(&testing.T{}, db, bucket, GetTestBytes(j), GetTestBytes(j), Persistent, nil, nil)
		}

		for j := 0; j < 500; j++ {
			txDel(&testing.T{}, db, bucket, GetTestBytes(j), nil)
		}

		// Perform merge without hint file creation
		b.StopTimer()
		start := time.Now()
		b.StartTimer()

		err = db.Merge()
		if err != nil {
			b.Fatalf("Failed to merge: %v", err)
		}

		b.StopTimer()
		mergeTime := time.Since(start)
		b.ReportMetric(float64(mergeTime.Nanoseconds())/1e6, "ms")
		b.StartTimer()

		err = db.Close()
		if err != nil {
			b.Fatalf("Failed to close database: %v", err)
		}
	}
}

// BenchmarkHintFileLoad benchmarks loading hint files during database startup
func BenchmarkHintFileLoad(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "hintfile-bench-load")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultOptions
	opts.Dir = tmpDir
	opts.SegmentSize = 64 * KB
	opts.EnableHintFile = true

	// Create database with hint files
	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}

	bucket := "bucket"
	txCreateBucket(&testing.T{}, db, DataStructureBTree, bucket, nil)

	// Add enough data to create multiple files
	for i := 0; i < 10000; i++ {
		txPut(&testing.T{}, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
	}

	// Perform merge to create hint files
	err = db.Merge()
	if err != nil {
		b.Fatalf("Failed to merge: %v", err)
	}

	err = db.Close()
	if err != nil {
		b.Fatalf("Failed to close database: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db, err := Open(opts)
		if err != nil {
			b.Fatalf("Failed to open database: %v", err)
		}

		// Just open and close to measure hint file loading time
		err = db.Close()
		if err != nil {
			b.Fatalf("Failed to close database: %v", err)
		}
	}
}
