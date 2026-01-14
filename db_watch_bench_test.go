package nutsdb

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	bucketName = "test_bucket"
)

func setupBenchmarkDBWatch(b *testing.B, dir string, incrBufferSizes bool) *DB {
	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 128 * 1024 * 1024 // 128MB
	opts.SyncEnable = false              // Disable sync for better performance
	opts.EnableWatch = true

	// modify the buffer size for benchmark
	if incrBufferSizes {
		watchChanBufferSize = 1024 * 1024
		receiveChanBufferSize = 1024 * 1024
		maxBatchSize = 1024 * 1024
		deadMessageThreshold = 100 * 1024
		distributeChanBufferSize = 128 * 1024
		victimBucketBufferSize = 128 * 1024
	}

	// Clean up directory
	_ = os.RemoveAll(dir)
	_ = os.MkdirAll(dir, 0755)

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}

	// Create test bucket
	err = db.Update(func(tx *Tx) error {
		return tx.NewKVBucket(bucketName)
	})
	if err != nil {
		b.Fatalf("Failed to create bucket: %v", err)
	}

	return db
}

func cleanupBenchmarkDBWatch(db *DB, dir string) {
	if db != nil && !db.IsClose() {
		_ = db.Close()
	}
	_ = os.RemoveAll(dir)
}

// createValue creates a value of the specified size
// If size is 0, creates a small default value
func createValue(size int) []byte {
	if size <= 0 {
		return []byte("value")
	}

	value := make([]byte, size)

	for i := 0; i < size; i++ {
		value[i] = byte('A' + (i % 26))
	}

	return value
}

func BenchmarkWatcherManager(b *testing.B) {
	// DISABLE GLOBAL LOGGING
	log.SetOutput(io.Discard)

	scenarios := []struct {
		name            string
		cntOfKeys       int
		numWatchers     int
		updateFreq      int  // updates per iteration
		incrBufferSizes bool // increase the buffer sizes for benchmark
		messageSize     int  // message size in bytes
	}{
		// Baseline
		{"single_key_single_watcher_low_freq", 1, 1, 1, true, 0},
		{"medium_scale_balanced", 10, 10, 5, true, 0},

		// Throughput
		{"single_key_single_watcher_high_freq", 1, 1, 10, true, 0},
		{"single_key_extreme_freq", 1, 1, 1000, true, 0},
		{"multi_key_very_high_freq", 10, 5, 100, true, 0},

		// Scalability
		{"single_key_multi_watcher_low_freq", 1, 50, 1, true, 0}, // Fan-Out
		{"hot_keys", 5, 50, 10, true, 0},                         // Asymmetric
		{"multi_key_multi_watcher", 50, 10, 1, true, 0},

		// DataVolume
		{"large_messages_1kb", 1, 1, 10, true, 1024},
		{"large_messages_10kb", 1, 1, 10, true, 10240},

		// Real World Scenarios
		{"stress_default_buffers", 10, 10, 10, false, 0}, // Small buffers
		{"no_subscribers_baseline", 10, 0, 10, false, 0}, // Filtering cost
	}

	for _, s := range scenarios {
		b.Run(s.name, func(b *testing.B) {
			dir := b.TempDir()
			ctx, cancel := context.WithCancel(context.Background())
			db := setupBenchmarkDBWatch(b, dir, s.incrBufferSizes)
			timesOfbenchmark := b.N

			defer func() {
				cancel()
				cleanupBenchmarkDBWatch(db, dir)
			}()

			expectedTotal := int64(timesOfbenchmark * s.cntOfKeys * s.updateFreq)
			b.Logf("Expected %d total", expectedTotal)
			done := make(chan struct{})
			var once sync.Once // Ensure done is closed only once

			keys := make([][]byte, s.cntOfKeys)
			watchers := make([]*Watcher, 0, s.cntOfKeys*s.numWatchers)

			for i := 0; i < s.cntOfKeys; i++ {
				keys[i] = fmt.Appendf(nil, "key_%d", i)
				for j := 0; j < s.numWatchers; j++ {
					watcher, err := db.Watch(ctx, bucketName, keys[i], func(msg *Message) error {
						return nil
					}, WatchOptions{
						CallbackTimeout: 60 * time.Second,
					})
					if err != nil {
						b.Fatalf("Failed to watch: %v", err)
					}

					watchers = append(watchers, watcher)

				}
			}

			b.ResetTimer()
			b.ReportAllocs()

			for _, watcher := range watchers {
				go func(w *Watcher) {
					err := w.Run()
					if err != nil {
						b.Errorf("Failed to run watcher: %v", err)
					}
				}(watcher)

				errWait := watcher.WaitReady(10 * time.Second)
				if errWait != nil {
					b.Fatalf("Failed to wait for watcher: %v", errWait)
				}
			}

			go func() {
				ticker := time.NewTicker(100 * time.Millisecond)
				for {
					select {
					case <-ticker.C:
					default:
						totalReceived := db.watchMgr.stats.GetTotalCount()
						if totalReceived == expectedTotal {
							once.Do(func() {
								close(done)
							})
							return
						}
					}
				}
			}()

			for i := 0; i < timesOfbenchmark; i++ {
				for j := 0; j < s.cntOfKeys; j++ {
					for k := 0; k < s.updateFreq; k++ {
						err := db.Update(func(tx *Tx) error {
							value := createValue(s.messageSize)
							return tx.Put(bucketName, keys[j], value, Persistent)
						})
						if err != nil {
							b.Fatalf("Put failed: %v", err)
						}
					}
				}
			}

			<-done

			// wait for process all messages
			time.Sleep(200 * time.Millisecond)

			b.StopTimer()

		})
	}
}
