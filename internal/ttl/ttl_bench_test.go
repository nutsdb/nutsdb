// Copyright 2025 The nutsdb Author. All rights reserved.
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

package ttl

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
)

type ttlBenchScenario struct {
	name        string
	keyCount    int
	ttlSeconds  uint32
	enableWheel bool
	queueSize   int
	batchSize   int
}

func runTTLScenario(b *testing.B, sc ttlBenchScenario) {
	b.Helper()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		clock := NewMockClock(1000000)
		config := DefaultConfig()
		config.EnableTimingWheel = sc.enableWheel
		config.QueueSize = sc.queueSize
		config.BatchSize = sc.batchSize
		config.BatchTimeout = time.Hour
		config.WheelSlotDuration = 100 * time.Millisecond
		config.WheelSize = 60

		service := NewService(clock, config, func([]*ExpirationEvent) {})
		if err := service.Start(context.Background()); err != nil {
			b.Fatalf("start service: %v", err)
		}

		bucketId := uint64(1)
		ds := uint16(1)
		timestamp := uint64(clock.NowMillis())

		keys := make([][]byte, sc.keyCount)
		for idx := 0; idx < sc.keyCount; idx++ {
			keys[idx] = []byte(fmt.Sprintf("key_%d", idx))
		}

		clock.AdvanceTime(time.Duration(sc.ttlSeconds+1) * time.Second)

		var memBefore, memAfter runtime.MemStats
		runtime.ReadMemStats(&memBefore)

		b.StartTimer()
		start := time.Now()

		if sc.enableWheel {
			for _, key := range keys {
				service.RegisterKeyForActiveExpiration(bucketId, key, ds, sc.ttlSeconds, timestamp)
			}
			for _, key := range keys {
				service.wheelManager.onKeyExpired(newKeyMetadata(bucketId, key, ds, timestamp), 0)
			}
		} else {
			record := &core.Record{TTL: sc.ttlSeconds, Timestamp: timestamp}
			checker := service.GetChecker()
			for _, key := range keys {
				record.Key = key
				checker.FilterExpiredRecord(bucketId, key, record, ds)
			}
		}

		elapsed := time.Since(start)
		b.StopTimer()

		runtime.ReadMemStats(&memAfter)

		if err := service.Stop(1 * time.Second); err != nil {
			b.Fatalf("stop service: %v", err)
		}

		allocDiff := int64(memAfter.Alloc) - int64(memBefore.Alloc)
		if allocDiff < 0 {
			allocDiff = 0
		}
		allocMB := float64(allocDiff) / 1024 / 1024
		b.ReportMetric(allocMB, "MB/op")
		if elapsed > 0 {
			b.ReportMetric(float64(sc.keyCount)/elapsed.Seconds(), "keys/sec")
		}
	}
}

func BenchmarkTTL_KeyCounts(b *testing.B) {
	scenarios := []ttlBenchScenario{
		{name: "1K_keys", keyCount: 1000, ttlSeconds: 1, enableWheel: true, queueSize: 1000, batchSize: 100},
		{name: "10K_keys", keyCount: 10000, ttlSeconds: 10, enableWheel: true, queueSize: 10000, batchSize: 100},
		{name: "100K_keys", keyCount: 100000, ttlSeconds: 60, enableWheel: true, queueSize: 10000, batchSize: 1000},
		{name: "1M_keys", keyCount: 1000000, ttlSeconds: 300, enableWheel: true, queueSize: 10000, batchSize: 1000},
	}

	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			runTTLScenario(b, sc)
		})
	}
}

func BenchmarkTTL_Durations(b *testing.B) {
	scenarios := []ttlBenchScenario{
		{name: "TTL_1s", keyCount: 10000, ttlSeconds: 1, enableWheel: true, queueSize: 1000, batchSize: 100},
		{name: "TTL_10s", keyCount: 10000, ttlSeconds: 10, enableWheel: true, queueSize: 1000, batchSize: 100},
		{name: "TTL_60s", keyCount: 10000, ttlSeconds: 60, enableWheel: true, queueSize: 1000, batchSize: 100},
		{name: "TTL_300s", keyCount: 10000, ttlSeconds: 300, enableWheel: true, queueSize: 1000, batchSize: 100},
	}

	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			runTTLScenario(b, sc)
		})
	}
}

func BenchmarkTTL_TimingWheelEnabled(b *testing.B) {
	sc := ttlBenchScenario{
		name:        "TimingWheelEnabled",
		keyCount:    10000,
		ttlSeconds:  10,
		enableWheel: true,
		queueSize:   1000,
		batchSize:   100,
	}
	runTTLScenario(b, sc)
}

func BenchmarkTTL_TimingWheelDisabled(b *testing.B) {
	sc := ttlBenchScenario{
		name:        "TimingWheelDisabled",
		keyCount:    10000,
		ttlSeconds:  10,
		enableWheel: false,
		queueSize:   1000,
		batchSize:   100,
	}
	runTTLScenario(b, sc)
}

func BenchmarkTTL_QueueSizeImpact(b *testing.B) {
	scenarios := []ttlBenchScenario{
		{name: "Queue_100", keyCount: 10000, ttlSeconds: 10, enableWheel: true, queueSize: 100, batchSize: 100},
		{name: "Queue_1000", keyCount: 10000, ttlSeconds: 10, enableWheel: true, queueSize: 1000, batchSize: 100},
		{name: "Queue_10000", keyCount: 10000, ttlSeconds: 10, enableWheel: true, queueSize: 10000, batchSize: 100},
	}

	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			runTTLScenario(b, sc)
		})
	}
}

func BenchmarkTTL_BatchSizeImpact(b *testing.B) {
	scenarios := []ttlBenchScenario{
		{name: "Batch_10", keyCount: 10000, ttlSeconds: 10, enableWheel: true, queueSize: 1000, batchSize: 10},
		{name: "Batch_100", keyCount: 10000, ttlSeconds: 10, enableWheel: true, queueSize: 1000, batchSize: 100},
		{name: "Batch_1000", keyCount: 10000, ttlSeconds: 10, enableWheel: true, queueSize: 1000, batchSize: 1000},
	}

	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			runTTLScenario(b, sc)
		})
	}
}

func BenchmarkTTL_WheelSlotDuration(b *testing.B) {
	scenarios := []struct {
		name         string
		slotDuration time.Duration
		wheelSize    int
	}{
		{name: "SlotDuration_100ms", slotDuration: 100 * time.Millisecond, wheelSize: 600},
		{name: "SlotDuration_1s", slotDuration: 1 * time.Second, wheelSize: 3600},
		{name: "SlotDuration_5s", slotDuration: 5 * time.Second, wheelSize: 720},
	}

	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				clock := NewMockClock(1000000)
				config := DefaultConfig()
				config.EnableTimingWheel = true
				config.WheelSlotDuration = sc.slotDuration
				config.WheelSize = sc.wheelSize
				config.QueueSize = 1000
				config.BatchSize = 100
				config.BatchTimeout = time.Hour

				service := NewService(clock, config, func([]*ExpirationEvent) {})
				if err := service.Start(context.Background()); err != nil {
					b.Fatalf("start service: %v", err)
				}

				bucketId := uint64(1)
				ds := uint16(1)
				timestamp := uint64(clock.NowMillis())
				keyCount := 10000
				ttlSeconds := uint32(10)

				keys := make([][]byte, keyCount)
				for idx := 0; idx < keyCount; idx++ {
					keys[idx] = []byte(fmt.Sprintf("key_%d", idx))
				}

				clock.AdvanceTime(time.Duration(ttlSeconds+1) * time.Second)

				var memBefore, memAfter runtime.MemStats
				runtime.ReadMemStats(&memBefore)

				b.StartTimer()
				start := time.Now()

				for _, key := range keys {
					service.RegisterKeyForActiveExpiration(bucketId, key, ds, ttlSeconds, timestamp)
				}
				for _, key := range keys {
					service.wheelManager.onKeyExpired(newKeyMetadata(bucketId, key, ds, timestamp), 0)
				}

				elapsed := time.Since(start)
				b.StopTimer()

				runtime.ReadMemStats(&memAfter)

				if err := service.Stop(1 * time.Second); err != nil {
					b.Fatalf("stop service: %v", err)
				}

				allocDiff := int64(memAfter.Alloc) - int64(memBefore.Alloc)
				if allocDiff < 0 {
					allocDiff = 0
				}
				allocMB := float64(allocDiff) / 1024 / 1024
				b.ReportMetric(allocMB, "MB/op")
				if elapsed > 0 {
					b.ReportMetric(float64(keyCount)/elapsed.Seconds(), "keys/sec")
				}
			}
		})
	}
}

func BenchmarkTTL_WheelSize(b *testing.B) {
	scenarios := []struct {
		name         string
		wheelSize    int
		slotDuration time.Duration
	}{
		{name: "WheelSize_360", wheelSize: 360, slotDuration: 1 * time.Second},
		{name: "WheelSize_3600", wheelSize: 3600, slotDuration: 1 * time.Second},
		{name: "WheelSize_36000", wheelSize: 36000, slotDuration: 1 * time.Second},
	}

	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				clock := NewMockClock(1000000)
				config := DefaultConfig()
				config.EnableTimingWheel = true
				config.WheelSlotDuration = sc.slotDuration
				config.WheelSize = sc.wheelSize
				config.QueueSize = 1000
				config.BatchSize = 100
				config.BatchTimeout = time.Hour

				service := NewService(clock, config, func([]*ExpirationEvent) {})
				if err := service.Start(context.Background()); err != nil {
					b.Fatalf("start service: %v", err)
				}

				bucketId := uint64(1)
				ds := uint16(1)
				timestamp := uint64(clock.NowMillis())
				keyCount := 10000
				ttlSeconds := uint32(10)

				keys := make([][]byte, keyCount)
				for idx := 0; idx < keyCount; idx++ {
					keys[idx] = []byte(fmt.Sprintf("key_%d", idx))
				}

				clock.AdvanceTime(time.Duration(ttlSeconds+1) * time.Second)

				var memBefore, memAfter runtime.MemStats
				runtime.ReadMemStats(&memBefore)

				b.StartTimer()
				start := time.Now()

				for _, key := range keys {
					service.RegisterKeyForActiveExpiration(bucketId, key, ds, ttlSeconds, timestamp)
				}
				for _, key := range keys {
					service.wheelManager.onKeyExpired(newKeyMetadata(bucketId, key, ds, timestamp), 0)
				}

				elapsed := time.Since(start)
				b.StopTimer()

				runtime.ReadMemStats(&memAfter)

				if err := service.Stop(1 * time.Second); err != nil {
					b.Fatalf("stop service: %v", err)
				}

				allocDiff := int64(memAfter.Alloc) - int64(memBefore.Alloc)
				if allocDiff < 0 {
					allocDiff = 0
				}
				allocMB := float64(allocDiff) / 1024 / 1024
				b.ReportMetric(allocMB, "MB/op")
				if elapsed > 0 {
					b.ReportMetric(float64(keyCount)/elapsed.Seconds(), "keys/sec")
				}
			}
		})
	}
}

func BenchmarkTTL_WheelConfigForDifferentTTLRanges(b *testing.B) {
	scenarios := []struct {
		name         string
		ttlSeconds   uint32
		slotDuration time.Duration
		wheelSize    int
	}{
		{name: "ShortTTL_HighPrecision", ttlSeconds: 5, slotDuration: 100 * time.Millisecond, wheelSize: 600},
		{name: "ShortTTL_LowPrecision", ttlSeconds: 5, slotDuration: 1 * time.Second, wheelSize: 60},
		{name: "MediumTTL_HighPrecision", ttlSeconds: 60, slotDuration: 1 * time.Second, wheelSize: 3600},
		{name: "MediumTTL_LowPrecision", ttlSeconds: 60, slotDuration: 5 * time.Second, wheelSize: 720},
		{name: "LongTTL_HighPrecision", ttlSeconds: 600, slotDuration: 1 * time.Second, wheelSize: 3600},
		{name: "LongTTL_LowPrecision", ttlSeconds: 600, slotDuration: 10 * time.Second, wheelSize: 360},
	}

	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				clock := NewMockClock(1000000)
				config := DefaultConfig()
				config.EnableTimingWheel = true
				config.WheelSlotDuration = sc.slotDuration
				config.WheelSize = sc.wheelSize
				config.QueueSize = 1000
				config.BatchSize = 100
				config.BatchTimeout = time.Hour

				service := NewService(clock, config, func([]*ExpirationEvent) {})
				if err := service.Start(context.Background()); err != nil {
					b.Fatalf("start service: %v", err)
				}

				bucketId := uint64(1)
				ds := uint16(1)
				timestamp := uint64(clock.NowMillis())
				keyCount := 10000

				keys := make([][]byte, keyCount)
				for idx := 0; idx < keyCount; idx++ {
					keys[idx] = []byte(fmt.Sprintf("key_%d", idx))
				}

				clock.AdvanceTime(time.Duration(sc.ttlSeconds+1) * time.Second)

				var memBefore, memAfter runtime.MemStats
				runtime.ReadMemStats(&memBefore)

				b.StartTimer()
				start := time.Now()

				for _, key := range keys {
					service.RegisterKeyForActiveExpiration(bucketId, key, ds, sc.ttlSeconds, timestamp)
				}
				for _, key := range keys {
					service.wheelManager.onKeyExpired(newKeyMetadata(bucketId, key, ds, timestamp), 0)
				}

				elapsed := time.Since(start)
				b.StopTimer()

				runtime.ReadMemStats(&memAfter)

				if err := service.Stop(1 * time.Second); err != nil {
					b.Fatalf("stop service: %v", err)
				}

				allocDiff := int64(memAfter.Alloc) - int64(memBefore.Alloc)
				if allocDiff < 0 {
					allocDiff = 0
				}
				allocMB := float64(allocDiff) / 1024 / 1024
				b.ReportMetric(allocMB, "MB/op")
				if elapsed > 0 {
					b.ReportMetric(float64(keyCount)/elapsed.Seconds(), "keys/sec")
				}
			}
		})
	}
}
