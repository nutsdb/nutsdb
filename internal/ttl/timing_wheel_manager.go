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
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RussellLuo/timingwheel"
	"github.com/nutsdb/nutsdb/internal/core"
)

// keyMetadata represents minimal metadata for a key in the timing wheel.
// Designed to minimize memory overhead while providing necessary information
// for expiration event generation.
type keyMetadata struct {
	bucketId  uint64 // bucket identifier
	keyHash   uint64 // hash of the key for identification
	timestamp uint64 // original timestamp for verification
	ds        uint16 // data structure type
}

// keyReference wraps metadata with the actual key for temporary storage
// during registration and expiration event generation.
type keyReference struct {
	metadata keyMetadata
	key      []byte // actual key bytes, stored temporarily
}

// newKeyMetadata creates a new keyMetadata instance from the provided parameters.
func newKeyMetadata(bucketId uint64, key []byte, ds uint16, timestamp uint64) keyMetadata {
	return keyMetadata{
		bucketId:  bucketId,
		keyHash:   hashKey(key),
		timestamp: timestamp,
		ds:        ds,
	}
}

// newKeyReference creates a new keyReference instance with both metadata and key.
func newKeyReference(bucketId uint64, key []byte, ds uint16, timestamp uint64) keyReference {
	return keyReference{
		metadata: newKeyMetadata(bucketId, key, ds, timestamp),
		key:      key,
	}
}

// hashKey computes a 64-bit hash of the key using FNV-1a algorithm.
// FNV-1a is chosen for its simplicity and good distribution properties.
func hashKey(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	return h.Sum64()
}

// makeRegistryKey creates a unique string key for the timer registry
// by combining bucket ID and key hash.
func makeRegistryKey(bucketId uint64, keyHash uint64) string {
	// Pre-allocate buffer for efficiency
	buf := make([]byte, 0, 64)
	buf = appendUint64(buf, bucketId)
	buf = append(buf, ':')
	buf = appendUint64(buf, keyHash)
	return string(buf)
}

// appendUint64 appends the string representation of a uint64 to a byte slice.
// This avoids allocations from fmt.Sprintf.
func appendUint64(buf []byte, n uint64) []byte {
	if n == 0 {
		return append(buf, '0')
	}

	var tmp [20]byte // max uint64 is 20 digits
	i := len(tmp)
	for n > 0 {
		i--
		tmp[i] = byte('0' + n%10)
		n /= 10
	}
	return append(buf, tmp[i:]...)
}

// timerRegistry tracks active timers for deregistration support.
// It maintains a mapping from keys to their scheduled timers, allowing
// cancellation when keys are deleted or updated before expiration.
// Uses sync.Map for better concurrent performance.
type timerRegistry struct {
	timers sync.Map // map[string]*timingwheel.Timer
}

// newTimerRegistry creates a new timer registry instance.
func newTimerRegistry() *timerRegistry {
	return &timerRegistry{}
}

// register stores a timer in the registry for later cancellation.
// The timer is indexed by a unique key derived from bucketId and keyHash.
func (r *timerRegistry) register(bucketId uint64, keyHash uint64, timer *timingwheel.Timer) {
	key := makeRegistryKey(bucketId, keyHash)
	r.timers.Store(key, timer)
}

// deregister cancels and removes a timer from the registry.
// Returns true if the timer was found and cancelled, false otherwise.
func (r *timerRegistry) deregister(bucketId uint64, keyHash uint64) bool {
	key := makeRegistryKey(bucketId, keyHash)
	if value, exists := r.timers.LoadAndDelete(key); exists {
		timer := value.(*timingwheel.Timer)
		timer.Stop()
		return true
	}
	return false
}

// clear cancels all timers and clears the registry.
// Used during shutdown to clean up all pending timers.
func (r *timerRegistry) clear() {
	r.timers.Range(func(key, value interface{}) bool {
		timer := value.(*timingwheel.Timer)
		timer.Stop()
		r.timers.Delete(key)
		return true
	})
}

// TimingWheelManager manages active TTL expiration using a timing wheel.
// It wraps the RussellLuo/timingwheel library and integrates it with nutsdb's
// TTL infrastructure, providing key registration, deregistration, and expiration
// event generation.
type TimingWheelManager struct {
	wheel        *timingwheel.TimingWheel
	queue        *expirationQueue
	enabled      bool
	slotDuration time.Duration
	wheelSize    int64 // number of slots in the wheel

	// Key cache for reconstruction during expiration
	keyCache sync.Map // map[uint64][]byte (keyHash -> key)

	// Timer registry for deregistration support
	registry *timerRegistry

	// Retry buffer for failed queue pushes
	retryBuffer  chan *ExpirationEvent
	maxRetrySize int

	// Metrics
	registeredKeys   atomic.Int64
	triggeredDeletes atomic.Int64
	failedPushes     atomic.Int64

	// Lifecycle management
	lifecycle core.ComponentLifecycle
}

// TimingWheelMetrics contains metrics for timing wheel operations.
type TimingWheelMetrics struct {
	RegisteredKeys   int64 // Total keys currently registered
	TriggeredDeletes int64 // Total deletions triggered by the wheel
	FailedPushes     int64 // Failed pushes to expiration queue
}

// NewTimingWheelManager creates a new timing wheel manager.
// If the timing wheel is disabled in config, it returns a manager with enabled=false
// that will no-op on all operations.
func NewTimingWheelManager(config Config, queue *expirationQueue) *TimingWheelManager {
	config.Validate()

	mgr := &TimingWheelManager{
		queue:        queue,
		enabled:      config.EnableTimingWheel,
		slotDuration: config.WheelSlotDuration,
		wheelSize:    int64(config.WheelSize),
		registry:     newTimerRegistry(),
		maxRetrySize: max(1, config.QueueSize/10), // At least 1, or 10% of queue size
	}

	if config.EnableTimingWheel {
		mgr.wheel = timingwheel.NewTimingWheel(
			config.WheelSlotDuration,
			int64(config.WheelSize),
		)

		mgr.retryBuffer = make(chan *ExpirationEvent, mgr.maxRetrySize)
	}

	return mgr
}

// Name returns the component name for lifecycle management.
func (m *TimingWheelManager) Name() string {
	return "TimingWheelManager"
}

// Start starts the timing wheel ticker.
// If the timing wheel is disabled, this is a no-op.
// Returns an error if the component has already been started or stopped.
func (m *TimingWheelManager) Start(ctx context.Context) error {
	if !m.enabled || m.wheel == nil {
		return nil
	}

	if err := m.lifecycle.Start(ctx); err != nil {
		return err
	}

	m.wheel.Start()

	return nil
}

// Stop stops the timing wheel immediately without processing remaining slots.
// This method does not wait for pending events to be pushed to the queue.
// It is idempotent - calling Stop() multiple times is safe.
func (m *TimingWheelManager) Stop(timeout time.Duration) error {
	if !m.enabled || m.wheel == nil {
		return nil
	}

	// Check if already stopped to avoid calling wheel.Stop() multiple times
	if m.lifecycle.IsStopped() {
		return nil
	}

	m.wheel.Stop()

	m.registry.clear()

	return m.lifecycle.Stop(timeout)
}

// RegisterKey registers a key for active expiration in the timing wheel.
// Returns true if registered successfully, false if wheel is disabled or closed.
//
// The method calculates the appropriate slot position based on the key's TTL
// and schedules a timer callback for expiration. For TTLs exceeding the wheel's
// coverage, it uses lap counters to handle multiple wheel rotations.
func (m *TimingWheelManager) RegisterKey(
	bucketId uint64,
	key []byte,
	ds uint16,
	ttl uint32,
	timestamp uint64,
) bool {
	if !m.enabled || m.wheel == nil {
		return false
	}

	keyRef := newKeyReference(bucketId, key, ds, timestamp)
	keyHash := keyRef.metadata.keyHash

	// Make a copy to avoid holding reference to caller's slice
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	m.keyCache.Store(keyHash, keyCopy)

	expirationDuration := time.Duration(ttl) * time.Second

	wheelCoverage := m.slotDuration * time.Duration(m.wheelSize)

	var lapCounter uint32
	if expirationDuration > wheelCoverage {
		lapCounter = uint32(expirationDuration / wheelCoverage)
		expirationDuration = expirationDuration % wheelCoverage
		if expirationDuration == 0 {
			// If exactly divisible, we need one less lap and full wheel duration
			lapCounter--
			expirationDuration = wheelCoverage
		}
	}

	timer := m.wheel.AfterFunc(expirationDuration, func() {
		m.onKeyExpired(keyRef.metadata, lapCounter)
	})

	m.registry.register(bucketId, keyHash, timer)

	m.registeredKeys.Add(1)

	return true
}

// DeregisterKey removes a key from the timing wheel.
// This should be called when a key is deleted before expiration or when its TTL is updated.
// Returns true if the key was found and deregistered, false otherwise.
func (m *TimingWheelManager) DeregisterKey(bucketId uint64, key []byte) bool {
	if !m.enabled || m.wheel == nil {
		return false
	}

	keyHash := hashKey(key)

	if !m.registry.deregister(bucketId, keyHash) {
		return false
	}

	m.keyCache.Delete(keyHash)

	m.registeredKeys.Add(-1)

	return true
}

// processRetries attempts to drain the retry buffer and push failed events
// to the main expiration queue. This should be called before processing new
// slot expirations to prioritize retried events.
//
// The method drains events from the retry buffer and attempts to push them
// to the main queue. If a push fails (queue still full), the event is put
// back in the retry buffer. Processing stops if the retry buffer is full
// to avoid blocking.
func (m *TimingWheelManager) processRetries() {
	if !m.enabled || m.retryBuffer == nil {
		return
	}

	for {
		select {
		case event := <-m.retryBuffer:
			if !m.queue.push(event) {
				select {
				case m.retryBuffer <- event:
					return
				default:
					// Retry buffer is full, stop processing; event is dropped and lazy deletion will clean up
					return
				}
			} else {
				m.triggeredDeletes.Add(1)
			}
		default:
			return
		}
	}
}

// onKeyExpired is the callback invoked when a key's timer expires.
// It handles lap counter decrements for multi-lap TTLs and pushes expiration events to the queue.
func (m *TimingWheelManager) onKeyExpired(metadata keyMetadata, lapCounter uint32) {
	// Process retries before handling new expirations to prioritize them
	m.processRetries()

	if lapCounter > 0 {
		_, exists := m.keyCache.Load(metadata.keyHash)
		if !exists {
			return
		}

		wheelCoverage := m.slotDuration * time.Duration(m.wheelSize)

		timer := m.wheel.AfterFunc(wheelCoverage, func() {
			m.onKeyExpired(metadata, lapCounter-1)
		})

		m.registry.register(metadata.bucketId, metadata.keyHash, timer)
		return
	}

	keyInterface, exists := m.keyCache.Load(metadata.keyHash)
	if !exists {
		return
	}
	key := keyInterface.([]byte)

	event := &ExpirationEvent{
		BucketId:  metadata.bucketId,
		Key:       key,
		Ds:        metadata.ds,
		Timestamp: metadata.timestamp,
	}

	if !m.queue.push(event) {
		m.failedPushes.Add(1)
		select {
		case m.retryBuffer <- event:
		default:
			// Retry buffer also full; event is dropped and lazy deletion will handle it
		}
	} else {
		m.triggeredDeletes.Add(1)
	}

	m.keyCache.Delete(metadata.keyHash)
	m.registry.deregister(metadata.bucketId, metadata.keyHash)
	m.registeredKeys.Add(-1)
}

// GetMetrics returns current timing wheel metrics.
// This provides visibility into the timing wheel's operations for monitoring
// and troubleshooting purposes.
func (m *TimingWheelManager) GetMetrics() TimingWheelMetrics {
	return TimingWheelMetrics{
		RegisteredKeys:   m.registeredKeys.Load(),
		TriggeredDeletes: m.triggeredDeletes.Load(),
		FailedPushes:     m.failedPushes.Load(),
	}
}
