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

package ttl

import (
	"time"

	"github.com/antlabs/timer"
)

// BucketId represents the bucket identifier type
type BucketId = uint64

// ExpiredDeleteType represents the type of expired deletion strategy
type ExpiredDeleteType int

const (
	TimeWheel ExpiredDeleteType = iota
	TimeHeap
)

type nodesInBucket map[string]timer.TimeNoder // key to timer node

func newNodesInBucket() nodesInBucket {
	return make(map[string]timer.TimeNoder)
}

type nodes map[BucketId]nodesInBucket // bucket to nodes that in a bucket

func (n nodes) getNode(bucketId BucketId, key string) (timer.TimeNoder, bool) {
	nib, ok := n[bucketId]
	if !ok {
		return nil, false
	}
	node, ok := nib[key]
	return node, ok
}

func (n nodes) addNode(bucketId BucketId, key string, node timer.TimeNoder) {
	nib, ok := n[bucketId]
	if !ok {
		nib = newNodesInBucket()
		n[bucketId] = nib
	}
	nib[key] = node
}

func (n nodes) delNode(bucketId BucketId, key string) {
	if nib, ok := n[bucketId]; ok {
		delete(nib, key)
	}
}

// Manager defines the interface for TTL management
type Manager interface {
	Run()
	Exist(bucketId BucketId, key string) bool
	Add(bucketId BucketId, key string, expire time.Duration, ds uint16, timestamp uint64, callback ExpireCallback)
	Del(bucketId BucketId, key string)
	Close()
}

// TimerManager handles TTL management using timer-based expiration
type TimerManager struct {
	t          timer.Timer
	timerNodes nodes
}

// NewTimerManager creates a new TTL manager with the specified expiration deletion type
func NewTimerManager(expiredDeleteType ExpiredDeleteType) *TimerManager {
	var t timer.Timer

	switch expiredDeleteType {
	case TimeWheel:
		t = timer.NewTimer(timer.WithTimeWheel())
	case TimeHeap:
		t = timer.NewTimer(timer.WithMinHeap())
	default:
		t = timer.NewTimer()
	}

	return &TimerManager{
		t:          t,
		timerNodes: make(nodes),
	}
}

// Run starts the TTL manager
func (tm *TimerManager) Run() {
	tm.t.Run()
}

// Exist checks if a TTL entry exists for the given bucket and key
func (tm *TimerManager) Exist(bucketId BucketId, key string) bool {
	_, ok := tm.timerNodes.getNode(bucketId, key)
	return ok
}

// ExpireCallback is a function type for timer expiration notifications.
// timestamp is the record timestamp when the TTL was set, used for validation.
type ExpireCallback func(bucketId BucketId, key []byte, ds uint16, timestamp uint64)

// Add adds a TTL entry with the specified expiration duration.
// When the timer expires, it triggers the callback with bucketId, key, data structure type, and timestamp.
func (tm *TimerManager) Add(bucketId BucketId, key string, expire time.Duration, ds uint16, timestamp uint64, callback ExpireCallback) {
	if node, ok := tm.timerNodes.getNode(bucketId, key); ok {
		node.Stop()
	}

	node := tm.t.AfterFunc(expire, func() {
		if callback != nil {
			callback(bucketId, []byte(key), ds, timestamp)
		}
	})
	tm.timerNodes.addNode(bucketId, key, node)
}

// Del removes a TTL entry for the given bucket and key
func (tm *TimerManager) Del(bucket BucketId, key string) {
	tm.timerNodes.delNode(bucket, key)
}

// Close closes the TTL manager and stops all timers
func (tm *TimerManager) Close() {
	tm.timerNodes = nil
	tm.t.Stop()
}
