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
	"github.com/antlabs/timer"
	"time"
)

type ttlManager struct {
	t          timer.Timer
	timerNodes map[string]map[string]timer.TimeNoder
}

func newTTLManager(expiredDeleteType ExpiredDeleteType) *ttlManager {
	var t timer.Timer

	switch expiredDeleteType {
	case TimeWheel:
		t = timer.NewTimer(timer.WithTimeWheel())
	case TimeHeap:
		t = timer.NewTimer(timer.WithMinHeap())
	default:
		t = timer.NewTimer()
	}

	return &ttlManager{
		t:          t,
		timerNodes: make(map[string]map[string]timer.TimeNoder),
	}
}

func (tm *ttlManager) run() {
	tm.t.Run()
}

func (tm *ttlManager) exist(bucket, key string) bool {
	if nodes, ok := tm.timerNodes[bucket]; ok {
		if _, ok := nodes[key]; ok {
			return true
		}
	}
	return false
}

func (tm *ttlManager) add(bucket, key string, expire time.Duration, callback func()) {
	nodes, ok := tm.timerNodes[bucket]

	if !ok {
		tm.timerNodes[bucket] = make(map[string]timer.TimeNoder)
		nodes = tm.timerNodes[bucket]
	}

	if node, ok := nodes[key]; ok {
		node.Stop()
	}

	node := tm.t.AfterFunc(expire, callback)
	tm.timerNodes[bucket][key] = node
}

func (tm *ttlManager) del(bucket, key string) {
	if _, ok := tm.timerNodes[bucket]; !ok {
		return
	}

	if node, ok := tm.timerNodes[bucket][key]; ok {
		node.Stop()
		delete(tm.timerNodes[bucket], key)
	}
}

func (tm *ttlManager) close() {
	tm.timerNodes = nil
	tm.t.Stop()
}
