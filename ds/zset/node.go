// Copyright (c) 2016, Jerry.Wang. All rights reserved.
// Use of this source code is governed by a BSD 2-Clause
// license that can be found in the LICENSE file.

// Copyright 2019 The nutsdb Authors. All rights reserved.
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

package zset

// SortedSetLevel records forward and span.
type SortedSetLevel struct {
	forward *SortedSetNode
	span    int64
}

// SortedSetNode represents a node in the SortedSet.
type SortedSetNode struct {
	key      string // unique key of this node
	Value    []byte // associated data
	score    SCORE  // score to determine the order of this node in the set
	backward *SortedSetNode
	level    []SortedSetLevel
}

// Key returns the key of the node.
func (ssn *SortedSetNode) Key() string {
	return ssn.key
}

// Score returns the score of the node.
func (ssn *SortedSetNode) Score() SCORE {
	return ssn.score
}
