// Copyright (c) 2016, Jerry.Wang. All rights reserved.
// Use of this source code is governed by a BSD 2-Clause
// license that can be found in the LICENSE file.
// link: https://github.com/wangjia184/sortedset

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

import (
	"math/rand"
)

const (
	// SkipListMaxLevel represents the skipList max level number.
	SkipListMaxLevel = 32

	// SkipListP represents the p parameter of the skipList.
	SkipListP = 0.25
)

// SCORE represents the score type.
type SCORE float64

// SortedSet represents the sorted set.
type SortedSet struct {
	header *SortedSetNode
	tail   *SortedSetNode
	length int64
	level  int
	Dict   map[string]*SortedSetNode
}

// createNode returns a newly initialized SortedSetNode Object that implements the SortedSetNode.
func createNode(level int, score SCORE, key string, value []byte) *SortedSetNode {
	node := SortedSetNode{
		score: score,
		key:   key,
		Value: value,
		level: make([]SortedSetLevel, level),
	}
	return &node
}

// randomLevel returns a random level for the new skiplist node we are going to create.
// The return value of this function is between 1 and SkipListMaxLevel
// (both inclusive), with a powerlaw-alike distribution where higher
// levels are less likely to be returned.
func randomLevel() int {
	level := 1
	for float64(rand.Int31()&0xFFFF) < float64(SkipListP*0xFFFF) {
		level += 1
	}
	if level < SkipListMaxLevel {
		return level
	}

	return SkipListMaxLevel
}

func (ss *SortedSet) insertNode(score SCORE, key string, value []byte) *SortedSetNode {
	var update [SkipListMaxLevel]*SortedSetNode
	var rank [SkipListMaxLevel]int64

	x := ss.header
	for i := ss.level - 1; i >= 0; i-- {
		// store rank that is crossed to reach the insert position
		if ss.level-1 == i {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && // score is the same but the key is different
					x.level[i].forward.key < key)) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}

	/* we assume the key is not already inside, since we allow duplicated
	 * scores, and the re-insertion of score and redis object should never
	 * happen since the caller of Insert() should test in the hash table
	 * if the element is already inside or not. */
	level := randomLevel()

	if level > ss.level { // add a new level
		for i := ss.level; i < level; i++ {
			rank[i] = 0
			update[i] = ss.header
			update[i].level[i].span = ss.length
		}
		ss.level = level
	}

	x = createNode(level, score, key, value)
	for i := 0; i < level; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x

		/* update span covered by update[i] as x is inserted here */
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])

		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// increment span for untouched levels
	for i := level; i < ss.level; i++ {
		update[i].level[i].span++
	}

	if update[0] == ss.header {
		x.backward = nil
	} else {
		x.backward = update[0]
	}

	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		ss.tail = x
	}

	ss.length++

	return x
}

// deleteNode represents internal function used by delete, DeleteByScore and DeleteByRank.
func (ss *SortedSet) deleteNode(x *SortedSetNode, update [SkipListMaxLevel]*SortedSetNode) {
	for i := 0; i < ss.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span -= 1
		}
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		ss.tail = x.backward
	}
	for ss.level > 1 && ss.header.level[ss.level-1].forward == nil {
		ss.level--
	}
	ss.length--
	delete(ss.Dict, x.key)
}

// delete removes an element with matching score/key from the skiplist.
func (ss *SortedSet) delete(score SCORE, key string) bool {
	var update [SkipListMaxLevel]*SortedSetNode

	x := ss.header
	for i := ss.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score &&
					x.level[i].forward.key < key)) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	/* We may have multiple elements with the same score, what we need
	 * is to find the element with both the right score and object. */
	x = x.level[0].forward
	if x != nil && score == x.score && x.key == key {
		ss.deleteNode(x, update)
		// free x
		return true
	}
	return false /* not found */
}

// New returns a newly initialized SortedSet Object that implements the SortedSet.
func New() *SortedSet {
	sortedSet := SortedSet{
		level: 1,
		Dict:  make(map[string]*SortedSetNode),
	}
	sortedSet.header = createNode(SkipListMaxLevel, 0, "", nil)
	return &sortedSet
}

// Size returns the number of elements in the SortedSet.
func (ss *SortedSet) Size() int {
	return int(ss.length)
}

// PeekMin returns the element with minimum score, nil if the set is empty.
//
// Time complexity of this method is : O(log(N)).
func (ss *SortedSet) PeekMin() *SortedSetNode {
	return ss.header.level[0].forward
}

// PopMin returns and remove the element with minimal score, nil if the set is empty.
//
// Time complexity of this method is : O(log(N)).
func (ss *SortedSet) PopMin() *SortedSetNode {
	x := ss.header.level[0].forward
	if x != nil {
		ss.Remove(x.key)
	}
	return x
}

// PeekMax returns the element with maximum score, nil if the set is empty.
//
// Time Complexity : O(1).
func (ss *SortedSet) PeekMax() *SortedSetNode {
	return ss.tail
}

// PopMax returns and remove the element with maximum score, nil if the set is empty.
//
// Time complexity of this method is : O(log(N)).
func (ss *SortedSet) PopMax() *SortedSetNode {
	x := ss.tail
	if x != nil {
		ss.Remove(x.key)
	}
	return x
}

// Put puts an element into the sorted set with specific key / value / score.
//
// Time complexity of this method is : O(log(N)).
func (ss *SortedSet) Put(key string, score SCORE, value []byte) error {
	var newNode *SortedSetNode

	if n, ok := ss.Dict[key]; ok {
		// score does not change, only update value
		if n.score == score {
			n.Value = value
		} else { // score changes, delete and re-insert
			ss.delete(n.score, n.key)
			newNode = ss.insertNode(score, key, value)
		}
	} else {
		newNode = ss.insertNode(score, key, value)
	}

	if newNode != nil {
		ss.Dict[key] = newNode
	}

	return nil
}

// Remove removes element specified at given key.
//
// Time complexity of this method is : O(log(N)).
func (ss *SortedSet) Remove(key string) *SortedSetNode {
	found := ss.Dict[key]
	if found != nil {
		ss.delete(found.score, found.key)
		return found
	}
	return nil
}

// GetByScoreRangeOptions represents the options of the GetByScoreRange function.
type GetByScoreRangeOptions struct {
	Limit        int  // limit the max nodes to return
	ExcludeStart bool // exclude start value, so it search in interval (start, end] or (start, end)
	ExcludeEnd   bool // exclude end value, so it search in interval [start, end) or (start, end)
}

// GetByScoreRange returns the nodes whose score within the specific range.
// If options is nil, it searches in interval [start, end] without any limit by default.
//
// Time complexity of this method is : O(log(N)).
func (ss *SortedSet) GetByScoreRange(start SCORE, end SCORE, options *GetByScoreRangeOptions) []*SortedSetNode {
	limit := 1<<31 - 1
	if options != nil && options.Limit > 0 {
		limit = options.Limit
	}

	excludeStart := options != nil && options.ExcludeStart
	excludeEnd := options != nil && options.ExcludeEnd
	reverse := start > end
	if reverse {
		start, end = end, start
		excludeStart, excludeEnd = excludeEnd, excludeStart
	}

	var nodes []*SortedSetNode

	// determine if out of range
	if ss.length == 0 {
		return nodes
	}

	if reverse {
		// search from end to start
		return ss.searchReverse(nodes, excludeStart, excludeEnd, start, end, limit)
	}
	// search from start to end
	return ss.searchForward(nodes, excludeStart, excludeEnd, start, end, limit)
}

func (ss *SortedSet) searchForward(nodes []*SortedSetNode, excludeStart, excludeEnd bool, start, end SCORE, limit int) []*SortedSetNode {
	// search from start to end
	x := ss.header
	if excludeStart {
		for i := ss.level - 1; i >= 0; i-- {
			for x.level[i].forward != nil &&
				x.level[i].forward.score <= start {
				x = x.level[i].forward
			}
		}
	} else {
		for i := ss.level - 1; i >= 0; i-- {
			for x.level[i].forward != nil &&
				x.level[i].forward.score < start {
				x = x.level[i].forward
			}
		}
	}

	/* Current node is the last with score < or <= start. */
	x = x.level[0].forward

	for x != nil && limit > 0 {
		if excludeEnd {
			if x.score >= end {
				break
			}
		} else {
			if x.score > end {
				break
			}
		}

		next := x.level[0].forward

		nodes = append(nodes, x)
		limit--

		x = next
	}

	return nodes
}

func (ss *SortedSet) searchReverse(nodes []*SortedSetNode, excludeStart, excludeEnd bool, start, end SCORE, limit int) []*SortedSetNode {
	x := ss.header

	if excludeEnd {
		for i := ss.level - 1; i >= 0; i-- {
			for x.level[i].forward != nil &&
				x.level[i].forward.score < end {
				x = x.level[i].forward
			}
		}
	} else {
		for i := ss.level - 1; i >= 0; i-- {
			for x.level[i].forward != nil &&
				x.level[i].forward.score <= end {
				x = x.level[i].forward
			}
		}
	}

	for x != nil && limit > 0 {
		if excludeStart {
			if x.score <= start {
				break
			}
		} else {
			if x.score < start {
				break
			}
		}

		next := x.backward

		nodes = append(nodes, x)
		limit--

		x = next
	}

	return nodes
}

// GetByRankRange returns nodes within specific rank range [start, end].
// Note that the rank is 1-based integer. Rank 1 means the first node; Rank -1 means the last node
// If start is greater than end, the returned array is in reserved order
// If remove is true, the returned nodes are removed.
//
// Time complexity of this method is : O(log(N)).
func (ss *SortedSet) GetByRankRange(start, end int, remove bool) []*SortedSetNode {
	var (
		update    [SkipListMaxLevel]*SortedSetNode
		nodes     []*SortedSetNode
		traversed int
	)

	start, end = ss.sanitizeIndexes(start, end)

	reverse := start > end
	if reverse { // swap start and end
		start, end = end, start
	}

	traversed = 0
	x := ss.header
	for i := ss.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			traversed+int(x.level[i].span) < start {
			traversed += int(x.level[i].span)
			x = x.level[i].forward
		}
		if remove {
			update[i] = x
		} else {
			if traversed+1 == start {
				break
			}
		}
	}

	traversed++
	x = x.level[0].forward
	for x != nil && traversed <= end {
		next := x.level[0].forward

		nodes = append(nodes, x)

		if remove {
			ss.deleteNode(x, update)
		}

		traversed++
		x = next
	}

	if reverse {
		for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
			nodes[i], nodes[j] = nodes[j], nodes[i]
		}
	}
	return nodes
}

func (ss *SortedSet) sanitizeIndexes(start, end int) (newStart, newEnd int) {
	if start < 0 {
		start = int(ss.length) + start + 1
	}
	if end < 0 {
		end = int(ss.length) + end + 1
	}
	if start <= 0 {
		start = 1
	}
	if end <= 0 {
		end = 1
	}

	return start, end
}

// GetByRank returns the node at given rank.
// Note that the rank is 1-based integer. Rank 1 means the first node; Rank -1 means the last node.
// If remove is true, the returned nodes are removed
// If node is not found at specific rank, nil is returned.
//
// Time complexity of this method is : O(log(N)).
func (ss *SortedSet) GetByRank(rank int, remove bool) *SortedSetNode {
	nodes := ss.GetByRankRange(rank, rank, remove)
	if len(nodes) == 1 {
		return nodes[0]
	}
	return nil
}

// GetByKey returns the  node at given key.
// If node is not found, nil is returned
//
// Time complexity : O(1).
func (ss *SortedSet) GetByKey(key string) *SortedSetNode {
	return ss.Dict[key]
}

// FindRank Returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
// Note that the rank is 1-based integer. Rank 1 means the first node
// If the node is not found, 0 is returned. Otherwise rank(> 0) is returned.
//
// Time complexity of this method is : O(log(N)).
func (ss *SortedSet) FindRank(key string) int {
	rank := 0
	node := ss.Dict[key]
	if node != nil {
		x := ss.header
		for i := ss.level - 1; i >= 0; i-- {
			for x.level[i].forward != nil &&
				(x.level[i].forward.score < node.score ||
					(x.level[i].forward.score == node.score &&
						x.level[i].forward.key <= node.key)) {
				rank += int(x.level[i].span)
				x = x.level[i].forward
			}

			if x.key == key {
				return rank
			}
		}
	}
	return 0
}

// FindRevRank Returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
func (ss *SortedSet) FindRevRank(key string) int {
	if ss.length == 0 {
		return 0
	}

	if _, ok := ss.Dict[key]; !ok {
		return 0
	}

	return ss.Size() - ss.FindRank(key) + 1
}
