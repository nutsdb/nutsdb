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
	"bytes"
	"errors"
	"math/rand"
)

var (
	ErrSortedSetNotFound = errors.New("the sortedSet does not exist")

	ErrSortedSetMemberNotExist = errors.New("the member of sortedSet does not exist")

	ErrSortedSetIsEmpty = errors.New("the sortedSet if empty")
)

const (
	// SkipListMaxLevel represents the skipList max level number.
	SkipListMaxLevel = 32

	// SkipListP represents the p parameter of the skipList.
	SkipListP = 0.25
)

type SortedSet struct {
	db *DB
	M  map[string]*SkipList
}

func NewSortedSet(db *DB) *SortedSet {
	return &SortedSet{
		db: db,
		M:  map[string]*SkipList{},
	}
}

func (z *SortedSet) ZAdd(key string, score SCORE, value []byte, record *Record) error {
	sortedSet, ok := z.M[key]
	if !ok {
		z.M[key] = newSkipList(z.db)
		sortedSet = z.M[key]
	}

	return sortedSet.Put(score, value, record)
}

func (z *SortedSet) ZMembers(key string) (map[*Record]SCORE, error) {
	sortedSet, ok := z.M[key]

	if !ok {
		return nil, ErrSortedSetNotFound
	}

	nodes := sortedSet.dict

	members := make(map[*Record]SCORE, len(nodes))
	for _, node := range nodes {
		members[node.record] = node.score
	}

	return members, nil
}

func (z *SortedSet) ZCard(key string) (int, error) {
	if sortedSet, ok := z.M[key]; ok {
		return int(sortedSet.length), nil
	}

	return 0, ErrSortedSetNotFound
}

func (z *SortedSet) ZCount(key string, start SCORE, end SCORE, opts *GetByScoreRangeOptions) (int, error) {
	if sortedSet, ok := z.M[key]; ok {
		return len(sortedSet.GetByScoreRange(start, end, opts)), nil
	}
	return 0, ErrSortedSetNotFound
}

func (z *SortedSet) ZPeekMax(key string) (*Record, SCORE, error) {
	if sortedSet, ok := z.M[key]; ok {
		node := sortedSet.PeekMax()
		if node != nil {
			return node.record, node.score, nil
		}
		return nil, 0, ErrSortedSetIsEmpty
	}

	return nil, 0, ErrSortedSetNotFound
}

func (z *SortedSet) ZPopMax(key string) (*Record, SCORE, error) {
	if sortedSet, ok := z.M[key]; ok {
		node := sortedSet.PopMax()
		if node != nil {
			return node.record, node.score, nil
		}
		return nil, 0, ErrSortedSetIsEmpty
	}

	return nil, 0, ErrSortedSetNotFound
}

func (z *SortedSet) ZPeekMin(key string) (*Record, SCORE, error) {
	if sortedSet, ok := z.M[key]; ok {
		node := sortedSet.PeekMin()
		if node != nil {
			return node.record, node.score, nil
		}
		return nil, 0, ErrSortedSetIsEmpty
	}

	return nil, 0, ErrSortedSetNotFound
}

func (z *SortedSet) ZPopMin(key string) (*Record, SCORE, error) {
	if sortedSet, ok := z.M[key]; ok {
		node := sortedSet.PopMin()
		if node != nil {
			return node.record, node.score, nil
		}
		return nil, 0, ErrSortedSetIsEmpty
	}

	return nil, 0, ErrSortedSetNotFound
}

func (z *SortedSet) ZRangeByScore(key string, start SCORE, end SCORE, opts *GetByScoreRangeOptions) ([]*Record, []float64, error) {
	if sortedSet, ok := z.M[key]; ok {

		nodes := sortedSet.GetByScoreRange(start, end, opts)

		records := make([]*Record, len(nodes))
		scores := make([]float64, len(nodes))

		for i, node := range nodes {
			records[i] = node.record
			scores[i] = float64(node.score)
		}

		return records, scores, nil
	}

	return nil, nil, ErrSortedSetNotFound
}

func (z *SortedSet) ZRangeByRank(key string, start int, end int) ([]*Record, []float64, error) {
	if sortedSet, ok := z.M[key]; ok {

		nodes := sortedSet.GetByRankRange(start, end, false)

		records := make([]*Record, len(nodes))
		scores := make([]float64, len(nodes))

		for i, node := range nodes {
			records[i] = node.record
			scores[i] = float64(node.score)
		}

		return records, scores, nil
	}

	return nil, nil, ErrSortedSetNotFound
}

func (z *SortedSet) ZRem(key string, value []byte) (*Record, error) {
	if sortedSet, ok := z.M[key]; ok {
		hash, err := getFnv32(value)
		if err != nil {
			return nil, err
		}
		node := sortedSet.Remove(hash)
		if node != nil {
			return node.record, nil
		}
		return nil, ErrSortedSetMemberNotExist
	}

	return nil, ErrSortedSetNotFound
}

func (z *SortedSet) ZRemRangeByRank(key string, start int, end int) error {
	if sortedSet, ok := z.M[key]; ok {

		_ = sortedSet.GetByRankRange(start, end, true)
		return nil
	}

	return ErrSortedSetNotFound
}

func (z *SortedSet) getZRemRangeByRankNodes(key string, start int, end int) ([]*SkipListNode, error) {
	if sortedSet, ok := z.M[key]; ok {
		return sortedSet.GetByRankRange(start, end, false), nil
	}

	return []*SkipListNode{}, nil
}

func (z *SortedSet) ZRank(key string, value []byte) (int, error) {
	if sortedSet, ok := z.M[key]; ok {
		hash, err := getFnv32(value)
		if err != nil {
			return 0, err
		}
		rank := sortedSet.FindRank(hash)
		if rank == 0 {
			return 0, ErrSortedSetMemberNotExist
		}
		return rank, nil
	}
	return 0, ErrSortedSetNotFound
}

func (z *SortedSet) ZRevRank(key string, value []byte) (int, error) {
	if sortedSet, ok := z.M[key]; ok {
		hash, err := getFnv32(value)
		if err != nil {
			return 0, err
		}
		rank := sortedSet.FindRevRank(hash)
		if rank == 0 {
			return 0, ErrSortedSetMemberNotExist
		}
		return rank, nil
	}
	return 0, ErrSortedSetNotFound
}

func (z *SortedSet) ZScore(key string, value []byte) (float64, error) {
	if sortedSet, ok := z.M[key]; ok {
		node := sortedSet.GetByValue(value)
		if node != nil {
			return float64(sortedSet.GetByValue(value).score), nil
		}
		return 0, ErrSortedSetMemberNotExist
	}
	return 0, ErrSortedSetNotFound
}

func (z *SortedSet) ZExist(key string, value []byte) (bool, error) {
	if sortedSet, ok := z.M[key]; ok {
		hash, err := getFnv32(value)
		if err != nil {
			return false, err
		}
		_, ok := sortedSet.dict[hash]
		return ok, nil
	}
	return false, ErrSortedSetNotFound
}

// SCORE represents the score type.
type SCORE float64

// SkipListLevel records forward and span.
type SkipListLevel struct {
	forward *SkipListNode
	span    int64
}

// The SkipList represents the sorted set.
type SkipList struct {
	db     *DB
	header *SkipListNode
	tail   *SkipListNode
	length int64
	level  int
	dict   map[uint32]*SkipListNode
}

// SkipListNode represents a node in the SkipList.
type SkipListNode struct {
	hash     uint32  // unique key of this node
	record   *Record // associated data
	score    SCORE   // score to determine the order of this node in the set
	backward *SkipListNode
	level    []SkipListLevel
}

// Hash returns the key of the node.
func (sln *SkipListNode) Hash() uint32 {
	return sln.hash
}

// Score returns the score of the node.
func (sln *SkipListNode) Score() SCORE {
	return sln.score
}

// createNode returns a newly initialized SkipListNode Object that implements the SkipListNode.
func createNode(level int, score SCORE, hash uint32, record *Record) *SkipListNode {
	node := SkipListNode{
		hash:   hash,
		record: record,
		score:  score,
		level:  make([]SkipListLevel, level),
	}
	return &node
}

// randomLevel returns a random level for the new skiplist node we are going to create.
// The return value of this function is between 1 and SkipListMaxLevel
// (both inclusive), with a powerlaw-alike distribution where higher
// levels are lesl likely to be returned.
func randomLevel() int {
	level := 1

	for float64(rand.Int31()&0xFFFF) < SkipListP*0xFFFF {
		level += 1
	}
	if level < SkipListMaxLevel {
		return level
	}

	return SkipListMaxLevel
}

func newSkipList(db *DB) *SkipList {
	skipList := &SkipList{
		db:    db,
		level: 1,
		dict:  make(map[uint32]*SkipListNode),
	}
	hash, _ := getFnv32([]byte(""))
	skipList.header = createNode(SkipListMaxLevel, 0, hash, nil)
	return skipList
}

func (sl *SkipList) cmp(r1 *Record, r2 *Record) int {
	val1, _ := sl.db.getValueByRecord(r1)
	val2, _ := sl.db.getValueByRecord(r2)
	return bytes.Compare(val1, val2)
}

func (sl *SkipList) insertNode(score SCORE, hash uint32, record *Record) *SkipListNode {
	var update [SkipListMaxLevel]*SkipListNode
	var rank [SkipListMaxLevel]int64

	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		// store rank that is crosled to reach the insert position
		if sl.level-1 == i {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && // score is the same but the key is different
					sl.cmp(x.level[i].forward.record, record) < 0)) {
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

	if level > sl.level { // add a new level
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.header
			update[i].level[i].span = sl.length
		}
		sl.level = level
	}

	x = createNode(level, score, hash, record)
	for i := 0; i < level; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x

		/* update span covered by update[i] as x is inserted here */
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])

		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// increment span for untouched levels
	for i := level; i < sl.level; i++ {
		update[i].level[i].span++
	}

	if update[0] == sl.header {
		x.backward = nil
	} else {
		x.backward = update[0]
	}

	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		sl.tail = x
	}

	sl.length++

	return x
}

// deleteNode represents internal function used by delete, DeleteByScore and DeleteByRank.
func (sl *SkipList) deleteNode(x *SkipListNode, update [SkipListMaxLevel]*SkipListNode) {
	for i := 0; i < sl.level; i++ {
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
		sl.tail = x.backward
	}
	for sl.level > 1 && sl.header.level[sl.level-1].forward == nil {
		sl.level--
	}
	sl.length--
	delete(sl.dict, x.hash)
}

// delete removes an element with matching score/key from the skiplist.
func (sl *SkipList) delete(score SCORE, hash uint32) bool {
	var update [SkipListMaxLevel]*SkipListNode

	targetNode := sl.dict[hash]

	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score &&
					sl.cmp(x.level[i].forward.record, targetNode.record) < 0)) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	/* We may have multiple elements with the same score, what we need
	 * is to find the element with both the right score and object. */
	x = x.level[0].forward
	if x != nil && score == x.score && sl.cmp(x.record, targetNode.record) == 0 {
		sl.deleteNode(x, update)
		// free x
		return true
	}
	return false /* not found */
}

// Size returns the number of elements in the SkipList.
func (sl *SkipList) Size() int {
	return int(sl.length)
}

// PeekMin returns the element with minimum score, nil if the set is empty.
//
// Time complexity of this method is : O(log(N)).
func (sl *SkipList) PeekMin() *SkipListNode {
	return sl.header.level[0].forward
}

// PopMin returns and remove the element with minimal score, nil if the set is empty.
//
// Time complexity of this method is : O(log(N)).
func (sl *SkipList) PopMin() *SkipListNode {
	x := sl.header.level[0].forward
	if x != nil {
		sl.Remove(x.hash)
	}
	return x
}

// PeekMax returns the element with maximum score, nil if the set is empty.
//
// Time Complexity : O(1).
func (sl *SkipList) PeekMax() *SkipListNode {
	return sl.tail
}

// PopMax returns and remove the element with maximum score, nil if the set is empty.
//
// Time complexity of this method is : O(log(N)).
func (sl *SkipList) PopMax() *SkipListNode {
	x := sl.tail
	if x != nil {
		sl.Remove(x.hash)
	}
	return x
}

// Put puts an element into the sorted set with specific key / value / score.
//
// Time complexity of this method is : O(log(N)).
func (sl *SkipList) Put(score SCORE, value []byte, record *Record) error {
	var newNode *SkipListNode

	hash, _ := getFnv32(value)

	if n, ok := sl.dict[hash]; ok {
		// score does not change, only update value
		if n.score != score { // score changes, delete and re-insert
			sl.delete(n.score, n.hash)
			newNode = sl.insertNode(score, hash, record)
		}
	} else {
		newNode = sl.insertNode(score, hash, record)
	}

	if newNode != nil {
		sl.dict[hash] = newNode
	}

	return nil
}

// Remove removes element specified at given key.
//
// Time complexity of this method is : O(log(N)).
func (sl *SkipList) Remove(hash uint32) *SkipListNode {
	found := sl.dict[hash]
	if found != nil {
		sl.delete(found.score, hash)
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
func (sl *SkipList) GetByScoreRange(start SCORE, end SCORE, options *GetByScoreRangeOptions) []*SkipListNode {
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

	var nodes []*SkipListNode

	// determine if out of range
	if sl.length == 0 {
		return nodes
	}

	if reverse {
		// search from end to start
		return sl.searchReverse(nodes, excludeStart, excludeEnd, start, end, limit)
	}
	// search from start to end
	return sl.searchForward(nodes, excludeStart, excludeEnd, start, end, limit)
}

func (sl *SkipList) searchForward(nodes []*SkipListNode, excludeStart, excludeEnd bool, start, end SCORE, limit int) []*SkipListNode {
	// search from start to end
	x := sl.header
	if excludeStart {
		for i := sl.level - 1; i >= 0; i-- {
			for x.level[i].forward != nil &&
				x.level[i].forward.score <= start {
				x = x.level[i].forward
			}
		}
	} else {
		for i := sl.level - 1; i >= 0; i-- {
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

func (sl *SkipList) searchReverse(nodes []*SkipListNode, excludeStart, excludeEnd bool, start, end SCORE, limit int) []*SkipListNode {
	x := sl.header

	if excludeEnd {
		for i := sl.level - 1; i >= 0; i-- {
			for x.level[i].forward != nil &&
				x.level[i].forward.score < end {
				x = x.level[i].forward
			}
		}
	} else {
		for i := sl.level - 1; i >= 0; i-- {
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
func (sl *SkipList) GetByRankRange(start, end int, remove bool) []*SkipListNode {
	var (
		update    [SkipListMaxLevel]*SkipListNode
		nodes     []*SkipListNode
		traversed int
	)

	start, end = sl.sanitizeIndexes(start, end)

	reverse := start > end
	if reverse { // swap start and end
		start, end = end, start
	}

	traversed = 0
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
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
			sl.deleteNode(x, update)
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

func (sl *SkipList) sanitizeIndexes(start, end int) (newStart, newEnd int) {
	if start < 0 {
		start = int(sl.length) + start + 1
	}
	if end < 0 {
		end = int(sl.length) + end + 1
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
func (sl *SkipList) GetByRank(rank int, remove bool) *SkipListNode {
	nodes := sl.GetByRankRange(rank, rank, remove)
	if len(nodes) == 1 {
		return nodes[0]
	}
	return nil
}

// GetByValue returns the node at given key.
// If node is not found, nil is returned
//
// Time complexity : O(1).
func (sl *SkipList) GetByValue(value []byte) *SkipListNode {
	hash, _ := getFnv32(value)
	return sl.dict[hash]
}

// FindRank Returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
// Note that the rank is 1-based integer. Rank 1 means the first node
// If the node is not found, 0 is returned. Otherwise rank(> 0) is returned.
//
// Time complexity of this method is : O(log(N)).
func (sl *SkipList) FindRank(hash uint32) int {
	rank := 0
	targetNode := sl.dict[hash]
	if targetNode != nil {
		x := sl.header
		for i := sl.level - 1; i >= 0; i-- {
			for x.level[i].forward != nil &&
				(x.level[i].forward.score < targetNode.score ||
					(x.level[i].forward.score == targetNode.score &&
						sl.cmp(x.level[i].forward.record, targetNode.record) <= 0)) {
				rank += int(x.level[i].span)
				x = x.level[i].forward
			}

			if x.hash == hash {
				return rank
			}
		}
	}
	return 0
}

// FindRevRank Returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
func (sl *SkipList) FindRevRank(hash uint32) int {
	if sl.length == 0 {
		return 0
	}

	if _, ok := sl.dict[hash]; !ok {
		return 0
	}

	return sl.Size() - sl.FindRank(hash) + 1
}
