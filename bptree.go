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

package nutsdb

import (
	"bytes"
	"errors"
)

var (
	// ErrStartKey is returned when Range is called by a error start key.
	ErrStartKey = errors.New("err start key")

	// ErrScansNoResult is returned when Range or prefixScan are called no result to found.
	ErrScansNoResult = errors.New("range scans or prefix scans no result")

	// ErrPrefixScansNoResult is returned when prefixScan is called no result to found.
	ErrPrefixScansNoResult = errors.New("prefix scans no result")

	// ErrKeyNotFound is returned when the key is not in the b+ tree.
	ErrKeyNotFound = errors.New("key not found")
)

const (
	// Default number of b+ tree orders
	order = 8

	// RangeScan returns range scanMode flag
	RangeScan = "RangeScan"

	// PrefixScan returns prefix scanMode flag
	PrefixScan = "PrefixScan"

	// CountFlagEnabled returns enabled CountFlag
	CountFlagEnabled = true

	// CountFlagDisabled returns disabled CountFlag
	CountFlagDisabled = false
)

type (
	// BPTree records root node and valid key number.
	BPTree struct {
		root          *Node
		ValidKeyCount int // the number of the key that not expired or deleted
		idxType       int
	}

	// Records records multi-records as result when is called Range or PrefixScan.
	Records map[string]*Record

	// Node records keys and pointers and parent node.
	Node struct {
		Keys     [][]byte
		pointers []interface{}
		parent   *Node
		isLeaf   bool
		KeysNum  int
	}
)

// newNode returns a newly initialized Node object that implements the Node.
func newNode() *Node {
	return &Node{
		Keys:     make([][]byte, order-1),
		pointers: make([]interface{}, order),
		isLeaf:   false,
		parent:   nil,
		KeysNum:  0,
	}
}

// newLeaf returns a newly initialized Node object that implements the Node and set isLeaf flag.
func newLeaf() *Node {
	leaf := newNode()
	leaf.isLeaf = true
	return leaf
}

// NewTree returns a newly initialized BPTree Object that implements the BPTree.
func NewTree() *BPTree {
	return &BPTree{}
}

// FindLeaf returns leaf at the given key.
func (t *BPTree) FindLeaf(key []byte) *Node {
	var (
		i    int
		curr *Node
	)

	if curr = t.root; curr == nil {
		return nil
	}

	for !curr.isLeaf {
		i = 0
		for i < curr.KeysNum {
			if compare(key, curr.Keys[i]) >= 0 {
				i++
			} else {
				break
			}
		}
		curr = curr.pointers[i].(*Node)
	}

	return curr
}

// Compare returns an integer comparing two byte slices lexicographically.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
// A nil argument is equivalent to an empty slice.
func compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// findRange returns numFound,keys and pointers at the given start key and end key.
func (t *BPTree) findRange(start, end []byte) (numFound int, keys [][]byte, pointers []interface{}) {
	var (
		n        *Node
		i, j     int
		scanFlag bool
	)

	if n = t.FindLeaf(start); n == nil {
		return 0, nil, nil
	}

	for j = 0; j < n.KeysNum && compare(n.Keys[j], start) < 0; {
		j++
	}

	scanFlag = true
	for n != nil && scanFlag {
		for i = j; i < n.KeysNum; i++ {
			if compare(n.Keys[i], end) > 0 {
				scanFlag = false
				break
			}
			keys = append(keys, n.Keys[i])
			pointers = append(pointers, n.pointers[i])
			numFound++
		}

		n, _ = n.pointers[order-1].(*Node)

		j = 0
	}

	return
}

// Range returns records at the given start key and end key.
func (t *BPTree) Range(start, end []byte) (records Records, err error) {
	if compare(start, end) > 0 {
		return nil, ErrStartKey
	}

	return getRecordWrapper(t.findRange(start, end))
}

// getRecordWrapper returns a wrapper of records when Range or PrefixScan are called.
func getRecordWrapper(numFound int, keys [][]byte, pointers []interface{}) (records Records, err error) {
	if numFound == 0 {
		return nil, ErrScansNoResult
	}

	records = make(Records)
	for i := 0; i < numFound; i++ {
		records[string(keys[i])] = pointers[i].(*Record)
	}

	return records, nil
}

// PrefixScan returns records at the given prefix and limitNum
// limitNum: limit the number of the scanned records return.
func (t *BPTree) PrefixScan(prefix []byte, limitNum int) (records Records, err error) {
	var (
		n              *Node
		scanFlag       bool
		keys           [][]byte
		pointers       []interface{}
		i, j, numFound int
	)

	n = t.FindLeaf(prefix)

	if n == nil {
		return nil, ErrPrefixScansNoResult
	}

	for j = 0; j < n.KeysNum && compare(n.Keys[j], prefix) < 0; {
		j++
	}

	scanFlag = true
	numFound = 0
	for n != nil && scanFlag {
		for i = j; i < n.KeysNum; i++ {
			if !bytes.HasPrefix(n.Keys[i], prefix) {
				scanFlag = false
				break
			}

			keys = append(keys, n.Keys[i])
			pointers = append(pointers, n.pointers[i])
			numFound++

			if limitNum > 0 && numFound == limitNum {
				scanFlag = false
				break
			}
		}

		n, _ = n.pointers[order-1].(*Node)
		j = 0
	}

	return getRecordWrapper(numFound, keys, pointers)
}

// Find retrieves record at the given key.
func (t *BPTree) Find(key []byte) (*Record, error) {
	var (
		leaf *Node
		i    int
	)

	// Find leaf by key.
	leaf = t.FindLeaf(key)

	if leaf == nil {
		return nil, ErrKeyNotFound
	}

	for i = 0; i < leaf.KeysNum; i++ {
		if compare(key, leaf.Keys[i]) == 0 {
			break
		}
	}

	if i == leaf.KeysNum {
		return nil, ErrKeyNotFound
	}

	return leaf.pointers[i].(*Record), nil
}

// startNewTree returns a start new tree.
func (t *BPTree) startNewTree(key []byte, pointer *Record) error {
	t.root = newLeaf()
	t.root.Keys[0] = key
	t.root.pointers[0] = pointer
	t.root.KeysNum = 1

	return nil
}

// Insert inserts record to the b+ tree,
// and if the key exists, update the record and the counter(if countFlag set true,it will start count).
func (t *BPTree) Insert(key []byte, e *Entry, h *Hint, countFlag bool) error {
	if r, err := t.Find(key); err == nil && r != nil {
		if countFlag && h.meta.Flag == DataDeleteFlag && r.H.meta.Flag != DataDeleteFlag && t.ValidKeyCount > 0 {
			t.ValidKeyCount--
		}

		if countFlag && h.meta.Flag != DataDeleteFlag && r.H.meta.Flag == DataDeleteFlag {
			t.ValidKeyCount++
		}

		return r.UpdateRecord(h, e)
	}

	// Initialize the Record object When key does not exist.
	pointer := &Record{H: h, E: e}

	// Update the validKeyCount number
	t.ValidKeyCount++

	// Check if the root node is nil or not
	// if nil build a start new tree for insert.
	if t.root == nil {
		return t.startNewTree(key, pointer)
	}

	// Find the leaf node to insert.
	leaf := t.FindLeaf(key)

	// Check if the leaf node is full or not
	// if not full insert into the leaf node.
	if leaf.KeysNum < order-1 {
		insertIntoLeaf(leaf, key, pointer)
		return nil
	}

	// split the leaf node when it is not enough space to insert.
	return t.splitLeaf(leaf, key, pointer)
}

// getSplitIndex returns split index at the given length.
func getSplitIndex(length int) int {
	if length%2 == 0 {
		return length / 2
	}

	return length/2 + 1
}

// splitLeaf splits leaf and insert the parent node when the leaf is full.
func (t *BPTree) splitLeaf(leaf *Node, key []byte, pointer *Record) error {
	var j, k, i int

	tmpKeys := make([][]byte, order)
	tmpPointers := make([]interface{}, order)

	// Find the ready position of the insertion.
	for i < order-1 {
		if compare(leaf.Keys[i], key) < 0 {
			i++
		} else {
			break
		}
	}

	// TmpKeys records the leaf keys
	// tmpPointers records the leaf pointers
	// and filter the ready position of the insertion.
	for j = 0; j < leaf.KeysNum; j++ {
		if k == i {
			k++
		}
		tmpKeys[k] = leaf.Keys[j]
		tmpPointers[k] = leaf.pointers[j]
		k++
	}

	tmpKeys[i] = key
	tmpPointers[i] = pointer

	// Get the split index for the leaf node.
	splitIndex := getSplitIndex(order)

	// Reset the the keysNum of the leaf.
	leaf.KeysNum = 0

	// Reset the keys and pointers.
	for i = 0; i < splitIndex; i++ {
		leaf.Keys[i] = tmpKeys[i]
		leaf.pointers[i] = tmpPointers[i]
		leaf.KeysNum++
	}

	// Set the keys and pointers for the new leaf.
	j = 0
	newLeaf := newLeaf()
	for i = splitIndex; i < order; i++ {
		newLeaf.Keys[j] = tmpKeys[i]
		newLeaf.pointers[j] = tmpPointers[i]
		newLeaf.KeysNum++
		j++
	}

	// Set the last pointer of the new leaf node to point the last pointer of the leaf node.
	if leaf.pointers[order-1] != nil {
		newLeaf.pointers[order-1] = leaf.pointers[order-1]
	}

	// Reset the last pointer of the leaf node.
	leaf.pointers[order-1] = newLeaf
	// Set the parent.
	newLeaf.parent = leaf.parent

	// Insert into the parent node at the given the the first key of the new leaf node.
	newKey := newLeaf.Keys[0]
	return t.insertIntoParent(leaf, newKey, newLeaf)
}

// insertIntoNewRoot returns a now root when the insertIntoParent is called
func (t *BPTree) insertIntoNewRoot(left *Node, key []byte, right *Node) error {
	t.root = newNode()

	t.root.Keys[0] = key
	t.root.pointers[0] = left
	t.root.pointers[1] = right
	t.root.KeysNum++
	t.root.parent = nil

	left.parent = t.root
	right.parent = t.root

	return nil
}

// insertIntoNode inserts into the given node at the given leftIndex,key and right node.
func (t *BPTree) insertIntoNode(node *Node, leftIndex int, key []byte, right *Node) error {
	for i := node.KeysNum; i > leftIndex; i-- {
		node.Keys[i] = node.Keys[i-1]
		node.pointers[i+1] = node.pointers[i]
	}

	node.Keys[leftIndex] = key
	node.pointers[leftIndex+1] = right
	node.KeysNum++

	return nil
}

// insertIntoParent inserts into the parent of the give node.
func (t *BPTree) insertIntoParent(left *Node, key []byte, right *Node) error {
	// Check if the parent of the leaf node is nil or not
	// if nil means the leaf is root node.
	if left.parent == nil {
		return t.insertIntoNewRoot(left, key, right)
	}

	// Get the left index.
	leftIndex := 0
	for leftIndex <= left.parent.KeysNum {
		if left == left.parent.pointers[leftIndex] {
			break
		} else {
			leftIndex++
		}
	}

	// Check if the parent of left node is full or not
	// if not full,then insert into the parent node.
	if left.parent.KeysNum < order-1 {
		return t.insertIntoNode(left.parent, leftIndex, key, right)
	}

	// The the parent of left node is full, split the parent node.
	return t.splitParent(left.parent, leftIndex, key, right)
}

// splitParent splits the given node at the given leftIndex,key and right node.
func (t *BPTree) splitParent(node *Node, leftIndex int, key []byte, right *Node) error {
	tmpKeys := make([][]byte, order)
	tmpPointers := make([]interface{}, order+1)

	// In addition to the index location of leftIndex filtered out
	// the other key of the node is stored in tmpKeys.
	var i, j int
	for i = 0; i < node.KeysNum; i++ {
		if i == leftIndex {
			j++
		}
		tmpKeys[j] = node.Keys[i]
		j++

	}

	// In addition to the index location of leftIndex+1 filtered out
	// the other pointer of the node is stored in tmpPointers.
	j = 0
	for i = 0; i < node.KeysNum+1; i++ {
		if i == leftIndex+1 {
			j++
		}
		tmpPointers[j] = node.pointers[i]
		j++

	}

	tmpKeys[leftIndex] = key
	tmpPointers[leftIndex+1] = right

	// Get the split index for the intermediate node.
	splitIndex := getSplitIndex(order - 1)

	// Reset the KeysNum of the node.
	node.KeysNum = 0
	for i = 0; i < splitIndex; i++ {
		node.Keys[i] = tmpKeys[i]
		node.pointers[i] = tmpPointers[i]
		node.KeysNum++
	}

	// Reset the last pointer of the node.
	node.pointers[i] = tmpPointers[i]

	newNode := newNode()

	j = 0
	for i++; i < order; i++ {
		newNode.Keys[j] = tmpKeys[i]
		newNode.pointers[j] = tmpPointers[i]
		newNode.KeysNum++
		j++
	}

	// Set the parent of the new node.
	newNode.parent = node.parent

	// Set the last pointer of the new node.
	newNode.pointers[j] = tmpPointers[i]

	// Set the parent of the pointer node for new node.
	for i = 0; i <= newNode.KeysNum; i++ {
		child := newNode.pointers[i].(*Node)
		child.parent = newNode
	}

	// Insert into the parent node at the given the the first key of the new node.
	newKey := tmpKeys[splitIndex]
	return t.insertIntoParent(node, newKey, newNode)
}

// insertIntoLeaf inserts the given node at the given key and pointer.
func insertIntoLeaf(leaf *Node, key []byte, pointer *Record) {
	i := 0
	for i < leaf.KeysNum {
		if compare(key, leaf.Keys[i]) > 0 {
			i++
		} else {
			break
		}
	}

	for j := leaf.KeysNum; j > i; j-- {
		leaf.Keys[j] = leaf.Keys[j-1]
		leaf.pointers[j] = leaf.pointers[j-1]
	}

	leaf.Keys[i] = key
	leaf.pointers[i] = pointer
	leaf.KeysNum++
}
