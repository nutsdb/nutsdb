// Copyright 2026 The nutsdb Author. All rights reserved.
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

package store

import (
	"bytes"

	"github.com/nutsdb/nutsdb/internal/fileio"
)

// MemStore is a thin facade over RBTree[fileio.Location] used by unit tests /
// benchmarks for the ordered-map implementation. The LSM engine uses MemTable
// (RBTree[memValue]) instead; this type is not on the StoreManager path.
type MemStore interface {
	// Put a key - value pair into the memstore.
	// operation: upsert.
	Put(key []byte, value fileio.Location) error
	// Get a value by key from the memstore.
	Get(key []byte) (fileio.Location, error)
	// Delete a key - value pair from the memstore.
	// if key is not found, return nil and false.
	// if key is found, return the value and true.
	Delete(key []byte) (fileio.Location, bool)
	// Iterate walks all keys and values in sorted key order and invokes callback function;
	// iteration stops when callback returns false.
	Iterate(callback func(key []byte, value fileio.Location) bool)
}

type color bool

const (
	Red   color = false
	Black color = true
)

type RBTree[V any] struct {
	root *rbNode[V]
	size int
}

func (rb *RBTree[V]) Size() int {
	if rb == nil {
		return 0
	}
	return rb.size
}

type rbNode[V any] struct {
	key                 []byte
	color               color
	value               V
	left, right, parent *rbNode[V]
}

func (node *rbNode[V]) setNode(v V) {
	if node == nil {
		return
	}
	node.value = v
}

// newRBTree creates a new red-black tree.
func newRBTree[V any]() *RBTree[V] {
	return &RBTree[V]{
		root: nil,
		size: 0,
	}
}

func NewMemStore() MemStore {
	return newRBTree[fileio.Location]()
}

func newRBNode[V any](key []byte, value V) *rbNode[V] {
	return &rbNode[V]{
		key:    key,
		value:  value,
		color:  Red,
		left:   nil,
		right:  nil,
		parent: nil,
	}
}

// Add inserts a node into the tree.
func (rb *RBTree[V]) Add(key []byte, value V) error {
	return rb.addNode(newRBNode(key, value))
}

// Delete removes a node by key.
func (rb *RBTree[V]) Delete(key []byte) (V, bool) {
	if node := rb.findNode(key); node != nil {
		value := node.value
		rb.deleteNode(node)
		return value, true
	}
	var v V
	return v, false
}

// Find looks up a node by key.
func (rb *RBTree[V]) Get(key []byte) (V, error) {
	var v V
	if node := rb.findNode(key); node != nil {
		return node.value, nil
	}
	return v, ErrKeyNotFound
}
func (rb *RBTree[V]) Put(key []byte, value V) error {
	if node := rb.findNode(key); node != nil {
		node.setNode(value)
		return nil
	}
	return rb.Add(key, value)
}

// Iterate walks nodes in key order and invokes cb; iteration stops when cb returns false.
func (rb *RBTree[V]) Iterate(cb func(key []byte, value V) bool) {
	rb.inOrderTraversal(func(node *rbNode[V]) bool {
		return cb(node.key, node.value)
	})
}

// inOrderTraversal performs an in-order traversal.
func (rb *RBTree[V]) inOrderTraversal(visit func(node *rbNode[V]) bool) {
	stack := make([]*rbNode[V], 0)
	curr := rb.root
	for curr != nil || len(stack) > 0 {
		for curr != nil {
			stack = append(stack, curr)
			curr = curr.left
		}
		curr = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if !visit(curr) {
			break
		}
		curr = curr.right
	}
}

// addNode inserts a new node.
func (rb *RBTree[V]) addNode(node *rbNode[V]) error {
	var fixNode *rbNode[V]
	if rb.root == nil {
		rb.root = newRBNode(node.key, node.value)
		fixNode = rb.root
	} else {
		t := rb.root
		cmp := 0
		parent := &rbNode[V]{}
		for t != nil {
			parent = t
			cmp = bytes.Compare(node.key, t.key)
			if cmp < 0 {
				t = t.left
			} else if cmp > 0 {
				t = t.right
			} else if cmp == 0 {
				return ErrRBTreeSameRBNode
			}
		}
		fixNode = &rbNode[V]{
			key:    node.key,
			parent: parent,
			value:  node.value,
			color:  Red,
		}
		if cmp < 0 {
			parent.left = fixNode
		} else {
			parent.right = fixNode
		}
	}
	rb.size++
	rb.fixAfterAdd(fixNode)
	return nil
}

// deleteNode removes a node from the red-black tree.
// Deletion has two phases: replace with successor, then recolor and rotate.
// Successor selection:
// case1: node has two children — use findSuccessor
// case2: node has one non-nil child
// case3: node has no children
// Recolor and rotate:
// case1: deleted node was black — rebalance to restore black-height invariant
// case2: deleted node was red — no rebalancing needed
func (rb *RBTree[V]) deleteNode(tgt *rbNode[V]) {
	node := tgt
	// Node has two children; copy successor data and delete successor.
	if node.left != nil && node.right != nil {
		s := rb.findSuccessor(node)
		node.key = bytes.Clone(s.key)
		node.value = s.value
		node = s
	}
	var replacement *rbNode[V]
	// Node has exactly one non-nil child.
	if node.left != nil {
		replacement = node.left
	} else {
		replacement = node.right
	}
	if replacement != nil {
		replacement.parent = node.parent
		if node.parent == nil {
			rb.root = replacement
		} else if node == node.parent.left {
			node.parent.left = replacement
		} else {
			node.parent.right = replacement
		}
		node.left = nil
		node.right = nil
		node.parent = nil
		if node.getColor() {
			rb.fixAfterDelete(replacement)
		}
	} else if node.parent == nil {
		// Node has no parent, so it is the root.
		rb.root = nil
	} else {
		// Node has no children.
		if node.getColor() {
			rb.fixAfterDelete(node)
		}
		if node.parent != nil {
			switch node {
			case node.parent.left:
				node.parent.left = nil
			case node.parent.right:
				node.parent.right = nil
			}
			node.parent = nil
		}
	}
	rb.size--
}

// findSuccessor returns the in-order successor of node.
// case1: if node has a right child, successor is the minimum node in the right subtree
// case2: otherwise, successor is the lowest ancestor whose left child is on the path from node
func (rb *RBTree[V]) findSuccessor(node *rbNode[V]) *rbNode[V] {
	if node == nil {
		return nil
	} else if node.right != nil {
		p := node.right
		for p.left != nil {
			p = p.left
		}
		return p
	} else {
		p := node.parent
		ch := node
		for p != nil && ch == p.right {
			ch = p
			p = p.parent
		}
		return p
	}

}

func (rb *RBTree[V]) findNode(key []byte) *rbNode[V] {
	node := rb.root
	for node != nil {
		cmp := bytes.Compare(key, node.key)
		if cmp < 0 {
			node = node.left
		} else if cmp > 0 {
			node = node.right
		} else {
			return node
		}
	}
	return nil
}

// fixAfterAdd restores red-black invariants after insertion.
// No fix is needed for nil nodes, the root, or when the parent is black.
// Three cases: fixUncleRed, fixAddLeftBlack, fixAddRightBlack.
func (rb *RBTree[V]) fixAfterAdd(x *rbNode[V]) {
	x.color = Red
	for x != nil && x != rb.root && x.getParent().getColor() == Red {
		uncle := x.getUncle()
		if uncle.getColor() == Red {
			x = rb.fixUncleRed(x, uncle)
			continue
		}
		if x.getParent() == x.getGrandParent().getLeft() {
			x = rb.fixAddLeftBlack(x)
			continue
		}
		x = rb.fixAddRightBlack(x)
	}
	rb.root.setColor(Black)
}

// fixUncleRed handles the case where the uncle is red.
// Parent and uncle are recolored black; grandparent is recolored red.
//
//							  b(b)                    b(r)
//							/		\				/		\
//						  a(r)        y(r)  ->   a(b)        y(b)
//						/   \       /  \         /   \       /  \
//		            x(r)    nil   nil  nil    x (r) nil   nil  nil
//	             	/  \                      /  \
//	            	nil nil                   nil nil
func (rb *RBTree[V]) fixUncleRed(x *rbNode[V], y *rbNode[V]) *rbNode[V] {
	x.getParent().setColor(Black)
	y.setColor(Black)
	x.getGrandParent().setColor(Red)
	x = x.getGrandParent()
	return x
}

// fixAddLeftBlack handles insertion fix-up when the uncle is black and the parent is a left child.
// If x is a right child, rotate left at the parent first; then recolor and rotate right at grandparent.
//
//							  b(b)                    b(b)                b(r)
//							/		\				/		\            /   \
//						  a(r)        y(b)  ->   a(r)        y(b)  ->  a(b)   y(b)
//						/   \       /  \         /   \       /  \      /  \    /  \
//		               nil   x (r) nil  nil      x(r) nil  nil  nil   x(r) nil nil nil
//	           		 		 /  \               /  \                  / \
//	           		 		nil nil             nil nil              nil nil
func (rb *RBTree[V]) fixAddLeftBlack(x *rbNode[V]) *rbNode[V] {
	if x == x.getParent().getRight() {
		x = x.getParent()
		rb.rotateLeft(x)
	}
	x.getParent().setColor(Black)
	x.getGrandParent().setColor(Red)
	rb.rotateRight(x.getGrandParent())
	return x
}

// fixAddRightBlack handles insertion fix-up when the uncle is black and the parent is a right child.
// If x is a left child, rotate right at the parent first; then recolor and rotate left at grandparent.
//
//							  b(b)                    b(b)                b(r)
//							/		\				/		\            /   \
//						  y(b)       a(r)  ->   y(b)        a(r)  ->  y(b)     a(b)
//						/   \       /  \         /   \       /  \      /  \    /  \
//		               nil   nil x(r)  nil      nil nil  nil  x(r)   nil nil  nil  x(r)
//	           		 		      /  \                         /  \               /  \
//	           		 		      nil nil                    nil nil              nil nil
func (rb *RBTree[V]) fixAddRightBlack(x *rbNode[V]) *rbNode[V] {
	if x == x.getParent().getLeft() {
		x = x.getParent()
		rb.rotateRight(x)
	}
	x.getParent().setColor(Black)
	x.getGrandParent().setColor(Red)
	rb.rotateLeft(x.getGrandParent())
	return x
}

// fixAfterDelete restores red-black invariants after deletion.
// Delegates to fixAfterDeleteLeft or fixAfterDeleteRight based on x's position.
func (rb *RBTree[V]) fixAfterDelete(x *rbNode[V]) {
	for x != rb.root && x.getColor() == Black {
		if x == x.parent.getLeft() {
			x = rb.fixAfterDeleteLeft(x)
		} else {
			x = rb.fixAfterDeleteRight(x)
		}
	}
	x.setColor(Black)
}

// fixAfterDeleteLeft rebalances when x is a left child.
func (rb *RBTree[V]) fixAfterDeleteLeft(x *rbNode[V]) *rbNode[V] {
	sib := x.getParent().getRight()
	if sib.getColor() == Red {
		sib.setColor(Black)
		sib.getParent().setColor(Red)
		rb.rotateLeft(x.getParent())
		sib = x.getParent().getRight()
	}
	if sib.getLeft().getColor() == Black && sib.getRight().getColor() == Black {
		sib.setColor(Red)
		x = x.getParent()
	} else {
		if sib.getRight().getColor() == Black {
			sib.getLeft().setColor(Black)
			sib.setColor(Red)
			rb.rotateRight(sib)
			sib = x.getParent().getRight()
		}
		sib.setColor(x.getParent().getColor())
		x.getParent().setColor(Black)
		sib.getRight().setColor(Black)
		rb.rotateLeft(x.getParent())
		x = rb.root
	}
	return x
}

// fixAfterDeleteRight rebalances when x is a right child.
func (rb *RBTree[V]) fixAfterDeleteRight(x *rbNode[V]) *rbNode[V] {
	sib := x.getParent().getLeft()
	if sib.getColor() == Red {
		sib.setColor(Black)
		x.getParent().setColor(Red)
		rb.rotateRight(x.getParent())
		sib = x.getBrother()
	}
	if sib.getRight().getColor() == Black && sib.getLeft().getColor() == Black {
		sib.setColor(Red)
		x = x.getParent()
	} else {
		if sib.getLeft().getColor() == Black {
			sib.getRight().setColor(Black)
			sib.setColor(Red)
			rb.rotateLeft(sib)
			sib = x.getParent().getLeft()
		}
		sib.setColor(x.getParent().getColor())
		x.getParent().setColor(Black)
		sib.getLeft().setColor(Black)
		rb.rotateRight(x.getParent())
		x = rb.root
	}
	return x
}

// rotateLeft performs a left rotation at node.
//
//							  b                    a
//							/	\				  /	  \
//						  c       a  ->    		 b     y
//								 / \            /  \
//		                     	x    y     		c	x

func (rb *RBTree[V]) rotateLeft(node *rbNode[V]) {
	if node == nil || node.getRight() == nil {
		return
	}
	r := node.right
	node.right = r.left
	if r.left != nil {
		r.left.parent = node
	}
	r.parent = node.parent
	if node.parent == nil {
		rb.root = r
	} else if node.parent.left == node {
		node.parent.left = r
	} else {
		node.parent.right = r
	}
	r.left = node
	node.parent = r

}

// rotateRight performs a right rotation at node.
//
//						  b                    c
//						/	\				  /	  \
//					  c       a  ->    		 x     b
//					 /	\	                       / \
//	                 x  y  	     	 	  	       y  a
func (rb *RBTree[V]) rotateRight(node *rbNode[V]) {
	if node == nil || node.getLeft() == nil {
		return
	}
	l := node.left
	node.left = l.right
	if l.right != nil {
		l.right.parent = node
	}
	l.parent = node.parent
	if node.parent == nil {
		rb.root = l
	} else if node.parent.right == node {
		node.parent.right = l
	} else {
		node.parent.left = l
	}
	l.right = node
	node.parent = l

}

func (node *rbNode[V]) getColor() color {
	if node == nil {
		return Black
	}
	return node.color
}

func (node *rbNode[V]) setColor(color color) {
	if node == nil {
		return
	}
	node.color = color
}

func (node *rbNode[V]) getParent() *rbNode[V] {
	if node == nil {
		return nil
	}
	return node.parent
}

func (node *rbNode[V]) getLeft() *rbNode[V] {
	if node == nil {
		return nil
	}
	return node.left
}

func (node *rbNode[V]) getRight() *rbNode[V] {
	if node == nil {
		return nil
	}
	return node.right
}

func (node *rbNode[V]) getUncle() *rbNode[V] {
	if node == nil {
		return nil
	}
	return node.getParent().getBrother()
}
func (node *rbNode[V]) getGrandParent() *rbNode[V] {
	if node == nil {
		return nil
	}
	return node.getParent().getParent()
}
func (node *rbNode[V]) getBrother() *rbNode[V] {
	if node == nil {
		return nil
	}
	if node == node.getParent().getLeft() {
		return node.getParent().getRight()
	}
	return node.getParent().getLeft()
}
