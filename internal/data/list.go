package data

import (
	"errors"
	"time"

	"github.com/nutsdb/nutsdb/internal/utils"
)

var (
	// ErrListNotFound is returned when the list not found.
	ErrListNotFound = errors.New("the list not found")

	// ErrCount is returned when count is error.
	ErrCount = errors.New("err count")

	// ErrEmptyList is returned when the list is empty.
	ErrEmptyList = errors.New("the list is empty")

	// ErrStartOrEnd is returned when start > end
	ErrStartOrEnd = errors.New("start or end error")
)

type ListImplementationType int

const (
	// ListImplDoublyLinkedList uses doubly linked list implementation (default).
	// Advantages: O(1) head/tail operations, lower memory overhead
	// Best for: High-frequency LPush/RPush/LPop/RPop operations
	ListImplDoublyLinkedList = iota

	// ListImplBTree uses BTree implementation.
	// Advantages: O(log n + k) range queries, efficient random access
	// Best for: Frequent range queries or indexed access patterns
	ListImplBTree
)

// HeadTailSeq list head and tail seq num
type HeadTailSeq struct {
	Head uint64
	Tail uint64
}

func (seq *HeadTailSeq) GenerateSeq(isLeft bool) uint64 {
	var res uint64
	if isLeft {
		res = seq.Head
		seq.Head--
	} else {
		res = seq.Tail
		seq.Tail++
	}

	return res
}

// ListStructure defines the interface for List storage implementations.
// It supports multiple implementations: BTree, DoublyLinkedList, SkipList, etc.
// This interface enables users to choose the most suitable implementation based on their use case:
// - DoublyLinkedList: O(1) head/tail operations, optimal for LPush/RPush/LPop/RPop
// - BTree: O(log n) operations, better for range queries and random access
type ListStructure interface {
	// InsertRecord inserts a record with the given key (sequence number in big-endian format).
	// Returns true if an existing record was replaced, false if a new record was inserted.
	InsertRecord(key []byte, record *Record) bool

	// Delete removes the record with the given key.
	// Returns true if the record was found and deleted, false otherwise.
	Delete(key []byte) bool

	// Find retrieves the record with the given key.
	// Returns the record and true if found, nil and false otherwise.
	Find(key []byte) (*Record, bool)

	// Min returns the item with the smallest key (head of the list).
	// Returns the item and true if the list is not empty, nil and false otherwise.
	Min() (*Item[Record], bool)

	// Max returns the item with the largest key (tail of the list).
	// Returns the item and true if the list is not empty, nil and false otherwise.
	Max() (*Item[Record], bool)

	// All returns all records in ascending key order.
	All() []*Record

	// AllItems returns all items (key + record pairs) in ascending key order.
	AllItems() []*Item[Record]

	// Count returns the number of elements in the list.
	Count() int

	// Range returns records with keys in the range [start, end] (inclusive).
	Range(start, end []byte) []*Record

	// PrefixScan scans records with keys matching the given prefix.
	// offset: number of matching records to skip
	// limitNum: maximum number of records to return
	PrefixScan(prefix []byte, offset, limitNum int) []*Record

	// PrefixSearchScan scans records with keys matching the given prefix and regex pattern.
	// The regex is applied to the portion of the key after removing the prefix.
	// offset: number of matching records to skip
	// limitNum: maximum number of records to return
	PrefixSearchScan(prefix []byte, reg string, offset, limitNum int) []*Record

	// PopMin removes and returns the item with the smallest key.
	// Returns the item and true if the list is not empty, nil and false otherwise.
	PopMin() (*Item[Record], bool)

	// PopMax removes and returns the item with the largest key.
	// Returns the item and true if the list is not empty, nil and false otherwise.
	PopMax() (*Item[Record], bool)
}

// Compile-time interface implementation checks
var (
	_ ListStructure = (*BTree)(nil)
	_ ListStructure = (*DoublyLinkedList)(nil)
)

// BTree represents the btree.

// List represents the list.
type List struct {
	Items     map[string]ListStructure
	TTL       map[string]uint32
	TimeStamp map[string]uint64
	Seq       map[string]*HeadTailSeq
	ListImpl  ListImplementationType
}

func NewList(listImpl ListImplementationType) *List {
	return &List{
		Items:     make(map[string]ListStructure),
		TTL:       make(map[string]uint32),
		TimeStamp: make(map[string]uint64),
		Seq:       make(map[string]*HeadTailSeq),
		ListImpl:  listImpl,
	}
}

// CreateListStructure creates a new list storage structure based on configuration.
func (l *List) CreateListStructure() ListStructure {
	switch l.ListImpl {
	case ListImplBTree:
		return NewBTree()
	case ListImplDoublyLinkedList:
		return NewDoublyLinkedList()
	default:
		// Default to DoublyLinkedList for safety
		return NewDoublyLinkedList()
	}
}

func (l *List) LPush(key string, r *Record) error {
	return l.Push(key, r, true)
}

func (l *List) RPush(key string, r *Record) error {
	return l.Push(key, r, false)
}

func (l *List) Push(key string, r *Record, isLeft bool) error {
	// key is seq + user_key
	userKey, curSeq := utils.DecodeListKey([]byte(key))
	userKeyStr := string(userKey)
	if l.IsExpire(userKeyStr) {
		return ErrListNotFound
	}

	list, ok := l.Items[userKeyStr]
	if !ok {
		l.Items[userKeyStr] = l.CreateListStructure()
		list = l.Items[userKeyStr]
	}

	// Initialize seq if not exists
	if _, ok := l.Seq[userKeyStr]; !ok {
		l.Seq[userKeyStr] = &HeadTailSeq{Head: InitialListSeq, Tail: InitialListSeq + 1}
	}

	list.InsertRecord(utils.ConvertUint64ToBigEndianBytes(curSeq), r)

	// Update seq boundaries to track the next insertion positions
	// This is important for recovery scenarios where we rebuild the index
	// Head and Tail should always represent the next available positions for insertion
	seq := l.Seq[userKeyStr]
	if isLeft {
		// LPush: Head should be the next available position on the left
		// If current seq is the actual head, set Head to current seq - 1
		if curSeq <= seq.Head {
			seq.Head = curSeq - 1
		}
	} else {
		// RPush: Tail should be the next available position on the right
		// If current seq is at or beyond current tail, update Tail accordingly
		if curSeq >= seq.Tail {
			seq.Tail = curSeq + 1
		}
	}

	return nil
}

func (l *List) LPop(key string) (*Record, error) {
	if l.IsExpire(key) {
		return nil, ErrListNotFound
	}

	list, ok := l.Items[key]
	if !ok {
		return nil, ErrListNotFound
	}

	// Use PopMin for efficient O(1) head removal
	item, ok := list.PopMin()
	if !ok {
		return nil, ErrEmptyList
	}

	// After LPop, Head should point to the next element's position
	// Note: We don't update Head here because it represents "next push position"
	// The popped element's sequence is already consumed
	return item.Record, nil
}

// RPop removes and returns the last element of the list stored at key.
func (l *List) RPop(key string) (*Record, error) {
	if l.IsExpire(key) {
		return nil, ErrListNotFound
	}

	list, ok := l.Items[key]
	if !ok {
		return nil, ErrListNotFound
	}

	// Use PopMax for efficient O(1) tail removal
	item, ok := list.PopMax()
	if !ok {
		return nil, ErrEmptyList
	}

	// After RPop, Tail should point to the next element's position
	// Note: We don't update Tail here because it represents "next push position"
	// The popped element's sequence is already consumed
	return item.Record, nil
}

func (l *List) LPeek(key string) (*Item[Record], error) {
	return l.peek(key, true)
}

func (l *List) RPeek(key string) (*Item[Record], error) {
	return l.peek(key, false)
}

func (l *List) peek(key string, isLeft bool) (*Item[Record], error) {
	if l.IsExpire(key) {
		return nil, ErrListNotFound
	}
	list, ok := l.Items[key]
	if !ok {
		return nil, ErrListNotFound
	}

	if isLeft {
		item, ok := list.Min()
		if ok {
			return item, nil
		}
	} else {
		item, ok := list.Max()
		if ok {
			return item, nil
		}
	}

	return nil, ErrEmptyList
}

// LRange returns the specified elements of the list stored at key [start,end]
func (l *List) LRange(key string, start, end int) ([]*Record, error) {
	size, err := l.Size(key)
	if err != nil || size == 0 {
		return nil, err
	}

	start, end, err = checkBounds(start, end, size)
	if err != nil {
		return nil, err
	}

	var res []*Record
	allRecords := l.Items[key].All()
	for i, item := range allRecords {
		if i >= start && i <= end {
			res = append(res, item)
		}
	}

	return res, nil
}

// GetRemoveIndexes returns a slice of indices to be removed from the list based on the count
func (l *List) GetRemoveIndexes(key string, count int, cmp func(r *Record) (bool, error)) ([][]byte, error) {
	if l.IsExpire(key) {
		return nil, ErrListNotFound
	}

	list, ok := l.Items[key]

	if !ok {
		return nil, ErrListNotFound
	}

	var res [][]byte
	var allItems []*Item[Record]
	if count == 0 {
		count = list.Count()
	}

	allItems = l.Items[key].AllItems()
	if count > 0 {
		for _, item := range allItems {
			if count <= 0 {
				break
			}
			r := item.Record
			ok, err := cmp(r)
			if err != nil {
				return nil, err
			}
			if ok {
				res = append(res, item.Key)
				count--
			}
		}
	} else {
		for i := len(allItems) - 1; i >= 0; i-- {
			if count >= 0 {
				break
			}
			r := allItems[i].Record
			ok, err := cmp(r)
			if err != nil {
				return nil, err
			}
			if ok {
				res = append(res, allItems[i].Key)
				count++
			}
		}
	}

	return res, nil
}

// LRem removes the first count occurrences of elements equal to value from the list stored at key.
// The count argument influences the operation in the following ways:
// count > 0: Remove elements equal to value moving from head to tail.
// count < 0: Remove elements equal to value moving from tail to head.
// count = 0: Remove all elements equal to value.
func (l *List) LRem(key string, count int, cmp func(r *Record) (bool, error)) error {
	removeIndexes, err := l.GetRemoveIndexes(key, count, cmp)
	if err != nil {
		return err
	}

	list := l.Items[key]
	for _, idx := range removeIndexes {
		list.Delete(idx)
	}

	return nil
}

// LTrim trim an existing list so that it will contain only the specified range of elements specified.
func (l *List) LTrim(key string, start, end int) error {
	if l.IsExpire(key) {
		return ErrListNotFound
	}
	if _, ok := l.Items[key]; !ok {
		return ErrListNotFound
	}

	list := l.Items[key]
	allItems := list.AllItems()
	for i, item := range allItems {
		if i < start || i > end {
			list.Delete(item.Key)
		}
	}

	return nil
}

// LRemByIndex remove the list element at specified index
func (l *List) LRemByIndex(key string, indexes []int) error {
	if l.IsExpire(key) {
		return ErrListNotFound
	}

	idxes := l.GetValidIndexes(key, indexes)
	if len(idxes) == 0 {
		return nil
	}

	list := l.Items[key]
	allItems := list.AllItems()
	for i, item := range allItems {
		if _, ok := idxes[i]; ok {
			list.Delete(item.Key)
		}
	}

	return nil
}

func (l *List) GetValidIndexes(key string, indexes []int) map[int]struct{} {
	idxes := make(map[int]struct{})
	listLen, err := l.Size(key)
	if err != nil || listLen == 0 {
		return idxes
	}

	for _, idx := range indexes {
		if idx < 0 || idx >= listLen {
			continue
		}
		idxes[idx] = struct{}{}
	}

	return idxes
}

func (l *List) IsExpire(key string) bool {
	if l == nil {
		return false
	}

	_, ok := l.TTL[key]
	if !ok {
		return false
	}

	now := time.Now().Unix()
	timestamp := l.TimeStamp[key]
	if l.TTL[key] > 0 && uint64(l.TTL[key])+timestamp > uint64(now) || l.TTL[key] == uint32(0) {
		return false
	}

	delete(l.Items, key)
	delete(l.TTL, key)
	delete(l.TimeStamp, key)
	delete(l.Seq, key)

	return true
}

func (l *List) Size(key string) (int, error) {
	if l.IsExpire(key) {
		return 0, ErrListNotFound
	}
	if _, ok := l.Items[key]; !ok {
		return 0, ErrListNotFound
	}

	return l.Items[key].Count(), nil
}

func (l *List) IsEmpty(key string) (bool, error) {
	size, err := l.Size(key)
	if err != nil || size > 0 {
		return false, err
	}
	return true, nil
}

func (l *List) GetListTTL(key string) (uint32, error) {
	if l.IsExpire(key) {
		return 0, ErrListNotFound
	}

	ttl := l.TTL[key]
	timestamp := l.TimeStamp[key]
	if ttl == 0 || timestamp == 0 {
		return 0, nil
	}

	now := time.Now().Unix()
	remain := timestamp + uint64(ttl) - uint64(now)

	return uint32(remain), nil
}

func (l *List) ExpireList(key []byte, ttl uint32) {
	l.TTL[string(key)] = ttl
	l.TimeStamp[string(key)] = uint64(time.Now().Unix())
}

func (l *List) GeneratePushKey(key []byte, isLeft bool) []byte {
	// 获取或创建HeadTailSeq
	keyStr := string(key)
	seq, ok := l.Seq[keyStr]
	if !ok {
		// 如果不存在，先尝试从现有项推断
		if items, exists := l.Items[keyStr]; exists && items.Count() > 0 {
			minSeq, okMinSeq := items.Min()
			maxSeq, okMaxSeq := items.Max()
			if !okMinSeq || !okMaxSeq {
				seq = &HeadTailSeq{Head: InitialListSeq, Tail: InitialListSeq + 1}
			} else {
				seq = &HeadTailSeq{
					Head: utils.ConvertBigEndianBytesToUint64(minSeq.Key) - 1,
					Tail: utils.ConvertBigEndianBytesToUint64(maxSeq.Key) + 1,
				}
			}
		} else {
			seq = &HeadTailSeq{Head: InitialListSeq, Tail: InitialListSeq + 1}
		}
		l.Seq[keyStr] = seq
	}

	seqValue := seq.GenerateSeq(isLeft)
	return utils.EncodeListKey(key, seqValue)
}

func checkBounds(start, end int, size int) (int, int, error) {
	if start >= 0 && end < 0 {
		end = size + end
	}

	if start < 0 && end > 0 {
		start = size + start
	}

	if start < 0 && end < 0 {
		start, end = size+start, size+end
	}

	if end >= size {
		end = size - 1
	}

	if start > end {
		return 0, 0, ErrStartOrEnd
	}

	return start, end, nil
}
