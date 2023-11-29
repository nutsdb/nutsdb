package nutsdb

import (
	"container/list"
	"sync"
)

// LRUCache is a least recently used (LRU) cache.
type LRUCache struct {
	m   *sync.Map
	l   *list.List
	cap int
}

// New creates a new LRUCache with the specified capacity.
func NewLruCache(cap int) *LRUCache {
	return &LRUCache{
		m:   &sync.Map{},
		l:   list.New(),
		cap: cap,
	}
}

// Add adds a new entry to the cache.
func (c *LRUCache) Add(key interface{}, value interface{}) {
	if c.cap <= 0 {
		return
	}

	if c.l.Len() >= c.cap {
		c.removeOldest()
	}

	e := &LruEntry{
		Key:   key,
		Value: value,
	}
	entry := c.l.PushFront(e)

	c.m.Store(key, entry)
}

// Get returns the entry associated with the given key, or nil if the key is not in the cache.
func (c *LRUCache) Get(key interface{}) interface{} {
	value, ok := c.m.Load(key)
	if !ok {
		return nil
	}

	entry := value.(*list.Element)

	c.l.MoveToFront(entry)
	return entry.Value.(*LruEntry).Value
}

// Remove removes the entry associated with the given key from the cache.
func (c *LRUCache) Remove(key interface{}) {
	value, ok := c.m.Load(key)
	if !ok {
		return
	}

	entry := value.(*list.Element)

	c.l.Remove(entry)
	c.m.Delete(key)
}

// Len returns the number of entries in the cache.
func (c *LRUCache) Len() int {
	return c.l.Len()
}

// Clear clears the cache.
func (c *LRUCache) Clear() {
	c.l.Init()
	c.m = &sync.Map{}
}

// removeOldest removes the oldest entry from the cache.
func (c *LRUCache) removeOldest() {
	entry := c.l.Back()
	if entry == nil {
		return
	}

	key := entry.Value.(*LruEntry).Key
	c.m.Delete(key)

	c.l.Remove(entry)
}

// LruEntry is a struct that represents an entry in the LRU cache.
type LruEntry struct {
	Key   interface{}
	Value interface{}
}
