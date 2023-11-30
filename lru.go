package nutsdb

import (
	"container/list"
	"sync"
)

// LRUCache is a least recently used (LRU) cache.
type LRUCache struct {
	m   map[interface{}]*list.Element
	l   *list.List
	cap int
	mu  *sync.RWMutex
}

// New creates a new LRUCache with the specified capacity.
func NewLruCache(cap int) *LRUCache {
	return &LRUCache{
		m:   make(map[interface{}]*list.Element),
		l:   list.New(),
		cap: cap,
		mu:  &sync.RWMutex{},
	}
}

// Add adds a new entry to the cache.
func (c *LRUCache) Add(key interface{}, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

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

	c.m[key] = entry
}

// Get returns the entry associated with the given key, or nil if the key is not in the cache.
func (c *LRUCache) Get(key interface{}) interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.m[key]
	if !ok {
		return nil
	}

	c.l.MoveToFront(entry)
	return entry.Value.(*LruEntry).Value
}

// Remove removes the entry associated with the given key from the cache.
func (c *LRUCache) Remove(key interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.m[key]
	if !ok {
		return
	}

	c.l.Remove(entry)
	delete(c.m, key)
}

// Len returns the number of entries in the cache.
func (c *LRUCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.l.Len()
}

// Clear clears the cache.
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.l.Init()
	c.m = make(map[interface{}]*list.Element)
}

// removeOldest removes the oldest entry from the cache.
func (c *LRUCache) removeOldest() {
	entry := c.l.Back()
	if entry == nil {
		return
	}

	key := entry.Value.(*LruEntry).Key
	delete(c.m, key)

	c.l.Remove(entry)
}

// LruEntry is a struct that represents an entry in the LRU cache.
type LruEntry struct {
	Key   interface{}
	Value interface{}
}
