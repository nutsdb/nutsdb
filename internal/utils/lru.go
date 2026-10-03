package utils

import (
	"container/list"
	"sync"
)

// LRUCache is a least recently used (LRU) cache.
type LRUCache struct {
	m   map[any]*list.Element
	l   *list.List
	cap int
	mu  *sync.RWMutex
}

// New creates a new LRUCache with the specified capacity.
func NewLruCache(cap int) *LRUCache {
	return &LRUCache{
		m:   make(map[any]*list.Element),
		l:   list.New(),
		cap: cap,
		mu:  &sync.RWMutex{},
	}
}

// Add inserts key or, when key is already present, replaces its value and
// marks it as most recently used. An update does not evict other entries.
func (c *LRUCache) Add(key any, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cap <= 0 {
		return
	}

	if elem, ok := c.m[key]; ok {
		elem.Value.(*LruEntry).Value = value
		c.l.MoveToFront(elem)
		return
	}

	c.pushFront(key, value)
}

// GetOrAdd returns the cached value when key is already present.
// loaded is then true and the provided value is left unused.
// When key is absent, value is inserted and loaded is false.
// A non-positive capacity stores nothing and returns value with loaded false,
// so the caller keeps the only copy.
func (c *LRUCache) GetOrAdd(key any, value any) (actual any, loaded bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cap <= 0 {
		return value, false
	}

	if elem, ok := c.m[key]; ok {
		c.l.MoveToFront(elem)
		return elem.Value.(*LruEntry).Value, true
	}

	c.pushFront(key, value)
	return value, false
}

// pushFront inserts a new key at the front, evicting the oldest entry when at capacity.
// Caller must hold c.mu, and key must not already be in the cache.
func (c *LRUCache) pushFront(key any, value any) {
	if c.l.Len() >= c.cap {
		c.removeOldest()
	}

	e := &LruEntry{
		Key:   key,
		Value: value,
	}
	c.m[key] = c.l.PushFront(e)
}

// Get returns the entry associated with the given key, or nil if the key is not in the cache.
func (c *LRUCache) Get(key any) any {
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
func (c *LRUCache) Remove(key any) {
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
	c.m = make(map[any]*list.Element)
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
	Key   any
	Value any
}
