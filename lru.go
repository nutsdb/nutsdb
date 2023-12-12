package nutsdb

import (
	stdList "container/list"
	"sync"
)

// lRUCache is a least recently used (LRU) cache.
type lRUCache struct {
	m   map[interface{}]*stdList.Element
	l   *stdList.List
	cap int
	mu  *sync.RWMutex
}

// newLruCache creates a new lRUCache with the specified capacity.
func newLruCache(cap int) *lRUCache {
	return &lRUCache{
		m:   make(map[interface{}]*stdList.Element),
		l:   stdList.New(),
		cap: cap,
		mu:  &sync.RWMutex{},
	}
}

// add adds a new entry to the cache.
func (c *lRUCache) add(key interface{}, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cap <= 0 {
		return
	}

	if c.l.Len() >= c.cap {
		c.removeOldest()
	}

	e := &lruEntry{
		Key:   key,
		Value: value,
	}
	entry := c.l.PushFront(e)

	c.m[key] = entry
}

// get returns the entry associated with the given key, or nil if the key is not in the cache.
func (c *lRUCache) get(key interface{}) interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.m[key]
	if !ok {
		return nil
	}

	c.l.MoveToFront(entry)
	return entry.Value.(*lruEntry).Value
}

// remove removes the entry associated with the given key from the cache.
func (c *lRUCache) remove(key interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.m[key]
	if !ok {
		return
	}

	c.l.Remove(entry)
	delete(c.m, key)
}

// len returns the number of entries in the cache.
func (c *lRUCache) len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.l.Len()
}

// clear clears the cache.
func (c *lRUCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.l.Init()
	c.m = make(map[interface{}]*stdList.Element)
}

// removeOldest removes the oldest entry from the cache.
func (c *lRUCache) removeOldest() {
	entry := c.l.Back()
	if entry == nil {
		return
	}

	key := entry.Value.(*lruEntry).Key
	delete(c.m, key)

	c.l.Remove(entry)
}

// lruEntry is a struct that represents an entry in the LRU cache.
type lruEntry struct {
	Key   interface{}
	Value interface{}
}
