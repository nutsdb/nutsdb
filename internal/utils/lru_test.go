package utils

import (
	"sync"
	"testing"
)

func TestLRUCache(t *testing.T) {
	cache := NewLruCache(3)

	// Add some entries to the cache
	cache.Add(1, "one")
	cache.Add(2, "two")
	cache.Add(3, "three")

	// Check if cache length is correct
	if cache.Len() != 3 {
		t.Errorf("Expected cache length to be 3, got %d", cache.Len())
	}

	// Test getting values from cache
	val := cache.Get(1)
	if val != "one" {
		t.Errorf("Expected value for key 1 to be 'one', got %v", val)
	}

	// Test LRU behavior: recently used item should be retained in cache, least recently used should be evicted
	cache.Add(4, "four")
	val = cache.Get(2) // Should return nil as key 2 is the least recently used item
	if val != nil {
		t.Errorf("Expected value for key 2 to be nil, got %v", val)
	}

	// Test removing an entry from cache
	cache.Remove(3)
	if cache.Len() != 2 {
		t.Errorf("Expected cache length after removal to be 2, got %d", cache.Len())
	}

	// Test clearing the cache
	cache.Clear()
	if cache.Len() != 0 {
		t.Errorf("Expected cache length after clearing to be 0, got %d", cache.Len())
	}
}

func TestLRUCache_RemoveOldest(t *testing.T) {
	cache := NewLruCache(2)

	// Add two entries to the cache
	cache.Add(1, "one")
	cache.Add(2, "two")

	// Remove the oldest entry
	cache.removeOldest()

	// Check if cache length is correct
	if cache.Len() != 1 {
		t.Errorf("Expected cache length after removing oldest to be 1, got %d", cache.Len())
	}

	// Check if the oldest entry has been evicted properly
	val := cache.Get(1)
	if val != nil {
		t.Errorf("Expected value for key 1 to be nil after removing oldest, got %v", val)
	}
}

func TestLRUCache_AddExistingKey(t *testing.T) {
	c := NewLruCache(3)
	c.Add("A", 1)
	c.Add("B", 2)
	c.Add("C", 3)

	c.Add("C", 33)

	if c.l.Len() != len(c.m) {
		t.Fatalf("list/map diverged: list=%d map=%d", c.l.Len(), len(c.m))
	}
	if c.Len() != 3 {
		t.Fatalf("Len()=%d, want 3", c.Len())
	}
	if got := c.Get("A"); got != 1 {
		t.Fatalf("A should stay cached when C is updated, got %v", got)
	}
	if got := c.Get("B"); got != 2 {
		t.Fatalf("B should stay cached when C is updated, got %v", got)
	}
	if got := c.Get("C"); got != 33 {
		t.Fatalf("C should be updated to 33, got %v", got)
	}

	// C was just used, so the next two inserts evict A then B.
	c.Add("D", 4)
	c.Add("E", 5)

	if c.l.Len() != len(c.m) {
		t.Fatalf("list/map diverged after further adds: list=%d map=%d", c.l.Len(), len(c.m))
	}
	if got := c.Get("C"); got != 33 {
		t.Fatalf("C was recently used and should survive, got %v", got)
	}
	if got := c.Get("A"); got != nil {
		t.Fatalf("A should have been evicted, got %v", got)
	}
	if c.Len() != 3 {
		t.Fatalf("Len()=%d, want 3", c.Len())
	}
}

func TestLRUCache_GetOrAdd(t *testing.T) {
	c := NewLruCache(2)

	actual, loaded := c.GetOrAdd("A", 1)
	if loaded || actual != 1 {
		t.Fatalf("first insert: actual=%v loaded=%v", actual, loaded)
	}

	actual, loaded = c.GetOrAdd("A", 99)
	if !loaded || actual != 1 {
		t.Fatalf("existing key should keep the original value: actual=%v loaded=%v", actual, loaded)
	}

	if _, loaded = c.GetOrAdd("B", 2); loaded {
		t.Fatal("B should be inserted")
	}

	actual, loaded = c.GetOrAdd("C", 3)
	if loaded || actual != 3 {
		t.Fatalf("new key at capacity: actual=%v loaded=%v", actual, loaded)
	}
	if got := c.Get("A"); got != nil {
		t.Fatalf("A should have been evicted, got %v", got)
	}
	if got := c.Get("B"); got != 2 {
		t.Fatalf("B should stay cached, got %v", got)
	}
	if c.Len() != 2 || c.l.Len() != len(c.m) {
		t.Fatalf("Len()=%d list=%d map=%d", c.Len(), c.l.Len(), len(c.m))
	}
}

func TestLRUCache_GetOrAddZeroCap(t *testing.T) {
	c := NewLruCache(0)
	actual, loaded := c.GetOrAdd("A", 1)
	if loaded || actual != 1 {
		t.Fatalf("actual=%v loaded=%v", actual, loaded)
	}
	if c.Len() != 0 {
		t.Fatalf("Len()=%d, want 0", c.Len())
	}
}

func TestLRUCache_GetOrAddConcurrent(t *testing.T) {
	c := NewLruCache(8)
	const n = 32
	got := make([]any, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			actual, _ := c.GetOrAdd("k", i)
			got[i] = actual
		}(i)
	}
	close(start)
	wg.Wait()

	if c.Len() != 1 || len(c.m) != 1 {
		t.Fatalf("Len()=%d map=%d, want 1", c.Len(), len(c.m))
	}
	first := got[0]
	for i := 1; i < n; i++ {
		if got[i] != first {
			t.Fatalf("goroutine %d got %v, want %v", i, got[i], first)
		}
	}
	if c.Get("k") != first {
		t.Fatalf("cached value %v, want %v", c.Get("k"), first)
	}
}
