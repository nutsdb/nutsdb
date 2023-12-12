package nutsdb

import (
	"testing"
)

func TestLRUCache(t *testing.T) {
	cache := newLruCache(3)

	// Add some entries to the cache
	cache.add(1, "one")
	cache.add(2, "two")
	cache.add(3, "three")

	// Check if cache length is correct
	if cache.len() != 3 {
		t.Errorf("Expected cache length to be 3, got %d", cache.len())
	}

	// Test getting values from cache
	val := cache.get(1)
	if val != "one" {
		t.Errorf("Expected value for key 1 to be 'one', got %v", val)
	}

	// Test LRU behavior: recently used item should be retained in cache, least recently used should be evicted
	cache.add(4, "four")
	val = cache.get(2) // Should return nil as key 2 is the least recently used item
	if val != nil {
		t.Errorf("Expected value for key 2 to be nil, got %v", val)
	}

	// Test removing an entry from cache
	cache.remove(3)
	if cache.len() != 2 {
		t.Errorf("Expected cache length after removal to be 2, got %d", cache.len())
	}

	// Test clearing the cache
	cache.clear()
	if cache.len() != 0 {
		t.Errorf("Expected cache length after clearing to be 0, got %d", cache.len())
	}
}

func TestLRUCache_RemoveOldest(t *testing.T) {
	cache := newLruCache(2)

	// Add two entries to the cache
	cache.add(1, "one")
	cache.add(2, "two")

	// remove the oldest entry
	cache.removeOldest()

	// Check if cache length is correct
	if cache.len() != 1 {
		t.Errorf("Expected cache length after removing oldest to be 1, got %d", cache.len())
	}

	// Check if the oldest entry has been evicted properly
	val := cache.get(1)
	if val != nil {
		t.Errorf("Expected value for key 1 to be nil after removing oldest, got %v", val)
	}
}
