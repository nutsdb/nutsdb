package utils

import (
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
