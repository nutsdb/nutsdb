package inmemory

import (
	"github.com/cespare/xxhash/v2"
)

// Hasher is responsible for generating unsigned, 64 bit hash of provided string.
type Hasher interface {
	Sum64(string) uint64
}

func newDefaultHasher() Hasher {
	return xxhasher{}
}

type xxhasher struct{}

// Sum64 gets the string and returns its uint64 hash value.
func (h xxhasher) Sum64(key string) uint64 {
	return xxhash.Sum64String(key)
}
