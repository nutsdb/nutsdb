package nutsdb

import (
	"testing"
	"time"
)

// TestIsExpired tests the IsExpired function for different scenarios.
func TestIsExpired(t *testing.T) {
	tests := []struct {
		ttl        uint32
		timestamp  uint64
		wantResult bool
	}{
		{ttl: 0, timestamp: 0, wantResult: false},
		{ttl: 1, timestamp: 0, wantResult: true},
		{ttl: 10, timestamp: 0, wantResult: true},
		{ttl: Persistent, timestamp: 0, wantResult: false},
		{ttl: 1, timestamp: uint64(time.Now().Unix()) + 2, wantResult: false},
		{ttl: 1, timestamp: uint64(time.Now().Unix()) - 2, wantResult: true},
		{ttl: 3, timestamp: uint64(time.Now().Unix()), wantResult: false},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if gotResult := IsExpired(tt.ttl, tt.timestamp); gotResult != tt.wantResult {
				t.Errorf("IsExpired(%v, %v) = %v; want %v", tt.ttl, tt.timestamp, gotResult, tt.wantResult)
			}
		})
	}
}
