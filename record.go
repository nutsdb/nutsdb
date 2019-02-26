package nutsdb

import "time"

// Record records entry and hint.
type Record struct {
	H *Hint
	E *Entry
}

// IsExpired returns the record if expired or not.
func (r *Record) IsExpired() bool {
	return IsExpired(r.H.meta.TTL, r.H.meta.timestamp)
}

// IsExpired checks the ttl if expired or not.
func IsExpired(ttl uint32, timestamp uint64) bool {
	now := time.Now().Unix()
	if ttl > 0 && uint64(ttl)+timestamp > uint64(now) || ttl == Persistent {
		return false
	}

	return true
}

// UpdateRecord updates the record.
func (r *Record) UpdateRecord(h *Hint, e *Entry) error {
	r.E = e
	r.H = h

	return nil
}
