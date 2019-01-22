package nutsdb

import (
	"testing"

	"github.com/xujiajun/utils/strconv2"
)

func TestPrintSortedMap(t *testing.T) {
	entries := make(map[string]*Entry, 10)
	for i := 0; i < 10; i++ {
		k := strconv2.IntToStr(i)
		entries[k] = &Entry{Key: []byte(k)}
	}

	keys, _ := SortedEntryKeys(entries)

	for i := 0; i < 10; i++ {
		k := strconv2.IntToStr(i)
		if k != keys[i] {
			t.Errorf("err TestPrintSortedMap. got %s want %s", keys[i], k)
		}
	}
}
