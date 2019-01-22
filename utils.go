package nutsdb

import (
	"sort"
)

func SortedEntryKeys(m map[string]*Entry) (keys []string, es map[string]*Entry) {
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys, m
}
