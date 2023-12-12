// Copyright 2023 The nutsdb Author. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nutsdb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

var (
	keyFormat = "key_%03d"
	valFormat = "val_%03d"
)

func runBTreeTest(t *testing.T, test func(t *testing.T, btree *bTree)) {
	btree := newBTree()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf(keyFormat, i))
		val := []byte(fmt.Sprintf(valFormat, i))

		_ = btree.insert(newRecord().withKey(key).withValue(val))
	}

	test(t, btree)
}

func TestBTree_Find(t *testing.T) {
	runBTreeTest(t, func(t *testing.T, btree *bTree) {
		r, ok := btree.find([]byte(fmt.Sprintf(keyFormat, 0)))
		require.Equal(t, []byte(fmt.Sprintf(keyFormat, 0)), r.Key)
		require.True(t, ok)
	})
}

func TestBTree_Delete(t *testing.T) {
	runBTreeTest(t, func(t *testing.T, btree *bTree) {
		require.True(t, btree.delete([]byte(fmt.Sprintf(keyFormat, 0))))
		require.False(t, btree.delete([]byte(fmt.Sprintf(keyFormat, 100))))

		_, ok := btree.find([]byte(fmt.Sprintf(keyFormat, 0)))
		require.False(t, ok)
	})
}

func TestBTree_PrefixScan(t *testing.T) {
	t.Run("prefix scan from beginning", func(t *testing.T) {
		runBTreeTest(t, func(t *testing.T, btree *bTree) {
			limit := 10

			records := btree.prefixScan([]byte("key_"), 0, limit)

			for i, r := range records {
				wantKey := []byte(fmt.Sprintf(keyFormat, i))
				wantValue := []byte(fmt.Sprintf(valFormat, i))

				assert.Equal(t, wantKey, r.Key)
				assert.Equal(t, wantValue, r.Value)
			}
		})
	})

	t.Run("prefix scan for not exists pre key", func(t *testing.T) {
		runBTreeTest(t, func(t *testing.T, btree *bTree) {
			records := btree.prefixScan([]byte("key_xx"), 0, 10)
			assert.Len(t, records, 0)
		})
	})
}

func TestBTree_PrefixSearchScan(t *testing.T) {
	t.Run("prefix search scan right email", func(t *testing.T) {
		runBTreeTest(t, func(t *testing.T, btree *bTree) {

			key := []byte("nutsdb-123456789@outlook.com")
			val := getRandomBytes(24)

			_ = btree.insert(newRecord().withKey(key).withValue(val))

			record, ok := btree.find(key)
			require.True(t, ok)
			require.Equal(t, key, record.Key)

			records := btree.prefixSearchScan([]byte("nutsdb-"),
				"[a-z\\d]+(\\.[a-z\\d]+)*@([\\da-z](-[\\da-z])?)+(\\.{1,2}[a-z]+)+$", 0, 1)
			require.Equal(t, key, records[0].Key)
		})
	})

	t.Run("prefix search scan wrong email", func(t *testing.T) {
		runBTreeTest(t, func(t *testing.T, btree *bTree) {

			key := []byte("nutsdb-123456789@outlook")
			val := getRandomBytes(24)

			_ = btree.insert(newRecord().withKey(key).withValue(val))

			record, ok := btree.find(key)
			require.True(t, ok)
			require.Equal(t, key, record.Key)

			records := btree.prefixSearchScan([]byte("nutsdb-"),
				"[a-z\\d]+(\\.[a-z\\d]+)*@([\\da-z](-[\\da-z])?)+(\\.{1,2}[a-z]+)+$", 0, 1)
			require.Len(t, records, 0)
		})
	})
}

func TestBTree_All(t *testing.T) {
	runBTreeTest(t, func(t *testing.T, btree *bTree) {
		expectRecords := make([]*record, 100)

		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf(keyFormat, i))
			val := []byte(fmt.Sprintf(valFormat, i))

			expectRecords[i] = newRecord().withKey(key).withValue(val)
		}

		require.ElementsMatch(t, expectRecords, btree.all())
	})
}

func TestBTree_Range(t *testing.T) {
	t.Run("btree range at begin", func(t *testing.T) {
		runBTreeTest(t, func(t *testing.T, btree *bTree) {
			expectRecords := make([]*record, 10)

			for i := 0; i < 10; i++ {
				key := []byte(fmt.Sprintf(keyFormat, i))
				val := []byte(fmt.Sprintf(valFormat, i))

				expectRecords[i] = newRecord().withKey(key).withValue(val)
			}

			records := btree.Range([]byte(fmt.Sprintf(keyFormat, 0)), []byte(fmt.Sprintf(keyFormat, 9)))

			require.ElementsMatch(t, records, expectRecords)
		})
	})

	t.Run("btree range at middle", func(t *testing.T) {
		runBTreeTest(t, func(t *testing.T, btree *bTree) {
			expectRecords := make([]*record, 10)

			for i := 40; i < 50; i++ {
				key := []byte(fmt.Sprintf(keyFormat, i))
				val := []byte(fmt.Sprintf(valFormat, i))

				expectRecords[i-40] = newRecord().withKey(key).withValue(val)
			}

			records := btree.Range([]byte(fmt.Sprintf(keyFormat, 40)), []byte(fmt.Sprintf(keyFormat, 49)))

			require.ElementsMatch(t, records, expectRecords)
		})
	})

	t.Run("btree range at end", func(t *testing.T) {
		runBTreeTest(t, func(t *testing.T, btree *bTree) {
			expectRecords := make([]*record, 10)

			for i := 90; i < 100; i++ {
				key := []byte(fmt.Sprintf(keyFormat, i))
				val := []byte(fmt.Sprintf(valFormat, i))

				expectRecords[i-90] = newRecord().withKey(key).withValue(val)
			}

			records := btree.Range([]byte(fmt.Sprintf(keyFormat, 90)), []byte(fmt.Sprintf(keyFormat, 99)))

			require.ElementsMatch(t, records, expectRecords)
		})
	})
}

func TestBTree_Update(t *testing.T) {
	runBTreeTest(t, func(t *testing.T, btree *bTree) {
		for i := 40; i < 50; i++ {
			key := []byte(fmt.Sprintf(keyFormat, i))
			val := []byte(fmt.Sprintf("val_%03d_modify", i))

			btree.insert(newRecord().withKey(key).withValue(val))
		}

		records := btree.Range([]byte(fmt.Sprintf(keyFormat, 40)), []byte(fmt.Sprintf(keyFormat, 49)))

		for i := 40; i < 50; i++ {
			require.Equal(t, []byte(fmt.Sprintf("val_%03d_modify", i)), records[i-40].Value)
		}
	})
}
