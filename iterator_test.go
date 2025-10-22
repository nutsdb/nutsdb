// Copyright 2022 The nutsdb Author. All rights reserved.
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
	"testing"

	"github.com/nutsdb/nutsdb/internal/testutils"
	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 100; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		_ = db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			defer iterator.Release()

			i := 0

			for {
				value, err := iterator.Value()
				require.NoError(t, err)
				require.Equal(t, testutils.GetTestBytes(i), value)
				if !iterator.Next() {
					break
				}
				i++
			}

			return nil
		})
	})
}

func TestIterator_Reverse(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 100; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		_ = db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: true})
			defer iterator.Release()

			i := 99
			for {
				value, err := iterator.Value()
				require.NoError(t, err)
				require.Equal(t, testutils.GetTestBytes(i), value)
				if !iterator.Next() {
					break
				}
				i--
			}

			return nil
		})
	})
}

func TestIterator_Seek(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 100; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		_ = db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: true})
			defer iterator.Release()

			iterator.Seek(testutils.GetTestBytes(40))

			value, err := iterator.Value()
			require.NoError(t, err)
			require.Equal(t, testutils.GetTestBytes(40), value)

			return nil
		})
	})
}

func TestIterator_Release(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 100; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		_ = db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: true})
			defer iterator.Release()
			return nil
		})

		for i := 0; i < 100; i++ {
			txDel(t, db, bucket, testutils.GetTestBytes(i), nil)
		}
	})
}

func TestIterator_Item(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 100; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		_ = db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			defer iterator.Release()

			i := 0
			for iterator.Valid() {
				item := iterator.Item()
				require.NotNil(t, item)
				require.NotNil(t, item.Key)
				require.NotNil(t, item.Record)
				require.Equal(t, testutils.GetTestBytes(i), item.Key)

				i++
				if !iterator.Next() {
					break
				}
			}

			require.Equal(t, 100, i)
			return nil
		})
	})
}

func TestIterator_Rewind(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 50; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		_ = db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			defer iterator.Release()

			// Move to position 10
			for i := 0; i < 10; i++ {
				require.True(t, iterator.Valid())
				iterator.Next()
			}

			// Rewind back to start
			require.True(t, iterator.Rewind())
			require.True(t, iterator.Valid())

			// Verify we're at the first element
			key := iterator.Key()
			require.Equal(t, testutils.GetTestBytes(0), key)

			return nil
		})
	})
}

func TestIterator_Rewind_Reverse(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 50; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		_ = db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: true})
			defer iterator.Release()

			// Move backward 10 positions
			for i := 0; i < 10; i++ {
				require.True(t, iterator.Valid())
				iterator.Next()
			}

			// Rewind back to the last element
			require.True(t, iterator.Rewind())
			require.True(t, iterator.Valid())

			// Verify we're at the last element
			key := iterator.Key()
			require.Equal(t, testutils.GetTestBytes(49), key)

			return nil
		})
	})
}

func TestIterator_EmptyBucket(t *testing.T) {
	bucket := "empty_bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		_ = db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			defer iterator.Release()

			require.False(t, iterator.Valid())
			require.Nil(t, iterator.Key())
			require.Nil(t, iterator.Item())

			_, err := iterator.Value()
			require.Error(t, err)
			require.Equal(t, ErrKeyNotFound, err)

			return nil
		})
	})
}

func TestIterator_NextAfterEnd(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 10; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		_ = db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			defer iterator.Release()

			// Move to the end
			count := 0
			for iterator.Valid() {
				count++
				if !iterator.Next() {
					break
				}
			}
			require.Equal(t, 10, count)

			// Try to move beyond end
			require.False(t, iterator.Valid())
			require.False(t, iterator.Next())
			require.False(t, iterator.Valid())

			// Key and Item should return nil
			require.Nil(t, iterator.Key())
			require.Nil(t, iterator.Item())

			return nil
		})
	})
}

func TestIterator_ValidConsistency(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 10; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		_ = db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			defer iterator.Release()

			// Multiple Valid() calls should return the same result
			for i := 0; i < 10; i++ {
				valid1 := iterator.Valid()
				valid2 := iterator.Valid()
				valid3 := iterator.Valid()
				require.Equal(t, valid1, valid2)
				require.Equal(t, valid2, valid3)

				if !iterator.Next() {
					break
				}
			}

			return nil
		})
	})
}

func TestIterator_KeyValueAfterRelease(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 10; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		_ = db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})

			// Move to first element
			require.True(t, iterator.Valid())

			// Release the iterator
			iterator.Release()

			// After release, should be invalid
			require.False(t, iterator.Valid())
			require.Nil(t, iterator.Key())
			require.Nil(t, iterator.Item())

			_, err := iterator.Value()
			require.Error(t, err)

			return nil
		})
	})
}

func TestIterator_SeekNonExistent(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		// Insert keys 0, 2, 4, 6, 8 (even numbers only)
		for i := 0; i < 10; i += 2 {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		_ = db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			defer iterator.Release()

			// Seek to key 3 (doesn't exist, should position at 4)
			found := iterator.Seek(testutils.GetTestBytes(3))
			require.True(t, found)
			require.True(t, iterator.Valid())

			key := iterator.Key()
			require.Equal(t, testutils.GetTestBytes(4), key)

			return nil
		})
	})
}

func TestIterator_MultipleSeeks(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 100; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		_ = db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			defer iterator.Release()

			// Seek to different positions
			positions := []int{10, 50, 20, 80, 5}
			for _, pos := range positions {
				require.True(t, iterator.Seek(testutils.GetTestBytes(pos)))
				require.True(t, iterator.Valid())
				key := iterator.Key()
				require.Equal(t, testutils.GetTestBytes(pos), key)
			}

			return nil
		})
	})
}

func TestIterator_CachedItemConsistency(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 50; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		_ = db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			defer iterator.Release()

			for iterator.Valid() {
				// Get the same data multiple ways
				key1 := iterator.Key()
				key2 := iterator.Key()
				item := iterator.Item()

				// All should be consistent
				require.Equal(t, key1, key2)
				require.Equal(t, key1, item.Key)

				// Value should match key for this test
				value, err := iterator.Value()
				require.NoError(t, err)
				require.Equal(t, key1, value)

				if !iterator.Next() {
					break
				}
			}

			return nil
		})
	})
}
