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
	"fmt"
	"github.com/xujiajun/nutsdb/consts"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIterator_SetNext(t *testing.T) {
	t.Run("setnext_happy_path", func(t *testing.T) {
		bucket := "bucket_for_iterator"
		withDefaultDB(t, func(t *testing.T, db *DB) {
			{
				tx, err := db.Begin(true)
				assert.NoError(t, err)

				for i := 0; i < 10; i++ {
					key := []byte("key_" + fmt.Sprintf("%07d", i))
					val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
					err = tx.Put(bucket, key, val, consts.Persistent)
					assert.NoError(t, err)
				}
				assert.NoError(t, tx.Commit())

				tx, err = db.Begin(false)
				assert.NoError(t, err)

				it := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
				i := 0
				for i < 10 {
					ok, err := it.SetNext()
					assert.Equal(t, true, ok)
					assert.NoError(t, err)

					key := []byte("key_" + fmt.Sprintf("%07d", i))
					val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
					assert.Equal(t, it.Entry().Value, val)
					assert.Equal(t, it.Entry().Key, key)

					i++
				}

				ok, err := it.SetNext()
				assert.Equal(t, false, ok)
				assert.NoError(t, err)

				assert.NoError(t, tx.Commit())
			}
		})
	})
	t.Run("reverse_setnext_happy_path", func(t *testing.T) {
		bucket := "bucket_for_iterator"
		withDefaultDB(t, func(t *testing.T, db *DB) {
			{
				tx, err := db.Begin(true)
				assert.NoError(t, err)

				for i := 0; i < 10; i++ {
					key := []byte("key_" + fmt.Sprintf("%07d", i))
					val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
					err = tx.Put(bucket, key, val, consts.Persistent)
					assert.NoError(t, err)
				}
				assert.NoError(t, tx.Commit())

				tx, err = db.Begin(false)
				assert.NoError(t, err)

				it := NewIterator(tx, bucket, IteratorOptions{Reverse: true})
				i := 9
				for i >= 0 {
					ok, err := it.SetNext()
					assert.Equal(t, true, ok)
					assert.NoError(t, err)

					key := []byte("key_" + fmt.Sprintf("%07d", i))
					val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
					assert.Equal(t, it.Entry().Value, val)
					assert.Equal(t, it.Entry().Key, key)

					i--
				}

				ok, err := it.SetNext()
				assert.Equal(t, false, ok)
				assert.NoError(t, err)

				assert.NoError(t, tx.Commit())
			}
		})
	})
}

func TestIterator_Seek(t *testing.T) {
	t.Run("seek_when_item_is_available", func(t *testing.T) {
		bucket := "bucket_for_iterator_seek1"
		withDefaultDB(t, func(t *testing.T, db *DB) {
			{
				tx, err := db.Begin(true)
				assert.NoError(t, err)

				for i := 0; i < 10; i++ {
					key := []byte("key_" + fmt.Sprintf("%07d", i))
					val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
					err = tx.Put(bucket, key, val, consts.Persistent)
					assert.NoError(t, err)
				}
				assert.NoError(t, tx.Commit())

				tx, err = db.Begin(false)
				assert.NoError(t, err)

				it := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
				err = it.Seek([]byte("key_" + fmt.Sprintf("%07d", 5)))
				assert.NoError(t, err)

				i := 5
				for i < 10 {
					ok, err := it.SetNext()
					assert.Equal(t, true, ok)
					assert.NoError(t, err)

					key := []byte("key_" + fmt.Sprintf("%07d", i))
					val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
					assert.Equal(t, it.Entry().Value, val)
					assert.Equal(t, it.Entry().Key, key)

					i++
				}

				ok, err := it.SetNext()
				assert.Equal(t, false, ok)
				assert.NoError(t, err)

				assert.NoError(t, tx.Commit())
			}
		})
	})
	t.Run("seek_when_item_is_not_available_and_in_items_range", func(t *testing.T) {
		bucket := "bucket_for_iterator_seek2"
		withDefaultDB(t, func(t *testing.T, db *DB) {
			tx, err := db.Begin(true)
			assert.NoError(t, err)

			key := []byte("key_" + fmt.Sprintf("%07d", 0))
			val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 0))
			err = tx.Put(bucket, key, val, consts.Persistent)
			assert.NoError(t, err)

			key = []byte("key_" + fmt.Sprintf("%07d", 1))
			val = []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 1))
			err = tx.Put(bucket, key, val, consts.Persistent)
			assert.NoError(t, err)

			key = []byte("key_" + fmt.Sprintf("%07d", 3))
			val = []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 3))
			err = tx.Put(bucket, key, val, consts.Persistent)
			assert.NoError(t, err)

			assert.NoError(t, tx.Commit())

			tx, err = db.Begin(false)
			assert.NoError(t, err)

			it := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			err = it.Seek([]byte("key_" + fmt.Sprintf("%07d", 2)))
			assert.NoError(t, err)

			i := 3
			for i < 4 {
				ok, err := it.SetNext()
				assert.Equal(t, true, ok)
				assert.NoError(t, err)

				key := []byte("key_" + fmt.Sprintf("%07d", i))
				val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
				assert.Equal(t, it.Entry().Value, val)
				assert.Equal(t, it.Entry().Key, key)

				i++
			}

			ok, err := it.SetNext()
			assert.Equal(t, false, ok)
			assert.NoError(t, err)

			assert.NoError(t, tx.Commit())
		})
	})
	t.Run("seek_when_item_is_not_available_and_greater_than_items_range", func(t *testing.T) {
		bucket := "bucket_for_iterator_seek3"
		withDefaultDB(t, func(t *testing.T, db *DB) {
			tx, err := db.Begin(true)
			assert.NoError(t, err)

			key := []byte("key_" + fmt.Sprintf("%07d", 0))
			val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 0))
			err = tx.Put(bucket, key, val, consts.Persistent)
			assert.NoError(t, err)

			key = []byte("key_" + fmt.Sprintf("%07d", 1))
			val = []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 1))
			err = tx.Put(bucket, key, val, consts.Persistent)
			assert.NoError(t, err)

			key = []byte("key_" + fmt.Sprintf("%07d", 3))
			val = []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 3))
			err = tx.Put(bucket, key, val, consts.Persistent)
			assert.NoError(t, err)

			assert.NoError(t, tx.Commit())

			tx, err = db.Begin(false)
			assert.NoError(t, err)

			it := NewIterator(tx, bucket, IteratorOptions{Reverse: false})
			err = it.Seek([]byte("key_" + fmt.Sprintf("%07d", 4)))
			assert.NoError(t, err)

			ok, err := it.SetNext()
			assert.Equal(t, false, ok)
			assert.NoError(t, err)

			assert.NoError(t, tx.Commit())
		})
	})
}
