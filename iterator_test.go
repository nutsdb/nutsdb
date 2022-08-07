package nutsdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIterator_SetNext(t *testing.T) {
	t.Run("setnext_happy", func(t *testing.T) {
		bucket := "bucket_for_iterator"
		withDefaultDB(t, func(t *testing.T, db *DB) {
			{
				tx, err := db.Begin(true)
				assert.NoError(t, err)

				for i := 0; i < 10; i++ {
					key := []byte("key_" + fmt.Sprintf("%07d", i))
					val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
					err = tx.Put(bucket, key, val, Persistent)
					assert.NoError(t, err)
				}
				assert.NoError(t, tx.Commit()) // tx commit

				tx, err = db.Begin(true)
				assert.NoError(t, err)

				it := newIterator(tx, bucket)
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
					err = tx.Put(bucket, key, val, Persistent)
					assert.NoError(t, err)
				}
				assert.NoError(t, tx.Commit())

				tx, err = db.Begin(true)
				assert.NoError(t, err)

				it := newIterator(tx, bucket)
				it.Seek([]byte("key_" + fmt.Sprintf("%07d", 5)))
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
			err = tx.Put(bucket, key, val, Persistent)
			assert.NoError(t, err)

			key = []byte("key_" + fmt.Sprintf("%07d", 1))
			val = []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 1))
			err = tx.Put(bucket, key, val, Persistent)
			assert.NoError(t, err)

			key = []byte("key_" + fmt.Sprintf("%07d", 3))
			val = []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 3))
			err = tx.Put(bucket, key, val, Persistent)
			assert.NoError(t, err)

			assert.NoError(t, tx.Commit())

			tx, err = db.Begin(true)
			assert.NoError(t, err)

			it := newIterator(tx, bucket)
			it.Seek([]byte("key_" + fmt.Sprintf("%07d", 2)))
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
		bucket := "bucket_for_iterator_seek2"
		withDefaultDB(t, func(t *testing.T, db *DB) {
			tx, err := db.Begin(true)
			assert.NoError(t, err)

			key := []byte("key_" + fmt.Sprintf("%07d", 0))
			val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 0))
			err = tx.Put(bucket, key, val, Persistent)
			assert.NoError(t, err)

			key = []byte("key_" + fmt.Sprintf("%07d", 1))
			val = []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 1))
			err = tx.Put(bucket, key, val, Persistent)
			assert.NoError(t, err)

			key = []byte("key_" + fmt.Sprintf("%07d", 3))
			val = []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 3))
			err = tx.Put(bucket, key, val, Persistent)
			assert.NoError(t, err)

			assert.NoError(t, tx.Commit())

			tx, err = db.Begin(true)
			assert.NoError(t, err)

			it := newIterator(tx, bucket)
			it.Seek([]byte("key_" + fmt.Sprintf("%07d", 4)))

			ok, err := it.SetNext()
			assert.Equal(t, false, ok)
			assert.NoError(t, err)

			assert.NoError(t, tx.Commit())
		})
	})
}
