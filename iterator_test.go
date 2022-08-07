package nutsdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIterator(t *testing.T) {
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
}

func TestIterator_Seek(t *testing.T) {
	bucket := "bucket_for_iterator_seek"
	withDefaultDB(t, func(t *testing.T, db *DB) {
		{
			tx, err := db.Begin(true)
			assert.NoError(t, err)

			for i := 0; i < 10; i++ {
				key := []byte("key_" + fmt.Sprintf("%07d", i))
				//fmt.Println(string(key))
				val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
				err = tx.Put(bucket, key, val, Persistent)
				assert.NoError(t, err)
			}
			assert.NoError(t, tx.Commit()) // tx commit

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
}
