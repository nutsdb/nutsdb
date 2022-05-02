// Copyright 2019 The nutsdb Author. All rights reserved.
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
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xujiajun/utils/strconv2"
)

func Init() {
	fileDir := "/tmp/nutsdbtesttx"
	files, _ := ioutil.ReadDir(fileDir)
	for _, f := range files {
		name := f.Name()
		if name != "" {
			err := os.RemoveAll(fileDir + "/" + name)
			if err != nil {
				panic(err)
			}
		}
	}

	opt = DefaultOptions
	opt.Dir = fileDir
	opt.SegmentSize = 8 * 1024
}

func InitForBPTSparseIdxMode() {
	fileDir := "/tmp/nutsdbtesttx2"
	files, _ := ioutil.ReadDir(fileDir)
	for _, f := range files {
		name := f.Name()
		if name != "" {
			err := os.RemoveAll(fileDir + "/" + name)
			if err != nil {
				panic(err)
			}
		}
	}

	opt = DefaultOptions
	opt.Dir = fileDir
	opt.SegmentSize = 1024
	opt.EntryIdxMode = HintBPTSparseIdxMode
	return
}

func TestTx_PutAndGet(t *testing.T) {

	var (
		bucket = "bucket1"
		key    = []byte("key1")
		val    = []byte("val1")
	)

	t.Run("put_and_get", func(t *testing.T) {

		withDefaultDB(t, func(t *testing.T, db *DB) {

			{
				tx, err := db.Begin(true)
				require.NoError(t, err)

				err = tx.Put(bucket, key, val, Persistent)
				assert.NoError(t, err)

				err = tx.Commit()
				assert.NoError(t, err)
			}

			{
				tx, err = db.Begin(false)
				require.NoError(t, err)

				e, err := tx.Get(bucket, key)
				assert.NoError(t, err)
				err = tx.Commit()
				assert.NoError(t, err)

				assert.Equal(t, val, e.Value)
			}

		})
	})

	t.Run("get by closed tx", func(t *testing.T) {

		withDefaultDB(t, func(t *testing.T, db *DB) {

			tx, err := db.Begin(false)
			require.NoError(t, err)
			err = tx.Commit()

			_, err = tx.Get(bucket, key) // use closed tx
			assert.Error(t, err)
		})
	})

}

func TestTx_GetAll(t *testing.T) {
	var (
		bucket = "bucket_for_scanAll"
	)

	t.Run("get_all_from_empty_db", func(t *testing.T) {

		withDefaultDB(t, func(t *testing.T, db *DB) {

			tx, err := db.Begin(false)
			require.NoError(t, err)
			defer tx.Commit()

			_, err = tx.GetAll(bucket)
			assert.Error(t, err)
		})
	})

	t.Run("get_all_from_db", func(t *testing.T) {

		withDefaultDB(t, func(t *testing.T, db *DB) {

			{
				// setup the data

				tx, err := db.Begin(true)
				require.NoError(t, err)

				key0 := []byte("key_" + fmt.Sprintf("%07d", 0))
				val0 := []byte("val" + fmt.Sprintf("%07d", 0))
				err = tx.Put(bucket, key0, val0, Persistent)
				assert.NoError(t, err)

				key1 := []byte("key_" + fmt.Sprintf("%07d", 1))
				val1 := []byte("val" + fmt.Sprintf("%07d", 1))
				err = tx.Put(bucket, key1, val1, Persistent)
				assert.NoError(t, err)

				assert.NoError(t, tx.Commit())
			}

			{
				// check the data

				tx, err = db.Begin(false)
				require.NoError(t, err)

				entries, err := tx.GetAll(bucket)
				assert.NoError(t, err)

				for i, entry := range entries {
					wantKey := []byte("key_" + fmt.Sprintf("%07d", i))
					wantVal := []byte("val" + fmt.Sprintf("%07d", i))

					assert.Equal(t, wantKey, entry.Key)
					assert.Equal(t, wantVal, entry.Value)
				}

				assert.NoError(t, tx.Commit())

			}
		})
	})
}

func TestTx_RangeScan_Err(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {

		bucket := "bucket_for_rangeScan"

		{
			// setup tht data

			tx, err := db.Begin(true)
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				key := []byte("key_" + fmt.Sprintf("%07d", i))
				val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
				err = tx.Put(bucket, key, val, Persistent)
				assert.NoError(t, err)
			}

			assert.NoError(t, tx.Commit()) // tx commit
		}

		{
			// verify

			tx, err := db.Begin(false)
			assert.NoError(t, err)

			start := []byte("key_0010001")
			end := []byte("key_0010010")
			_, err = tx.RangeScan(bucket, start, end)
			assert.Error(t, err)

			assert.NoError(t, tx.Rollback())
		}
	})
}

func TestTx_RangeScan(t *testing.T) {
	bucket := "bucket_for_range"

	withDefaultDB(t, func(t *testing.T, db *DB) {

		{
			// setup tht data

			tx, err := db.Begin(true)
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				key := []byte("key_" + fmt.Sprintf("%07d", i))
				val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
				err = tx.Put(bucket, key, val, Persistent)
				assert.NoError(t, err)
			}

			assert.NoError(t, tx.Commit()) // tx commit
		}

		{

			tx, err = db.Begin(false)
			require.NoError(t, err)

			var (
				s = 5
				e = 8
			)

			start := []byte(fmt.Sprintf("key_%07d", s))
			end := []byte(fmt.Sprintf("key_%07d", e))

			entries, err := tx.RangeScan(bucket, start, end)
			assert.NoError(t, err)
			assert.NoError(t, tx.Commit()) // tx commit

			wantCount := (e - s) + 1 // the range: [5, 8]
			assert.Equal(t, wantCount, len(entries))

			for i, e := range entries {

				wantKey := []byte("key_" + fmt.Sprintf("%07d", i+s))
				assert.Equal(t, wantKey, e.Key)
			}

			entries, err = tx.RangeScan(bucket, start, end)
			assert.Error(t, err)
			assert.Empty(t, entries)
		}

	})

}

func TestTx_PrefixScan(t *testing.T) {

	bucket := "bucket_for_prefix_scan"

	withDefaultDB(t, func(t *testing.T, db *DB) {

		{
			// setup tht data

			tx, err := db.Begin(true)
			require.NoError(t, err)

			for _, prefix := range []string{"key1_", "key2_"} {

				for i := 0; i < 10; i++ {
					key := []byte(prefix + fmt.Sprintf("%07d", i))
					val := []byte("foobar" + fmt.Sprintf("%07d", i))
					err = tx.Put(bucket, key, val, Persistent)
					assert.NoError(t, err)
				}
			}

			assert.NoError(t, tx.Commit()) // tx commit
		}

		{

			tx, err = db.Begin(false)
			require.NoError(t, err)

			var (
				offset = 5
				limit  = 3
			)

			prefix := []byte("key1_")
			entries, _, err := tx.PrefixScan(bucket, prefix, offset, limit)
			assert.NoError(t, err)

			assert.NoError(t, tx.Commit())

			assert.Equal(t, limit, len(entries))

			for i := 0; i < limit; i++ {
				keyIndex := offset + i

				wantKey := []byte("key1_" + fmt.Sprintf("%07d", keyIndex))
				gotEntry := entries[i]
				assert.Equal(t, wantKey, gotEntry.Key)
			}
		}
	})
}

func TestTx_PrefixSearchScan(t *testing.T) {
	bucket := "bucket_for_prefix_search_scan"

	withDefaultDB(t, func(t *testing.T, db *DB) {

		regs := "1"

		tx, err := db.Begin(true)
		require.NoError(t, err)

		key := []byte("key_" + fmt.Sprintf("%07d", 0))
		val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 0))
		err = tx.Put(bucket, key, val, Persistent)
		assert.NoError(t, err)

		tx.Commit() // tx commit

		tx, err = db.Begin(true)
		require.NoError(t, err)

		key = []byte("key_" + fmt.Sprintf("%07d", 1))
		val = []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 1))
		err = tx.Put(bucket, key, val, Persistent)
		assert.NoError(t, err)
		tx.Commit() // tx commit

		tx, err = db.Begin(false)
		require.NoError(t, err)

		prefix := []byte("key_")
		entries, _, err := tx.PrefixSearchScan(bucket, prefix, regs, 0, 1)
		assert.NoError(t, err)

		tx.Commit() // tx commit

		c := 0
		for _, entry := range entries {
			key := []byte("key_" + fmt.Sprintf("%07d", 1))

			assert.Equal(t, key, entry.Key)
			c++
		}

		assert.Equal(t, 1, c)
	})
}

func TestTx_DeleteAndGet(t *testing.T) {

	withDefaultDB(t, func(t *testing.T, db *DB) {

		bucket := "bucket_delete_test"

		{
			tx, err := db.Begin(true)
			require.NoError(t, err)

			for i := 0; i <= 10; i++ {
				key := []byte("key_" + fmt.Sprintf("%07d", i))
				val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
				err := tx.Put(bucket, key, val, Persistent)
				assert.NoError(t, err)
			}

			// tx commit
			tx.Commit()
		}

		{

			tx, err := db.Begin(true)
			require.NoError(t, err)

			for i := 0; i <= 5; i++ {
				key := []byte("key_" + fmt.Sprintf("%07d", i))
				err := tx.Delete(bucket, key)
				assert.NoError(t, err)
			}

			// tx commit
			tx.Commit()

			err = tx.Delete(bucket, []byte("key_"+fmt.Sprintf("%07d", 1)))
			assert.Error(t, err)
		}
	})

}

func TestTx_GetAndScansFromHintKey(t *testing.T) {
	Init()
	opt.EntryIdxMode = HintKeyAndRAMIdxMode
	db, err = Open(opt)
	defer db.Close()

	if err != nil {
		t.Fatal(err)
	}
	// write tx begin
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket_get_test"

	for i := 0; i <= 10; i++ {
		key := []byte("key_" + fmt.Sprintf("%07d", i))
		val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
		if err = tx.Put(bucket, key, val, Persistent); err != nil {
			// tx rollback
			err = tx.Rollback()
			t.Fatal(err)
		}
	}
	// tx commit
	tx.Commit()

	for i := 0; i <= 10; i++ {
		tx, err = db.Begin(false)
		if err != nil {
			t.Fatal(err)
		}
		key := []byte("key_" + fmt.Sprintf("%07d", i))
		if entry, err := tx.Get(bucket, key); err != nil {
			// tx rollback
			tx.Rollback()
		} else {
			val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
			if string(val) != string(entry.Value) {
				t.Error("err read tx ")
			}
		}
		// tx commit
		tx.Commit()
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	start := []byte("key_0000001")
	end := []byte("key_0000010")
	if entries, err := tx.RangeScan(bucket, start, end); err != nil {
		// tx rollback
		tx.Rollback()
	} else {
		j := 0
		for i := 1; i <= 10; i++ {
			key := []byte("key_" + fmt.Sprintf("%07d", i))
			if string(key) != string(entries[j].Key) {
				t.Errorf("err tx RangeScan. got %s want %s", string(entries[j].Key), string(key))
			}
			j++
		}
	}
	// tx commit
	tx.Commit()
}

func TestTx_Put_Err(t *testing.T) {
	Init()
	db, err = Open(opt)
	defer db.Close()

	if err != nil {
		t.Fatal(err)
	}
	// write tx begin err setting here
	tx, err := db.Begin(false) // tx not writable
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket_get_test2"

	key := []byte("key_" + fmt.Sprintf("%07d", 0))
	val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 0))
	if err = tx.Put(bucket, key, val, Persistent); err != nil {
		// tx rollback
		tx.Rollback()
	} else {
		t.Fatal("err TestTx_Put_Err")
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket = "bucket_get_test2"

	key = []byte("") // key cannot be empty
	val = []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", 0))
	if err = tx.Put(bucket, key, val, Persistent); err != nil {
		// tx rollback
		tx.Rollback()
	} else {
		t.Fatal("err TestTx_Put_Err")
	}

	// too big size
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	key = []byte("key_bigone")
	var bigVal string
	for i := 1; i <= 9*1024; i++ {
		bigVal += "val" + strconv2.IntToStr(i)
	}

	tx.Put(bucket, key, []byte(bigVal), Persistent)

	if err = tx.Commit(); err != nil {
		tx.Rollback()
	} else {
		t.Error("err put too big val")
	}

}

func TestTx_PrefixScan_NotFound(t *testing.T) {
	Init()
	db, err = Open(opt)
	defer db.Close()
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	prefix := []byte("key_")
	if entries, _, err := tx.PrefixScan("foobucket", prefix, 0, 10); err != nil {
		// tx rollback
		if entries != nil {
			t.Error("err TestTx_PrefixScan_NotFound")
		}
		tx.Rollback()
	} else {
		t.Error("err TestTx_PrefixScan_NotFound")
	}
	// tx commit
	tx.Commit()

	// write tx begin
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket_prefix_scan_test"

	for i := 0; i <= 10; i++ {
		key := []byte("key_" + fmt.Sprintf("%07d", i))
		val := []byte("val" + fmt.Sprintf("%07d", i))
		if err = tx.Put(bucket, key, val, Persistent); err != nil {
			// tx rollback
			err = tx.Rollback()
			t.Fatal(err)
		}
	}
	// tx commit
	tx.Commit()

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	prefix = []byte("key_foo")
	if entries, _, err := tx.PrefixScan(bucket, prefix, 0, 10); err != nil {
		// tx rollback
		if entries != nil {
			t.Error("err TestTx_PrefixScan_NotFound")
		}
		tx.Rollback()
	} else {
		t.Error("err TestTx_PrefixScan_NotFound")
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	entries, _, err := tx.PrefixScan(bucket, []byte("key_"), 0, 10)
	if len(entries) == 0 || err != nil {
		t.Error("err TestTx_PrefixScan_NotFound")
	}

	tx.Commit()

	entries, _, err = tx.PrefixScan(bucket, []byte("key_"), 0, 10)
	if len(entries) > 0 || err == nil {
		t.Error("err TestTx_PrefixScan_NotFound")
	}
}

func TestTx_PrefixSearchScan_NotFound(t *testing.T) {
	Init()
	db, err = Open(opt)
	defer db.Close()
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	regs := "(.+)"

	prefix := []byte("key_")
	if entries, _, err := tx.PrefixSearchScan("foobucket", prefix, regs, 0, 10); err != nil {
		// tx rollback
		if entries != nil {
			t.Error("err TestTx_PrefixSearchScan_NotFound")
		}
		tx.Rollback()
	} else {
		t.Error("err TestTx_PrefixSearchScan_NotFound")
	}
	// tx commit
	tx.Commit()

	// write tx begin
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket_prefix_search_scan_test"

	for i := 0; i <= 10; i++ {
		key := []byte("key_" + fmt.Sprintf("%07d", i))
		val := []byte("val" + fmt.Sprintf("%07d", i))
		if err = tx.Put(bucket, key, val, Persistent); err != nil {
			// tx rollback
			err = tx.Rollback()
			t.Fatal(err)
		}
	}
	// tx commit
	tx.Commit()

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	prefix = []byte("key_foo")
	if entries, _, err := tx.PrefixSearchScan(bucket, prefix, regs, 0, 10); err != nil {
		// tx rollback
		if entries != nil {
			t.Error("err TestTx_PrefixSearchScan_NotFound")
		}
		tx.Rollback()
	} else {
		t.Error("err TestTx_PrefixSearchScan_NotFound")
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	entries, _, err := tx.PrefixSearchScan(bucket, []byte("key_"), regs, 0, 10)
	if len(entries) == 0 || err != nil {
		t.Error("err TestTx_PrefixSearchScan_NotFound")
	}

	tx.Commit()

	entries, _, err = tx.PrefixSearchScan(bucket, []byte("key_"), regs, 0, 10)
	if len(entries) > 0 || err == nil {
		t.Error("err TestTx_PrefixSearchScan_NotFound")
	}
}

func TestTx_RangeScan_NotFound(t *testing.T) {
	Init()
	db, err = Open(opt)
	defer db.Close()

	// write tx begin
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket_range_scan_test"

	for i := 0; i <= 10; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val" + fmt.Sprintf("%03d", i))
		if err = tx.Put(bucket, key, val, Persistent); err != nil {
			// tx rollback
			err = tx.Rollback()
			t.Fatal(err)
		}
	}
	// tx commit
	tx.Commit()

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	start := []byte("key_011")
	end := []byte("key_012")
	_, err = tx.RangeScan(bucket, start, end)
	if err != nil {
		tx.Rollback()
	} else {
		tx.Commit()
		t.Error("err TestTx_RangeScan_NotFound")
	}
}

func TestTx_Get_SCan_For_BPTSparseIdxMode(t *testing.T) {
	InitForBPTSparseIdxMode()

	db, err = Open(opt)

	if err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket_get_test3"

	for i := 0; i <= 10; i++ {
		key := []byte("key_" + fmt.Sprintf("%07d", i))
		val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
		if err = tx.Put(bucket, key, val, Persistent); err != nil {
			err = tx.Rollback()
			t.Fatal(err)
		}
	}
	tx.Commit()

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i <= 10; i++ {
		key := []byte("key_" + fmt.Sprintf("%07d", i))
		if e, err := tx.Get(bucket, key); err != nil {
			err = tx.Rollback()
			t.Fatal(err)
		} else {

			val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))

			if string(val) != string(e.Value) {
				t.Error("err TestTx_Get")
			}

		}
	}
	tx.Commit()

	db.Close()

	// reopen
	db, err = Open(opt)

	if err != nil {
		t.Fatal(err)
	}

	db.Close()
}

func TestTx_SCan_For_BPTSparseIdxMode(t *testing.T) {
	InitForBPTSparseIdxMode()

	db, err = Open(opt)

	if err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket_get_test4"

	for i := 0; i <= 10; i++ {
		key := []byte("key_" + fmt.Sprintf("%07d", i))
		val := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i))
		if err = tx.Put(bucket, key, val, Persistent); err != nil {
			err = tx.Rollback()
			t.Fatal(err)
		}
	}
	tx.Commit()

	// range scans
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	startKey := []byte("key_" + fmt.Sprintf("%07d", 1))
	endKey := []byte("key_" + fmt.Sprintf("%07d", 9))
	es, err := tx.RangeScan(bucket, startKey, endKey)

	if err != nil {
		t.Error(err)
	}

	for i := 0; i <= 8; i++ {
		expectedVal := []byte("valvalvalvalvalvalvalvalval" + fmt.Sprintf("%07d", i+1))
		if string(es[i].Value) != string(expectedVal) {
			t.Error("err BPTSparseIdxMode RangeScan")
		}
	}

	tx.Commit()

	// prefix scans
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	limit := 5
	es, _, err = tx.PrefixScan(bucket, []byte("key_"), 0, limit)

	if len(es) != limit || err != nil {
		t.Error("err BPTSparseIdxMode prefixScan")
	}
	tx.Commit()

	// prefix search scans

	regs := "(.+)"

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	limit = 5
	es, _, err = tx.PrefixSearchScan(bucket, []byte("key_"), regs, 0, limit)

	if len(es) != limit || err != nil {
		t.Error("err BPTSparseIdxMode prefixSearchScan")
	}
	tx.Commit()

}

func TestTx_Notfound_For_BPTSparseIdxMode(t *testing.T) {
	InitForBPTSparseIdxMode()
	db, err = Open(opt)
	defer db.Close()

	if err != nil {
		t.Fatal(err)
	}

	// get not found
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket_get_test3"

	e, err := tx.Get(bucket, []byte("key_fake"))

	if err == nil || e != nil {
		t.Error("err BPTSparseIdxMode get")
	}

	tx.Commit()

	// prefix scan not found
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	es, _, err := tx.PrefixScan(bucket, []byte("key_prefix_fake"), 0, 10)

	if es != nil || err == nil {
		t.Error("err BPTSparseIdxMode PrefixScan")
	}
	tx.Commit()

	// prefix search scan not found

	regs := "(.+)"

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	es, _, err = tx.PrefixSearchScan(bucket, []byte("key_prefix_fake"), regs, 0, 10)

	if es != nil || err == nil {
		t.Error("err BPTSparseIdxMode PrefixSearchScan")
	}
	tx.Commit()

	// range scan not found
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	es, err = tx.RangeScan(bucket, []byte("key_start_fake"), []byte("key_end_fake"))
	if es != nil || err == nil {
		t.Error("err BPTSparseIdxMode get")
	}
	tx.Commit()
}
