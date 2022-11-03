// Copyright 2021 The nutsdb Author. All rights reserved.
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

package inmemory

import (
	"bytes"
	"fmt"
	"github.com/xujiajun/nutsdb/errs"
	"testing"

	"github.com/stretchr/testify/assert"
)

func initTestDB() {
	testDB, _ = Open(
		DefaultOptions,
		WithShardsCount(1024),
	)
}

type TestKV struct {
	key []byte
	val []byte
}

func TestDB_Put_GET(t *testing.T) {
	initTestDB()
	bucket := "bucket1"
	testCases := []TestKV{
		{
			key: []byte("key1"),
			val: []byte("val1"),
		},
		{
			key: []byte("key2"),
			val: []byte("val2"),
		},
		{
			key: []byte("key3"),
			val: []byte("val3"),
		},
	}

	for _, kv := range testCases {
		err := testDB.Put(bucket, kv.key, kv.val, 0)
		if err != nil {
			t.Error(err)
		}
	}

	for _, kv := range testCases {
		entry, err := testDB.Get(bucket, kv.key)
		if err != nil {
			t.Error(err)
		}
		if !bytes.Equal(entry.Value, kv.val) {
			t.Errorf("expect value %s, but get %s", kv.val, entry.Value)
		}
	}
}

func TestDB_Range(t *testing.T) {
	initTestDB()
	bucket := "bucket1"
	for i := 1; i <= 100; i++ {
		key := []byte("key_" + fmt.Sprintf("%07d", i))
		val := []byte("val_" + fmt.Sprintf("%07d", i))
		err := testDB.Put(bucket, key, val, 0)
		if err != nil {
			t.Error(err)
		}
	}
	// delete the first key
	_ = testDB.Delete(bucket, []byte("key_0000001"))

	start := []byte("key_" + fmt.Sprintf("%07d", 1))
	end := []byte("key_" + fmt.Sprintf("%07d", 100))
	i := 2
	err := testDB.Range(bucket, start, end, func(key, value []byte) bool {
		expectKey := []byte("key_" + fmt.Sprintf("%07d", i))
		expectValue := []byte("val_" + fmt.Sprintf("%07d", i))
		if !bytes.Equal(expectKey, key) {
			t.Errorf("expect key %s, but get %s", expectKey, key)
		}
		if !bytes.Equal(expectValue, value) {
			t.Errorf("expect key %s, but get %s", expectValue, value)
		}
		i++
		return true
	})
	if err != nil {
		t.Error(err)
	}
}

func TestDB_Delete(t *testing.T) {
	initTestDB()
	bucket := "bucket1"
	key := []byte("key_" + fmt.Sprintf("%07d", 1))
	val := []byte("val_" + fmt.Sprintf("%07d", 1))
	err := testDB.Put(bucket, key, val, 0)
	if err != nil {
		t.Error(err)
	}

	entry, err := testDB.Get(bucket, key)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(val, entry.Value) {
		t.Errorf("expect val %s, but get val %s", val, entry.Value)
	}

	_, _ = testDB.Get(bucket, key)
	err = testDB.Delete(bucket, key)
	if err != nil {
		t.Error(err)
	}
	entry, err = testDB.Get(bucket, key)
	if entry != nil || err == nil {
		t.Error("err testDB get")
	}
}

func TestDB_Get_ERR(t *testing.T) {
	initTestDB()
	assertions := assert.New(t)
	err := testDB.Put("bucket", []byte("key"), []byte("val"), 0)
	assertions.NoError(err)
	err = testDB.Put("bucket1", []byte("key1"), nil, 0)
	assertions.NoError(err)
	tests := []struct {
		bkt         string
		key         []byte
		wantedError error
	}{
		{"neBucket", []byte("key"), errs.ErrBucket}, // this case should return ErrBucket
		{"bucket", []byte("neKey"), errs.ErrKeyNotFound},
		{"bucket1", []byte("key1"), nil},
	}
	for _, test := range tests {
		_, err := testDB.Get(test.bkt, test.key)
		assertions.Equal(test.wantedError, err)
	}
}

func TestDB_AllKeys(t *testing.T) {
	initTestDB()
	assertions := assert.New(t)

	bucket := "bucket-keys"
	key := []byte("key")
	value := []byte("val")
	keys, err := testDB.AllKeys("bucket-not-exists")
	assertions.NoError(err)
	assertions.Equal(0, len(keys))

	err = testDB.Put(bucket, key, value, 0)
	assertions.NoError(err)
	keys, err = testDB.AllKeys(bucket)
	assertions.NoError(err)
	assertions.Equal(1, len(keys))
	assertions.True(bytes.Equal(key, keys[0]))

	err = testDB.Delete(bucket, key)
	assertions.NoError(err)
	keys, err = testDB.AllKeys(bucket)
	assertions.NoError(err)
	assertions.Equal(0, len(keys))
}

func TestDB_PrefixScan(t *testing.T) {
	initTestDB()
	assertions := assert.New(t)

	bucket := "bucket-keys"
	prefix1 := []byte("key-")
	key1 := []byte("key-1")
	value1 := []byte("val-1")

	prefix2 := []byte("key-2")
	key2a := []byte("key-2a")
	key2b := []byte("key-2b")
	value2a := []byte("val-2a")
	value2b := []byte("val-2b")

	_, _, err := testDB.PrefixScan(bucket, prefix1, 0, 5)
	assertions.Error(err)

	err = testDB.Put(bucket, key1, value1, 0)
	assertions.NoError(err)
	es, _, err := testDB.PrefixScan(bucket, prefix1, 0, 5)
	assertions.NoError(err)
	assertions.Equal(1, len(es))

	err = testDB.Put(bucket, key2a, value2a, 0)
	assertions.NoError(err)
	err = testDB.Put(bucket, key2b, value2b, 0)
	assertions.NoError(err)
	es, _, err = testDB.PrefixScan(bucket, prefix2, 0, 5)
	assertions.NoError(err)
	assertions.Equal(2, len(es))
	assertions.True(bytes.Equal(value2a, es[0].Value))
	assertions.True(bytes.Equal(value2b, es[1].Value))

	es, _, _ = testDB.PrefixScan(bucket, prefix2, 0, 1)
	assertions.Equal(1, len(es))
	assertions.True(bytes.Equal(value2a, es[0].Value))

	_ = testDB.Delete(bucket, key2a)
	es, _, _ = testDB.PrefixScan(bucket, prefix2, 0, -1)
	assertions.Equal(1, len(es))
	assertions.True(bytes.Equal(value2b, es[0].Value))
}
