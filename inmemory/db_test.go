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
	"testing"
)

func initTestDB() {
	opts := DefaultOptions
	opts.ShardsCount = 1024
	testDB, _ = Open(opts)
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
		if bytes.Compare(entry.Value, kv.val) != 0 {
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

	start := []byte("key_" + fmt.Sprintf("%07d", 1))
	end := []byte("key_" + fmt.Sprintf("%07d", 100))
	var i int
	i = 1
	err := testDB.Range(bucket, start, end, func(key, value []byte) bool {
		expectKey := []byte("key_" + fmt.Sprintf("%07d", i))
		expectValue := []byte("val_" + fmt.Sprintf("%07d", i))
		if bytes.Compare(expectKey, key) != 0 {
			t.Errorf("expect key %s, but get %s", expectKey, key)
		}
		if bytes.Compare(expectValue, value) != 0 {
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

	if bytes.Compare(val, entry.Value) != 0 {
		t.Errorf("expect val %s, but get val %s", val, entry.Value)
	}

	testDB.Get(bucket, key)
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
	entry, err := testDB.Get("bucket1", []byte("test1"))
	if err == nil || entry != nil {
		t.Error("err testDB Get")
	}
}
