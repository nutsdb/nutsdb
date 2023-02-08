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
	"testing"

	"github.com/nutsdb/nutsdb"
	"github.com/nutsdb/nutsdb/ds/list"
	"github.com/stretchr/testify/assert"
)

func initRPushData(bucket, key string) error {
	return testDB.RPush(bucket, key, []byte("a"), []byte("b"), []byte("c"))
}

func initLPushData(bucket, key string) error {
	return testDB.LPush(bucket, key, []byte("a"), []byte("b"), []byte("c"))
}
func TestDB_RPush_RPop(t *testing.T) {
	initTestDB()
	bucket := "bucket1"
	key := "myList1"
	assertions := assert.New(t)
	_, err := testDB.RPop(bucket, key)
	assertions.EqualError(err, list.ErrListNotFound.Error())
	err = initRPushData(bucket, key)
	if err != nil {
		t.Error(err)
	}

	expects := []string{
		"c",
		"b",
		"a",
	}

	for _, expect := range expects {
		item, err := testDB.RPop(bucket, key)
		if err != nil {
			t.Error(err)
		}
		if string(item) != expect {
			t.Errorf("expect %s , but get %s", expect, string(item))
		}
	}
}

func TestDB_RPeek(t *testing.T) {
	initTestDB()
	bucket := "bucket1"
	key := "myList1"
	err := initRPushData(bucket, key)
	if err != nil {
		t.Error(err)
	}

	item, err := testDB.RPeek(bucket, key)
	if err != nil {
		t.Error(err)
	}
	if string(item) != "c" {
		t.Errorf("expect %s , but get %s", "c", string(item))
	}
}

func TestDB_LPush_LPop(t *testing.T) {
	initTestDB()
	bucket := "bucket1"
	key := "myList1"
	assertions := assert.New(t)
	_, err := testDB.LPop(bucket, key)
	assertions.EqualError(err, list.ErrListNotFound.Error())
	err = initLPushData(bucket, key)
	if err != nil {
		t.Error(err)
	}

	expects := []string{
		"c",
		"b",
		"a",
	}

	for _, expect := range expects {
		item, err := testDB.LPop(bucket, key)
		if err != nil {
			t.Error(err)
		}
		if string(item) != expect {
			t.Errorf("expect %s , but get %s", expect, string(item))
		}
	}
}
func TestDB_LPeek(t *testing.T) {
	initTestDB()
	bucket := "bucket1"
	key := "myList1"
	assertions := assert.New(t)
	_, err := testDB.LPeek(bucket, key)
	assertions.EqualError(err, list.ErrListNotFound.Error())
	err = initLPushData(bucket, key)
	if err != nil {
		t.Error(err)
	}

	item, err := testDB.LPeek(bucket, key)
	if err != nil {
		t.Error(err)
	}
	if string(item) != "c" {
		t.Errorf("expect %s , but get %s", "c", string(item))
	}
}

func TestDB_LSize(t *testing.T) {
	initTestDB()
	bucket := "bucket1"
	key := "myList1"
	assertions := assert.New(t)
	_, err := testDB.LSize(bucket, key)
	assertions.EqualError(err, nutsdb.ErrBucket.Error())
	err = initLPushData(bucket, key)
	if err != nil {
		t.Error(err)
	}
	size, err := testDB.LSize(bucket, key)
	if err != nil {
		t.Error(err)
	}
	if size != 3 {
		t.Error("err size")
	}
}

func TestDB_LRange(t *testing.T) {
	initTestDB()
	bucket := "bucket1"
	key := "myList1"
	assertions := assert.New(t)
	_, err := testDB.LRange(bucket, key, 1, 2)
	assertions.EqualError(err, nutsdb.ErrBucket.Error())
	err = initLPushData(bucket, key)
	if err != nil {
		t.Error(err)
	}

	items, err := testDB.LRange(bucket, key, -1, -2)
	if err == nil || len(items) > 0 {
		t.Error("err LRange start or end")
	}
	items, err = testDB.LRange(bucket, key, -2, -1)

	expects := []string{
		"b",
		"a",
	}

	for i, expect := range expects {
		if expect != string(items[i]) {
			t.Error("err testDB.LRange")
		}
	}
}

func TestDB_LRem(t *testing.T) {
	initTestDB()
	bucket := "bucket1"
	key := "myList1"
	assertions := assert.New(t)
	_, err := testDB.LRem(bucket, key, -1, []byte("a"))
	assertions.EqualError(err, nutsdb.ErrBucket.Error())
	err = initLPushData(bucket, key)
	if err != nil {
		t.Error(err)
	}
	_, err = testDB.LRem(bucket, "nonExisted", -1, []byte("a"))
	assertions.EqualError(err, list.ErrListNotFound.Error())
	_, err = testDB.LRem(bucket, key, 1<<63-1, []byte("a"))
	assertions.EqualError(err, list.ErrCount.Error())
	_, err = testDB.LRem(bucket, key, -1<<63, []byte("a"))
	assertions.EqualError(err, list.ErrCount.Error())
	err = testDB.LPush(bucket, key, []byte("a"))
	if err != nil {
		t.Error(err)
	}

	num, err := testDB.LRem(bucket, key, 2, []byte("a"))
	if err != nil {
		t.Error(err)
	}

	if num != 2 {
		t.Errorf("err LRem num, expect %d, but %d", 2, num)
	}
	items, err := testDB.LRange(bucket, key, 0, 3)
	if err != nil {
		t.Error(err)
	}
	expects := []string{
		"c",
		"b",
	}
	for i, expect := range expects {
		if expect != string(items[i]) {
			t.Error("err testDB.LRange")
		}
	}
}

func TestDB_LSet(t *testing.T) {
	initTestDB()
	bucket := "bucket1"
	key := "myList1"
	assertions := assert.New(t)
	err := testDB.LSet(bucket, key, 1, []byte("a"))
	assertions.EqualError(err, nutsdb.ErrBucket.Error())

	err = initLPushData(bucket, key)
	if err != nil {
		t.Error(err)
	}
	err = testDB.LSet(bucket, "nonExisted", 1, []byte("a"))
	assertions.EqualError(err, nutsdb.ErrKeyNotFound.Error())
	err = testDB.LSet(bucket, key, 1<<63-1, []byte("a"))
	assertions.EqualError(err, list.ErrIndexOutOfRange.Error())
	err = testDB.LSet(bucket, key, -1<<63, []byte("a"))
	assertions.EqualError(err, list.ErrIndexOutOfRange.Error())

	err = testDB.LSet(bucket, key, 1, []byte("d"))
	if err != nil {
		t.Error(err)
	}

	items, err := testDB.LRange(bucket, key, 0, 3)
	if err != nil {
		t.Error(err)
	}
	expects := []string{
		"c",
		"d",
		"a",
	}
	for i, expect := range expects {
		if expect != string(items[i]) {
			t.Error("err testDB.LRange")
		}
	}
}

func TestDB_LTrim(t *testing.T) {
	initTestDB()
	bucket := "bucket1"
	key := "myList1"
	assertions := assert.New(t)
	err := testDB.LTrim(bucket, key, 1, 2)
	assertions.EqualError(err, nutsdb.ErrBucket.Error())
	err = initLPushData(bucket, key)
	if err != nil {
		t.Error(err)
	}

	err = testDB.LTrim(bucket, key, 1, 2)
	if err != nil {
		t.Error(err)
	}

	expects := []string{
		"b",
		"a",
	}
	items, err := testDB.LRange(bucket, key, 0, 3)
	if err != nil {
		t.Error(err)
	}
	for i, item := range items {
		if expects[i] != string(item) {
			t.Error("err testDB.LRange")
		}
	}
	size, err := testDB.LSize(bucket, key)
	if err != nil {
		t.Error(err)
	}
	if size != 2 {
		t.Error("err testDB.LSize")
	}
}
