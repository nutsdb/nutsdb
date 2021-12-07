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
	"testing"
)

var (
	bucket = "bucket1"
	key    = "key1"
)

func initSAddItems(t *testing.T) {
	initTestDB()
	val1 := []byte("val1_1_1")
	val2 := []byte("val1_1_2")
	val3 := []byte("val1_1_3")
	val4 := []byte("val1_1_4")

	key2 := "key2"
	bucket2 := "bucket2"

	err := testDB.SAdd(bucket, key, val1, val2, val3, val4)
	if err != nil {
		t.Error(err)
	}

	err = testDB.SAdd(bucket, key2, []byte("val1_2_1"), []byte("val1_2_2"))
	if err != nil {
		t.Error(err)
	}

	err = testDB.SAdd(bucket2, key2, []byte("val2_2_1"), []byte("val2_2_2"))
	if err != nil {
		t.Error(err)
	}
}

func TestDB_SAdd(t *testing.T) {
	initSAddItems(t)
	num, err := testDB.SCard(bucket, key)
	if err != nil {
		t.Error(err)
	}
	if num != 4 {
		t.Errorf("expect %d, but get %d", 2, num)
	}
}

func TestDB_SHasKey(t *testing.T) {
	initSAddItems(t)
	isOk, err := testDB.SHasKey(bucket, key)
	if err != nil {
		t.Error(err)
	}
	if !isOk {
		t.Errorf("err SHasKey bucket %s, key %s", bucket, key)
	}
}
func TestDB_SIsMember(t *testing.T) {
	initSAddItems(t)
	val1 := []byte("val1_1_1")
	isMember, err := testDB.SIsMember(bucket, key, val1)
	if err != nil {
		t.Error(err)
	}
	if !isMember {
		t.Error("err SIsMember")
	}
}

func TestDB_SAreMembers(t *testing.T) {
	initSAddItems(t)
	val1 := []byte("val1_1_1")
	val2 := []byte("val1_1_2")
	areMembers, err := testDB.SAreMembers(bucket, key, val1, val2)
	if err != nil {
		t.Error(err)
	}
	if !areMembers {
		t.Error("err SIsMember")
	}
}

func TestDB_SMembers(t *testing.T) {
	initSAddItems(t)
	list, err := testDB.SMembers(bucket, key)
	if err != nil {
		t.Error(err)
	}
	for _, v := range list {
		isMember, err := testDB.SIsMember(bucket, key, v)
		if err != nil {
			t.Error(err)
		}
		if !isMember {
			t.Error("err SIsMember")
		}
	}
}

func TestDB_SDiffByOneBucket(t *testing.T) {
	initTestDB()
	commonVal := []byte("val")
	key1 := "key1"
	key2 := "key2"
	err := testDB.SAdd(bucket, key1, []byte("val1_1_1"), []byte("val1_1_2"), commonVal)
	if err != nil {
		t.Error(err)
	}

	val := []byte("val1_2_1")
	err = testDB.SAdd(bucket, key2, val, commonVal)
	if err != nil {
		t.Error(err)
	}
	list, err := testDB.SDiffByOneBucket(bucket, key2, key1)
	if err != nil {
		t.Error(err)
	}
	for _, v := range list {
		if bytes.Compare(v, val) != 0 {
			t.Errorf("err SDiffByOneBucket, expect %s, but get %s", val, v)
		}
	}
}

func TestDB_SDiffByTwoBuckets(t *testing.T) {
	initTestDB()
	commonVal := []byte("val")
	bucket1 := "bucket1"
	err := testDB.SAdd(bucket1, key, []byte("val1_1_1"), []byte("val1_1_2"), commonVal)
	if err != nil {
		t.Error(err)
	}

	val := []byte("val2_2_1")
	bucket2 := "bucket2"
	err = testDB.SAdd(bucket2, key, val, commonVal)
	if err != nil {
		t.Error(err)
	}

	list, err := testDB.SDiffByTwoBuckets(bucket2, key, bucket1, key)
	if err != nil {
		t.Error(err)
	}
	for _, v := range list {
		if bytes.Compare(v, val) != 0 {
			t.Errorf("err SDiffByOneBucket, expect %s, but get %s", val, v)
		}
	}
}

func TestDB_SMoveByOneBucket(t *testing.T) {
	initTestDB()
	key1 := "key1"
	key2 := "key2"
	val1 := []byte("val1_1_1")
	err := testDB.SAdd(bucket, key1, val1, []byte("val1_1_2"))
	if err != nil {
		t.Error(err)
	}

	val := []byte("val1_2_1")
	err = testDB.SAdd(bucket, key2, val)
	if err != nil {
		t.Error(err)
	}

	isOK, err := testDB.SMoveByOneBucket(bucket, key1, key2, val1)
	if err != nil {
		t.Error(err)
	}
	if !isOK {
		t.Error("err SMoveByOneBucket")
	}
	list, err := testDB.SMembers(bucket, key2)
	if err != nil {
		t.Error(err)
	}
	if len(list) != 2 {
		t.Error("err num")
	}

	list, err = testDB.SMembers(bucket, key)
	if err != nil {
		t.Error(err)
	}
	if len(list) != 1 {
		t.Error("err num")
	}
}

func TestDB_SMoveByTwoBuckets(t *testing.T) {
	initTestDB()
	key1 := "key1"
	key2 := "key2"
	val1 := []byte("val1_1_1")
	bucket1 := "bucket1"

	err := testDB.SAdd(bucket1, key1, val1, []byte("val1_1_2"))
	if err != nil {
		t.Error(err)
	}

	val := []byte("val1_2_1")
	bucket2 := "bucket2"
	err = testDB.SAdd(bucket2, key2, val)
	if err != nil {
		t.Error(err)
	}

	isOK, err := testDB.SMoveByTwoBuckets(bucket1, key1, bucket2, key2, val1)
	if err != nil {
		t.Error(err)
	}
	if !isOK {
		t.Error("err SMoveByOneBucket")
	}
	list, err := testDB.SMembers(bucket2, key2)
	if err != nil {
		t.Error(err)
	}
	if len(list) != 2 {
		t.Error("err num")
	}

	list, err = testDB.SMembers(bucket1, key1)
	if err != nil {
		t.Error(err)
	}
	if len(list) != 1 {
		t.Error("err num")
	}
}

func TestDB_SUnionByOneBucket(t *testing.T) {
	initTestDB()
	key1 := "key1"
	key2 := "key2"
	val1 := []byte("val1_1_1")
	val2 := []byte("val1_1_2")
	bucket1 := "bucket1"

	err := testDB.SAdd(bucket1, key1, val1)
	if err != nil {
		t.Error(err)
	}
	err = testDB.SAdd(bucket1, key2, val2)
	if err != nil {
		t.Error(err)
	}
	list, err := testDB.SUnionByOneBucket(bucket, key1, key2)
	if err != nil {
		t.Error(err)
	}
	if len(list) != 2 {
		t.Error("err num")
	}
}

func TestDB_SUnionByTwoBuckets(t *testing.T) {
	initTestDB()
	key1 := "key1"
	key2 := "key2"
	val1 := []byte("val1_1_1")
	val2 := []byte("val1_1_2")
	bucket1 := "bucket1"

	err := testDB.SAdd(bucket1, key1, val1)
	if err != nil {
		t.Error(err)
	}

	bucket2 := "bucket2"
	err = testDB.SAdd(bucket2, key2, val2)
	if err != nil {
		t.Error(err)
	}

	list, err := testDB.SUnionByTwoBuckets(bucket1, key1, bucket2, key2)
	if err != nil {
		t.Error(err)
	}
	if len(list) != 2 {
		t.Error("err num")
	}
}

func TestDB_SPop(t *testing.T) {
	initTestDB()
	key1 := "key1"
	val1 := []byte("val1_1_1")
	bucket1 := "bucket1"

	err := testDB.SAdd(bucket1, key1, val1)
	if err != nil {
		t.Error(err)
	}

	_, err = testDB.SPop(bucket1, key1)
	if err != nil {
		t.Error(err)
	}

	list, err := testDB.SMembers(bucket1, key1)
	if err != nil {
		t.Error(err)
	}
	if len(list) != 0 {
		t.Errorf("expect %d, but get %d", 0, len(list))
	}
}
