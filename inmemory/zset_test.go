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
	"errors"
	"testing"

	"github.com/nutsdb/nutsdb"
	"github.com/nutsdb/nutsdb/ds/zset"
	"github.com/stretchr/testify/assert"
)

func TestDB_ZAdd(t *testing.T) {
	initTestDB()
	bucket := "myZSet"
	key1 := "key1"
	val1 := []byte("val1")
	testDB.ZAdd(bucket, key1, 100, val1)

	num, err := testDB.ZCard(bucket)
	if err != nil {
		t.Error(err)
	}
	if num != 1 {
		t.Errorf("expect %d, but get %d", 1, num)
	}
}

func initZAddItems() (bucket, key1, key2, key3 string) {
	initTestDB()
	bucket = "myZSet"
	key1 = "key1"
	key2 = "key2"
	key3 = "key3"

	testDB.ZAdd(bucket, key1, 11, []byte("val1"))
	testDB.ZAdd(bucket, key2, 12, []byte("val2"))
	testDB.ZAdd(bucket, key3, 13, []byte("val3"))
	return
}

func TestDB_ZMembers(t *testing.T) {
	bucket, key1, key2, key3 := initZAddItems()
	_, err := testDB.ZMembers("neBucket")
	assert.New(t).Equal(nutsdb.ErrBucket, err)
	nodes, err := testDB.ZMembers(bucket)
	if err != nil {
		t.Error(err)
	}
	if len(nodes) != 3 {
		t.Error("err ZMembers number")
	}
	if _, ok := nodes[key1]; !ok {
		t.Errorf("ZMembers err, key %s not found", key1)
	}
	if _, ok := nodes[key2]; !ok {
		t.Errorf("ZMembers err, key %s not found", key2)
	}
	if _, ok := nodes[key3]; !ok {
		t.Errorf("ZMembers err, key %s not found", key3)
	}
}

func TestDB_ZCard(t *testing.T) {
	bucket, _, _, _ := initZAddItems()
	tests := []struct {
		bkt string
		num int
		err error
	}{
		{bucket, 3, nil},
		{"neBucket", 0, nutsdb.ErrBucket},
	}
	assertions := assert.New(t)
	for _, test := range tests {
		num, err := testDB.ZCard(test.bkt)
		assertions.Equal(test.num, num)
		assertions.Equal(test.err, err)
	}
}

func TestDB_ZCount(t *testing.T) {
	bucket, _, _, _ := initZAddItems()
	_, err := testDB.ZCount("neBucket", 11, 12, nil)
	assert.New(t).Equal(nutsdb.ErrBucket, err)
	num, err := testDB.ZCount(bucket, 11, 12, nil)
	if err != nil {
		t.Error(err)
	}
	if num != 2 {
		t.Error("err num")
	}
}

func TestDB_ZRangeByScore(t *testing.T) {
	bucket, _, _, _ := initZAddItems()
	nodes, err := testDB.ZRangeByScore(bucket, 11, 12, nil)
	if err != nil {
		t.Error(err)
	}

	expectKeys := make(map[string]float64)
	expectKeys["key1"] = 11
	expectKeys["key2"] = 12
	err = checkExpectKeys(nodes, expectKeys)
	if err != nil {
		t.Error(err)
	}
}

func TestDB_ZRangByRank(t *testing.T) {
	bucket, _, _, _ := initZAddItems()
	_, err := testDB.ZRangeByRank("neBucket", 1, 2)
	assert.New(t).Equal(nutsdb.ErrBucket, err)
	nodes, err := testDB.ZRangeByRank(bucket, 1, 2)
	if err != nil {
		t.Error(err)
	}
	expectKeys := make(map[string]float64)
	expectKeys["key1"] = 11
	expectKeys["key2"] = 12
	err = checkExpectKeys(nodes, expectKeys)
	if err != nil {
		t.Error(err)
	}
}

func TestDB_ZRem(t *testing.T) {
	bucket, key1, _, _ := initZAddItems()
	err := testDB.ZRem("neBucket", key1)
	assert.New(t).Equal(nutsdb.ErrBucket, err)
	err = testDB.ZRem(bucket, key1)
	if err != nil {
		t.Error(err)
	}
	nodes, err := testDB.ZRangeByRank(bucket, 1, 2)
	expectKeys := make(map[string]float64)
	expectKeys["key2"] = 12
	expectKeys["key3"] = 13
	err = checkExpectKeys(nodes, expectKeys)
	if err != nil {
		t.Error(err)
	}
}

func TestDB_ZRemRangeByRank(t *testing.T) {
	bucket, _, _, _ := initZAddItems()
	err := testDB.ZRemRangeByRank("neBucket", 1, 2)
	assert.New(t).Equal(nutsdb.ErrBucket, err)
	err = testDB.ZRemRangeByRank(bucket, 1, 2)
	if err != nil {
		t.Error(err)
	}
	nodes, err := testDB.ZRangeByRank(bucket, 1, 2)
	expectKeys := make(map[string]float64)
	expectKeys["key3"] = 13
	err = checkExpectKeys(nodes, expectKeys)
	if err != nil {
		t.Error(err)
	}
}

func TestDB_ZRank(t *testing.T) {
	bucket, key1, _, _ := initZAddItems()
	_, err := testDB.ZRank("neBucket", key1)
	assert.New(t).Equal(nutsdb.ErrBucket, err)
	rank, err := testDB.ZRank(bucket, key1)
	if err != nil {
		t.Error(err)
	}
	if rank != 1 {
		t.Errorf("err rank expect %d, but %d", 1, rank)
	}
}

func TestDB_ZRevRank(t *testing.T) {
	bucket, key1, _, _ := initZAddItems()
	_, err := testDB.ZRevRank("neBucket", key1)
	assert.New(t).Equal(nutsdb.ErrBucket, err)
	rank, err := testDB.ZRevRank(bucket, key1)
	if err != nil {
		t.Error(err)
	}
	if rank != 3 {
		t.Errorf("err rank expect %d, but %d", 1, rank)
	}
}

func TestDB_ZScore(t *testing.T) {
	bucket, key1, _, _ := initZAddItems()
	_, err := testDB.ZScore("neBucket", key1)
	assert.New(t).Equal(nutsdb.ErrBucket, err)
	score, err := testDB.ZScore(bucket, key1)
	if err != nil {
		t.Error(err)
	}
	if score != 11 {
		t.Errorf("err score")
	}
}

func TestDB_ZGetByKey(t *testing.T) {
	bucket, key1, _, _ := initZAddItems()
	_, err := testDB.ZGetByKey("neBucket", key1)
	assert.New(t).Equal(nutsdb.ErrBucket, err)
	node, err := testDB.ZGetByKey(bucket, key1)
	if err != nil {
		t.Error(err)
	}
	if node.Score() != 11 {
		t.Errorf("err ZGetByKey")
	}
}

func checkExpectKeys(nodes []*zset.SortedSetNode, expectKeys map[string]float64) error {
	for _, n := range nodes {
		if score, ok := expectKeys[n.Key()]; ok {
			if score != float64(n.Score()) {
				return errors.New("err ZRangeByScore")
			}
		}
	}
	return nil
}
