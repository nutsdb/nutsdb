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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

var tx *Tx

func InitForZSet() {
	fileDir := "/tmp/nutsdbtestzsettx"
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
	return
}

func TestTx_ZAdd(t *testing.T) {
	InitForZSet()
	db, err = Open(opt)
	tx, err := db.Begin(true)
	defer func(db *DB) {
		err := db.Close()
		assert.NoError(t, err)
	}(db)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "myZSet"

	if err := tx.ZAdd(bucket, []byte("key1"), 100, []byte("val1")); err != nil {
		assert.NoError(t, tx.Rollback())
		t.Fatal(err)
	} else {
		err := tx.ZAdd(bucket, []byte("key1"+SeparatorForZSetKey), 100, []byte("val1"))
		if err == nil {
			assert.NoError(t, tx.Rollback())
			t.Fatal("TestTx_ZAdd err")
		}
		assert.NoError(t, tx.Commit())
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	num, err := tx.ZCard(bucket)
	if num != 1 || err != nil {
		t.Error("TestTx_ZAdd err")
	}

	assert.NoError(t, tx.Commit())
}

func InitDataForZSet(t *testing.T) (bucket, key1, key2, key3 string) {
	InitForZSet()
	db, err = Open(opt)
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	bucket = "myZSet"
	key1 = "key1"
	key2 = "key2"
	key3 = "key3"
	if err := tx.ZAdd(bucket, []byte(key1), 79, []byte("val1")); err != nil {
		assert.NoError(t, tx.Rollback())
		t.Fatal(err)
	}
	if err := tx.ZAdd(bucket, []byte(key2), 98, []byte("val2")); err != nil {
		assert.NoError(t, tx.Rollback())
		t.Fatal(err)
	}
	if err := tx.ZAdd(bucket, []byte(key3), 99, []byte("val3")); err != nil {
		assert.NoError(t, tx.Rollback())
		t.Fatal(err)
	}

	assert.NoError(t, tx.Commit())

	return
}

func TestTx_ZMembers(t *testing.T) {
	bucket, key1, key2, key3 := InitDataForZSet(t)
	assertions := assert.New(t)
	tx, err = db.Begin(false)
	defer func(db *DB) {
		err := db.Close()
		assert.NoError(t, err)
	}(db)
	if err != nil {
		t.Fatal(err)
	}

	if nodes, err := tx.ZMembers(bucket); err != nil {
		assert.NoError(t, tx.Rollback())
		t.Fatal(err)
	} else {
		assert.NoError(t, tx.Commit())

		assertions.Len(nodes, 3, "TestTx_ZMembers err")

		_, exist := nodes[key1]
		assertions.True(exist, "TestTx_ZMembers err")
		_, exist = nodes[key2]
		assertions.True(exist, "TestTx_ZMembers err")
		_, exist = nodes[key3]
		assertions.True(exist, "TestTx_ZMembers err")
	}
	_, err := tx.ZMembers(bucket)

	assertions.Error(err, "TestTx_ZMembers err")
}

func TestTx_ZCard(t *testing.T) {
	bucket, _, _, _ := InitDataForZSet(t)
	assertions := assert.New(t)
	tx, err = db.Begin(false)
	defer func(db *DB) {
		err := db.Close()
		assert.NoError(t, err)
	}(db)
	if err != nil {
		t.Fatal(err)
	}

	if num, err := tx.ZCard(bucket); err != nil {
		fmt.Println("err", err)
		assert.NoError(t, tx.Rollback())
		t.Fatal("TestTx_ZCard err")
	} else {
		assertions.Equal(3, num, "TestTx_ZCard err")
	}

	assert.NoError(t, tx.Commit())
	_, err := tx.ZCard(bucket)
	assertions.Error(err)
}

func TestTx_ZCount(t *testing.T) {
	bucket, _, _, _ := InitDataForZSet(t)

	tx, err = db.Begin(false)
	defer func(db *DB) {
		err := db.Close()
		assert.NoError(t, err)
	}(db)

	require.NoError(t, err)

	num, err := tx.ZCount(bucket, 90, 100, nil)
	if err != nil {
		assert.NoError(t, tx.Rollback())
		t.Fatal(err)
	} else {
		if num != 2 {
			assert.NoError(t, tx.Rollback())
			t.Fatal("TestTx_ZCount err")
		}

		num, err := tx.ZCount("bucket_fake", 90, 100, nil)
		if err == nil || num != 0 {
			assert.NoError(t, tx.Rollback())
			t.Fatal("TestTx_ZCount err")
		}

		assert.NoError(t, tx.Commit())
	}

	num, err = tx.ZCount(bucket, 90, 100, nil)
	assert.Error(t, err, "TestTx_ZCount err")
	assert.Equal(t, num, 0, "TestTx_ZCount err")
}

func TestTx_ZPopMax(t *testing.T) {
	bucket, _, _, _ := InitDataForZSet(t)
	tx, err = db.Begin(true)
	defer func(db *DB) {
		err := db.Close()
		assert.NoError(t, err)
	}(db)

	require.NoError(t, err)

	node, err := tx.ZPopMax(bucket)
	if err != nil {
		assert.NoError(t, tx.Rollback())
		t.Fatal(err)
	}

	if node.Key() != "key3" || node.Score() != 99 {
		assert.NoError(t, tx.Rollback())
		t.Fatal("TestTx_ZPopMax err")
	} else {
		_, err := tx.ZPopMax("bucket_fake")
		if err == nil {
			assert.NoError(t, tx.Rollback())
			t.Fatal("TestTx_ZPopMax err")
		}
		assert.NoError(t, tx.Commit())
	}

	node, err = tx.ZPopMax(bucket)

	require.Error(t, err, "TestTx_ZPopMax err")
}

func TestTx_ZPopMin(t *testing.T) {
	bucket, _, _, _ := InitDataForZSet(t)
	tx, err = db.Begin(true)
	defer func(db *DB) {
		err := db.Close()
		assert.NoError(t, err)
	}(db)

	require.NoError(t, err)

	node, err := tx.ZPopMin(bucket)
	if err != nil {
		assert.NoError(t, tx.Rollback())
		t.Fatal(err)
	}

	if node.Key() != "key1" || node.Score() != 79 {
		assert.NoError(t, tx.Rollback())
		t.Fatal("TestTx_ZPopMin err")
	} else {
		_, err := tx.ZPopMin("bucket_fake")
		if err == nil {
			assert.NoError(t, tx.Rollback())
			t.Fatal("TestTx_ZPopMin err")
		}
		assert.NoError(t, tx.Commit())
	}

	node, err = tx.ZPopMin(bucket)
	require.Error(t, err, "TestTx_ZPopMin err")
}

func TestTx_ZPickMax(t *testing.T) {
	bucket, _, _, _ := InitDataForZSet(t)
	tx, err = db.Begin(false)
	defer func(db *DB) {
		err := db.Close()
		assert.NoError(t, err)
	}(db)

	require.NoError(t, err)

	node, err := tx.ZPeekMax("bucket_fake")

	assert.Error(t, err, "TestTx_ZPickMax err")
	assert.Nil(t, node, "TestTx_ZPickMax err")

	node, err = tx.ZPeekMax(bucket)
	if err != nil {
		assert.NoError(t, tx.Rollback())
		t.Fatal(err)
	} else {
		assert.NoError(t, tx.Commit())

		assert.Equal(t, "key3", node.Key(), "TestTx_ZPickMax err")

		node, err = tx.ZPeekMax(bucket)
		assert.Error(t, err, "TestTx_ZPickMax err")
		assert.Nil(t, node, "TestTx_ZPickMax err")
	}
}

func TestTx_ZPickMin(t *testing.T) {
	bucket, _, _, _ := InitDataForZSet(t)
	tx, err = db.Begin(false)
	defer func(db *DB) {
		err := db.Close()
		assert.NoError(t, err)
	}(db)
	if err != nil {
		t.Fatal(err)
	}

	node, err := tx.ZPeekMin("bucket_fake")
	if err == nil || node != nil {
		t.Error("TestTx_ZPickMin err")
	}

	node, err = tx.ZPeekMin(bucket)
	if err != nil {
		assert.NoError(t, tx.Rollback())
		t.Fatal(err)
	} else {
		assert.NoError(t, tx.Commit())

		if node.Key() != "key1" {
			t.Error("TestTx_ZPickMin err")
		}

		node, err = tx.ZPeekMin(bucket)
		if err == nil || node != nil {
			t.Error("TestTx_ZPickMin err")
		}
	}
}

func TestTx_ZRangByRank(t *testing.T) {
	bucket, key1, key2, _ := InitDataForZSet(t)
	tx, err = db.Begin(false)
	defer func(db *DB) {
		err := db.Close()
		assert.NoError(t, err)
	}(db)
	require.NoError(t, err)

	var expectResult map[string]struct{}
	expectResult = make(map[string]struct{})
	expectResult[key1] = struct{}{}
	expectResult[key2] = struct{}{}

	nodes, err := tx.ZRangeByRank("bucket_fake", 1, 2)

	assert.Error(t, err, "TestTx_ZRangByRank err")
	assert.Nil(t, nodes, "TestTx_ZRangByRank err")

	nodes, err = tx.ZRangeByRank(bucket, 1, 2)
	if err != nil {
		assert.NoError(t, tx.Rollback())
		t.Fatal(err)
	} else {
		assert.NoError(t, tx.Commit())

		assert.Len(t, nodes, 2, "TestTx_ZRangByRank err")

		for _, node := range nodes {
			_, exist := expectResult[node.Key()]
			assert.True(t, exist, "TestTx_ZRangByRank err")
		}

		nodes, err = tx.ZRangeByRank(bucket, 1, 3)
		assert.Error(t, err, "TestTx_ZRangByRank err")
		assert.Nil(t, nodes, "TestTx_ZRangByRank err")
	}
}

func TestTx_ZRem(t *testing.T) {
	bucket, key1, key2, _ := InitDataForZSet(t)
	assertions := assert.New(t)
	tx, err = db.Begin(true)
	defer func(db *DB) {
		err := db.Close()
		assert.NoError(t, err)
	}(db)

	require.NoError(t, err)

	err := tx.ZRem("bucket_fake", key1)

	assertions.Error(err, "TestTx_ZRem err")

	err = tx.ZRem(bucket, key1)
	assertions.NoError(err, "TestTx_ZRem err")

	err = tx.ZRem(bucket, key1)

	assertions.NoError(err, "TestTx_ZRem err")

	assert.NoError(t, tx.Commit())
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, err)

	n, err := tx.ZGetByKey(bucket, []byte(key1))

	assertions.Nil(n, "TestTx_ZRem err")
	assertions.Error(err, "TestTx_ZRem err")

	n, err = tx.ZGetByKey(bucket, []byte(key2))

	assertions.NotNil(n, "TestTx_ZRem err")
	assertions.NoError(err, "TestTx_ZRem err")

	assert.NoError(t, tx.Commit())

	err = tx.ZRem(bucket, "key2")

	assertions.Error(err, "TestTx_ZRem err")
}

func TestTx_ZRemRangeByRank(t *testing.T) {
	bucket, key1, key2, key3 := InitDataForZSet(t)
	assertions := assert.New(t)
	tx, err = db.Begin(true)
	defer func(db *DB) {
		err := db.Close()
		assert.NoError(t, err)
	}(db)

	require.NoError(t, err)

	err := tx.ZRemRangeByRank("bucket_fake", 1, 2)

	assert.Error(t, err, "TestTx_ZRemRangeByRank err")

	err = tx.ZRemRangeByRank(bucket, 1, 2)

	assertions.NoError(err, "TestTx_ZRemRangeByRank err")

	assert.NoError(t, tx.Commit())

	tx, err = db.Begin(false)

	require.NoError(t, err)
	dict, err := tx.ZMembers(bucket)

	assertions.NoError(err, "TestTx_ZRemRangeByRank err")
	_, ok := dict[key3]
	assertions.True(ok, "TestTx_ZRemRangeByRank err")
	_, ok = dict[key2]
	assertions.False(ok, "TestTx_ZRemRangeByRank err")
	_, ok = dict[key1]
	assertions.False(ok, "TestTx_ZRemRangeByRank err")

	assertions.NoError(tx.Commit())

	tx, err = db.Begin(true)

	require.NoError(t, err)
	err = tx.ZRemRangeByRank(bucket, 1, 2)

	assertions.NoError(err, "TestTx_ZRemRangeByRank err")
	assertions.NoError(tx.Commit())

	tx, err = db.Begin(false)

	require.NoError(t, err)
	items, err := tx.ZMembers(bucket)
	assertions.NoError(err, "TestTx_ZRemRangeByRank err")
	assertions.Len(items, 0, "TestTx_ZRemRangeByRank err")

	assertions.NoError(tx.Commit())

	err = tx.ZRemRangeByRank(bucket, 1, 2)
	assertions.Error(err, "TestTx_ZRemRangeByRank err")
}

func TestTx_ZRank(t *testing.T) {
	bucket, key1, key2, key3 := InitDataForZSet(t)
	assertions := assert.New(t)
	tx, err = db.Begin(false)
	defer func(db *DB) {
		err := db.Close()
		assert.NoError(t, err)
	}(db)

	require.NoError(t, err)

	rank, err := tx.ZRank("bucket_fake", []byte(key1))
	assertions.Error(err, "TestTx_ZRank err")
	assertions.Equal(0, rank)

	rank, err = tx.ZRank(bucket, []byte(key1))
	assertions.NoError(err, "TestTx_ZRank err")
	assertions.Equal(1, rank, "TestTx_ZRank err")

	rank, err = tx.ZRank(bucket, []byte(key2))
	assertions.NoError(err, "TestTx_ZRank err")
	assertions.Equal(2, rank, "TestTx_ZRank err")

	rank, err = tx.ZRank(bucket, []byte(key3))
	assertions.NoError(err, "TestTx_ZRank err")
	assertions.Equal(3, rank, "TestTx_ZRank err")

	assertions.NoError(tx.Commit())

	rank, err = tx.ZRank(bucket, []byte(key3))

	assertions.Error(err, "TestTx_ZRank err")
	assertions.Equal(0, rank, "TestTx_ZRank err")
}

func TestTx_ZRevRank(t *testing.T) {
	bucket, key1, key2, key3 := InitDataForZSet(t)
	assertions := assert.New(t)
	tx, err = db.Begin(false)
	defer func(db *DB) {
		err := db.Close()
		assert.NoError(t, err)
	}(db)

	require.NoError(t, err)

	rank, err := tx.ZRevRank("bucket_fake", []byte(key1))
	assertions.Error(err, "TestTx_ZRevRank err")
	assertions.Equal(0, rank, "TestTx_ZRevRank err")

	rank, err = tx.ZRevRank(bucket, []byte(key1))
	assertions.NoError(err, "TestTx_ZRevRank err")
	assertions.Equal(3, rank, "TestTx_ZRevRank err")

	rank, err = tx.ZRevRank(bucket, []byte(key2))

	assertions.NoError(err, "TestTx_ZRevRank err")
	assertions.Equal(2, rank, "TestTx_ZRevRank err")
	rank, err = tx.ZRevRank(bucket, []byte(key3))
	assertions.NoError(err, "TestTx_ZRevRank err")
	assertions.Equal(1, rank, "TestTx_ZRevRank err")

	assertions.NoError(tx.Commit())

	rank, err = tx.ZRevRank(bucket, []byte(key3))

	assertions.Error(err, "TestTx_ZRevRank err")
	assertions.Equal(0, rank, "TestTx_ZRevRank err")
}

func TestTx_ZScore(t *testing.T) {
	bucket, key1, _, _ := InitDataForZSet(t)
	assertions := assert.New(t)
	tx, err = db.Begin(false)
	defer func(db *DB) {
		err := db.Close()
		assert.NoError(t, err)
	}(db)

	require.NoError(t, err)

	score, err := tx.ZScore(bucket, []byte(key1))
	assertions.NoError(err, "TestTx_ZScore err")
	assertions.EqualValues(79, score, "TestTx_ZScore err")

	score, err = tx.ZScore("bucket_fake", []byte(key1))
	assertions.Error(err, "TestTx_ZScore err")
	assertions.NotEqual(0, score, "TestTx_ZScore err")

	score, err = tx.ZScore(bucket, []byte("key_fake"))

	assertions.Error(err, "TestTx_ZScore err")
	assertions.NotEqual(0, score, "TestTx_ZScore err")

	assertions.NoError(tx.Commit())

	score, err = tx.ZScore(bucket, []byte(key1))

	assertions.Error(err, "TestTx_ZScore err")
	assertions.NotEqual(0, score, "TestTx_ZScore err")

}

func TestTx_ZGetByKey(t *testing.T) {
	bucket, key1, _, _ := InitDataForZSet(t)
	assertions := assert.New(t)
	tx, err = db.Begin(false)
	defer func(db *DB) {
		err := db.Close()
		assert.NoError(t, err)
	}(db)

	require.NoError(t, err)

	node, err := tx.ZGetByKey(bucket, []byte(key1))
	assertions.NoError(err, "TestTx_ZGetByKey err")
	assertions.Equal(key1, node.Key(), "TestTx_ZGetByKey err")

	node, err = tx.ZGetByKey("bucket_fake", []byte(key1))
	assertions.Error(err, "TestTx_ZGetByKey err")
	assertions.Nil(node, "TestTx_ZGetByKey err")

	node, err = tx.ZGetByKey(bucket, []byte("key_fake"))

	assertions.Error(err, "TestTx_ZGetByKey err")
	assertions.Nil(node, "TestTx_ZGetByKey err")

	assertions.NoError(tx.Commit())
	node, err = tx.ZGetByKey(bucket, []byte(key1))

	assertions.Error(err, "TestTx_ZGetByKey err")
	assertions.Nil(node, "TestTx_ZGetByKey err")
}
