package inmemory

import (
	"errors"
	"testing"

	"github.com/xujiajun/nutsdb/ds/zset"
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

func TestDB_ZCount(t *testing.T) {
	bucket, _, _, _ := initZAddItems()
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
	err := testDB.ZRem(bucket, key1)
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
	err := testDB.ZRemRangeByRank(bucket, 1, 2)
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
