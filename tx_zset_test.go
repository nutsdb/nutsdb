package nutsdb

import (
	"fmt"
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
			err := os.Remove(fileDir + "/" + name)
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
	if err != nil {
		t.Fatal(err)
	}

	bucket := "myZSet"

	if err := tx.ZAdd(bucket, []byte("key1"), 100, []byte("val1")); err != nil {
		t.Fatal(err)
		tx.Rollback()
	} else {
		err := tx.ZAdd(bucket, []byte("key1"+SeparatorForZSetKey), 100, []byte("val1"))
		if err == nil {
			tx.Rollback()
			t.Fatal("TestTx_ZAdd err")
		}

		tx.Commit()
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	num, err := tx.ZCard(bucket)
	if num != 1 || err != nil {
		t.Error("TestTx_ZAdd err")
	}

	tx.Commit()
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
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.ZAdd(bucket, []byte(key2), 98, []byte("val2")); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.ZAdd(bucket, []byte(key3), 99, []byte("val3")); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}

	tx.Commit()

	return
}

func TestTx_ZMembers(t *testing.T) {
	bucket, key1, key2, key3 := InitDataForZSet(t)
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	if nodes, err := tx.ZMembers(bucket); err != nil {
		tx.Rollback()
		t.Fatal(err)
	} else {
		tx.Commit()
		if len(nodes) != 3 {
			t.Error("TestTx_ZMembers err")
		}

		if _, ok := nodes[key1]; !ok {
			t.Error("TestTx_ZMembers err")
		}
		if _, ok := nodes[key2]; !ok {
			t.Error("TestTx_ZMembers err")
		}
		if _, ok := nodes[key3]; !ok {
			t.Error("TestTx_ZMembers err")
		}
	}

	if _, err := tx.ZMembers(bucket); err == nil {
		t.Error("TestTx_ZMembers err")
	}
}

func TestTx_ZCard(t *testing.T) {
	bucket, _, _, _ := InitDataForZSet(t)
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	if num, err := tx.ZCard(bucket); err != nil {
		fmt.Println("err", err)
		tx.Rollback()
		t.Fatal("TestTx_ZCard err")
	} else {
		if num != 3 {
			t.Error("TestTx_ZCard err")
		}
	}

	tx.Commit()

	if _, err := tx.ZCard(bucket); err == nil {
		t.Error("TestTx_ZCard err")
	}
}

func TestTx_ZCount(t *testing.T) {
	bucket, _, _, _ := InitDataForZSet(t)

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	num, err := tx.ZCount(bucket, 90, 100, nil)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	} else {
		if num != 2 {
			tx.Rollback()
			t.Fatal("TestTx_ZCount err")
		}

		num, err := tx.ZCount("bucket_fake", 90, 100, nil)
		if err == nil || num != 0 {
			tx.Rollback()
			t.Fatal("TestTx_ZCount err")
		}

		tx.Commit()
	}

	num, err = tx.ZCount(bucket, 90, 100, nil)
	if err == nil || num != 0 {
		t.Error("TestTx_ZCount err")
	}
}

func TestTx_ZPopMax(t *testing.T) {
	bucket, _, _, _ := InitDataForZSet(t)
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	node, err := tx.ZPopMax(bucket)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}

	if node.Key() != "key3" || node.Score() != 99 {
		tx.Rollback()
		t.Fatal("TestTx_ZPopMax err")
	} else {
		_, err := tx.ZPopMax("bucket_fake")
		if err == nil {
			tx.Rollback()
			t.Fatal("TestTx_ZPopMax err")
		}
		tx.Commit()
	}

	node, err = tx.ZPopMax(bucket)
	if err == nil {
		t.Fatal("TestTx_ZPopMax err")
	}
}

func TestTx_ZPopMin(t *testing.T) {
	bucket, _, _, _ := InitDataForZSet(t)
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	node, err := tx.ZPopMin(bucket)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}

	if node.Key() != "key1" || node.Score() != 79 {
		tx.Rollback()
		t.Fatal("TestTx_ZPopMin err")
	} else {
		_, err := tx.ZPopMin("bucket_fake")
		if err == nil {
			tx.Rollback()
			t.Fatal("TestTx_ZPopMin err")
		}
		tx.Commit()
	}

	node, err = tx.ZPopMin(bucket)
	if err == nil {
		t.Fatal("TestTx_ZPopMin err")
	}
}

func TestTx_ZPickMax(t *testing.T) {
	bucket, _, _, _ := InitDataForZSet(t)
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	node, err := tx.ZPeekMax("bucket_fake")
	if err == nil || node != nil {
		t.Error("TestTx_ZPickMax err")
	}

	node, err = tx.ZPeekMax(bucket)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	} else {
		tx.Commit()

		if node.Key() != "key3" {
			t.Error("TestTx_ZPickMax err")
		}

		node, err = tx.ZPeekMax(bucket)
		if err == nil || node != nil {
			t.Error("TestTx_ZPickMax err")
		}
	}
}

func TestTx_ZPickMin(t *testing.T) {
	bucket, _, _, _ := InitDataForZSet(t)
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	node, err := tx.ZPeekMin("bucket_fake")
	if err == nil || node != nil {
		t.Error("TestTx_ZPickMin err")
	}

	node, err = tx.ZPeekMin(bucket)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	} else {
		tx.Commit()

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
	if err != nil {
		t.Fatal(err)
	}

	var expectResult map[string]struct{}
	expectResult = make(map[string]struct{})
	expectResult[key1] = struct{}{}
	expectResult[key2] = struct{}{}

	nodes, err := tx.ZRangeByRank("bucket_fake", 1, 2)
	if err == nil || nodes != nil {
		t.Error("TestTx_ZRangByRank err")
	}

	nodes, err = tx.ZRangeByRank(bucket, 1, 2)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	} else {
		tx.Commit()

		if len(nodes) != 2 {
			t.Error("TestTx_ZRangByRank err")
		}

		for _, node := range nodes {
			if _, ok := expectResult[node.Key()]; !ok {
				t.Error("TestTx_ZRangByRank err")
			}
		}

		nodes, err = tx.ZRangeByRank(bucket, 1, 3)
		if err == nil || nodes != nil {
			t.Error("TestTx_ZRangByRank err")
		}
	}
}

func TestTx_ZRem(t *testing.T) {
	bucket, key1, key2, _ := InitDataForZSet(t)
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	err := tx.ZRem("bucket_fake", key1)
	if err == nil {
		t.Error("TestTx_ZRem err")
	}

	err = tx.ZRem(bucket, key1)
	if err != nil {
		t.Error("TestTx_ZRem err")
	}

	err = tx.ZRem(bucket, key1)
	if err != nil {
		t.Error("TestTx_ZRem err")
	}

	tx.Commit()
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	n, err := tx.ZGetByKey(bucket, []byte(key1))
	if n != nil || err == nil {
		t.Error("TestTx_ZRem err")
	}

	n, err = tx.ZGetByKey(bucket, []byte(key2))
	if n == nil || err != nil {
		t.Error("TestTx_ZRem err")
	}

	tx.Commit()

	err = tx.ZRem(bucket, "key2")
	if err == nil {
		t.Error("TestTx_ZRem err")
	}
}

func TestTx_ZRemRangeByRank(t *testing.T) {
	bucket, key1, key2, key3 := InitDataForZSet(t)
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	err := tx.ZRemRangeByRank("bucket_fake", 1, 2)
	if err == nil {
		t.Error("TestTx_ZRemRangeByRank err")
	}

	err = tx.ZRemRangeByRank(bucket, 1, 2)
	if err != nil {
		t.Error(err)
	}

	tx.Commit()

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	dict, err := tx.ZMembers(bucket)
	if err != nil {
		t.Error("TestTx_ZRemRangeByRank err")
	}

	if _, ok := dict[key3]; !ok {
		t.Error("TestTx_ZRemRangeByRank err")
	}

	if _, ok := dict[key2]; ok {
		t.Error("TestTx_ZRemRangeByRank err")
	}

	if _, ok := dict[key1]; ok {
		t.Error("TestTx_ZRemRangeByRank err")
	}

	tx.Commit()

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	err = tx.ZRemRangeByRank(bucket, 1, 2)

	if err != nil {
		t.Error("TestTx_ZRemRangeByRank err")
	}

	tx.Commit()

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	if items, err := tx.ZMembers(bucket); err != nil && items != nil {
		t.Error("TestTx_ZRemRangeByRank err")
	}
	tx.Commit()

	err = tx.ZRemRangeByRank(bucket, 1, 2)
	if err == nil {
		t.Error("TestTx_ZRemRangeByRank err")
	}
}

func TestTx_ZRank(t *testing.T) {
	bucket, key1, key2, key3 := InitDataForZSet(t)
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	rank, err := tx.ZRank("bucket_fake", []byte(key1))
	if err == nil || rank != 0 {
		t.Error("TestTx_ZRank err")
	}

	rank, err = tx.ZRank(bucket, []byte(key1))
	if err != nil || rank != 1 {
		t.Error("TestTx_ZRank err")
	}
	rank, err = tx.ZRank(bucket, []byte(key2))
	if err != nil || rank != 2 {
		t.Error("TestTx_ZRank err")
	}
	rank, err = tx.ZRank(bucket, []byte(key3))
	if err != nil || rank != 3 {
		t.Error("TestTx_ZRank err")
	}

	tx.Commit()

	rank, err = tx.ZRank(bucket, []byte(key3))
	if err == nil || rank == 3 {
		t.Error("TestTx_ZRank err")
	}
}

func TestTx_ZRevRank(t *testing.T) {
	bucket, key1, key2, key3 := InitDataForZSet(t)
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	rank, err := tx.ZRevRank("bucket_fake", []byte(key1))
	if err == nil || rank != 0 {
		t.Error("TestTx_ZRevRank err")
	}

	rank, err = tx.ZRevRank(bucket, []byte(key1))
	if err != nil || rank != 3 {
		t.Error("TestTx_ZRevRank err")
	}
	rank, err = tx.ZRevRank(bucket, []byte(key2))
	if err != nil || rank != 2 {
		t.Error("TestTx_ZRevRank err")
	}
	rank, err = tx.ZRevRank(bucket, []byte(key3))
	if err != nil || rank != 1 {
		t.Error("TestTx_ZRevRank err")
	}

	tx.Commit()

	rank, err = tx.ZRevRank(bucket, []byte(key3))
	if err == nil || rank == 1 {
		t.Error("TestTx_ZRevRank err")
	}
}

func TestTx_ZScore(t *testing.T) {
	bucket, key1, _, _ := InitDataForZSet(t)
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	score, err := tx.ZScore(bucket, []byte(key1))
	if err != nil || score != 79 {
		t.Error("TestTx_ZScore err")
	}
	score, err = tx.ZScore("bucket_fake", []byte(key1))
	if err == nil || score != 0 {
		t.Error("TestTx_ZScore err")
	}

	score, err = tx.ZScore(bucket, []byte("key_fake"))
	if err == nil || score != 0 {
		t.Error("TestTx_ZScore err")
	}

	tx.Commit()

	score, err = tx.ZScore(bucket, []byte(key1))
	if err == nil || score != 0 {
		t.Error("TestTx_ZScore err")
	}
}

func TestTx_ZGetByKey(t *testing.T) {
	bucket, key1, _, _ := InitDataForZSet(t)
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	node, err := tx.ZGetByKey(bucket, []byte(key1))
	if err != nil || node.Key() != key1 {
		t.Error("TestTx_ZGetByKey err")
	}

	node, err = tx.ZGetByKey("bucket_fake", []byte(key1))
	if err == nil || node != nil {
		t.Error("TestTx_ZGetByKey err")
	}

	node, err = tx.ZGetByKey(bucket, []byte("key_fake"))
	if err == nil || node != nil {
		t.Error("TestTx_ZGetByKey err")
	}

	tx.Commit()
	node, err = tx.ZGetByKey(bucket, []byte(key1))
	if err == nil || node != nil {
		t.Error("TestTx_ZGetByKey err")
	}
}
