package inmemory

import (
	"testing"
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
	err := initRPushData(bucket, key)
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
	err := initLPushData(bucket, key)
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
	err := initLPushData(bucket, key)
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
	err := initLPushData(bucket, key)
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
	err := initLPushData(bucket, key)
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
	err := initLPushData(bucket, key)
	if err != nil {
		t.Error(err)
	}
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
	err := initLPushData(bucket, key)
	if err != nil {
		t.Error(err)
	}
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
	err := initLPushData(bucket, key)
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
