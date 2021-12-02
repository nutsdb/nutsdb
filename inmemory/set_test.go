package inmemory

import (
	"testing"
)

func TestDB_Set(t *testing.T) {
	initTestDB()

	bucket := "bucket1"
	key := "key1"
	val1 := []byte("val1")
	val2 := []byte("val2")
	val3 := []byte("val3")
	val4 := []byte("val4")

	key2 := "key2"
	bucket2 := "bucket2"
	// SAdd
	{
		err := testDB.SAdd(bucket, key, val1, val2, val3, val4)
		if err != nil {
			t.Error(err)
		}

		err = testDB.SAdd(bucket, key2, []byte("val1"), []byte("val12"))
		if err != nil {
			t.Error(err)
		}

		err = testDB.SAdd(bucket2, key2, []byte("val1"), []byte("val22"))
		if err != nil {
			t.Error(err)
		}
	}
	// SCard
	{
		num, err := testDB.SCard(bucket, key)
		if err != nil {
			t.Error(err)
		}
		if num != 4 {
			t.Errorf("expect %d, but get %d", 2, num)
		}
	}
	// SHasKey
	{
		isOk, err := testDB.SHasKey(bucket, key)
		if err != nil {
			t.Error(err)
		}
		if !isOk {
			t.Errorf("err SHasKey bucket %s, key %s", bucket, key)
		}
	}
	// SIsMember
	{
		isMember, err := testDB.SIsMember(bucket, key, val1)
		if err != nil {
			t.Error(err)
		}
		if !isMember {
			t.Error("err SIsMember")
		}
	}
	// SAreMembers
	{
		areMembers, err := testDB.SAreMembers(bucket, key, val1, val2)
		if err != nil {
			t.Error(err)
		}
		if !areMembers {
			t.Error("err SIsMember")
		}
	}
	// SMembers
	{
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
	// SDiffByOneBucket
	{
		list, err := testDB.SDiffByOneBucket(bucket, key2, key)
		if err != nil {
			t.Error(err)
		}
		for _, v := range list {
			if string(v) != "val12" {
				t.Error("err SDiffByOneBucket")
			}
		}
	}
	// SDiffByTwoBuckets
	{
		list, err := testDB.SDiffByTwoBuckets(bucket2, key2, bucket, key)
		if err != nil {
			t.Error(err)
		}
		for _, v := range list {
			if string(v) != "val22" {
				t.Error("err SDiffByTwoBuckets")
			}
		}
	}
	// SMoveByOneBucket
	{
		isOK, err := testDB.SMoveByOneBucket(bucket, key, key2, val2)
		if err != nil {
			t.Error(err)
		}
		if !isOK {
			t.Error("err SMoveByOneBucket")
		}
		list, err := testDB.SMembers(bucket, key)
		if err != nil {
			t.Error(err)
		}
		if len(list) != 3 {
			t.Error("err num")
		}
	}
	// SMoveByTwoBuckets
	{
		isOk, err := testDB.SMoveByTwoBuckets(bucket, key, bucket2, key2, val3)
		if err != nil {
			t.Error(err)
		}
		if !isOk {
			t.Error("err SMoveByTwoBuckets")
		}
		list, err := testDB.SMembers(bucket, key)
		if err != nil {
			t.Error(err)
		}

		list, err = testDB.SMembers(bucket, key2)
		if err != nil {
			t.Error(err)
		}
		if len(list) != 2 {
			t.Error("err SMoveByTwoBuckets")
		}
		list, err = testDB.SMembers(bucket2, key2)
		if err != nil {
			t.Error(err)
		}
		if len(list) != 3 {
			t.Error("err SMoveByTwoBuckets")
		}
	}
	// SUnionByOneBucket
	{
		list, err := testDB.SUnionByOneBucket(bucket, key, key2)
		if err != nil {
			t.Error(err)
		}

		if len(list) != 4 {
			t.Error("err SUnionByOneBucket")
		}
	}
	// SUnionByTwoBuckets
	{
		list, err := testDB.SUnionByTwoBuckets(bucket, key, bucket2, key2)
		if err != nil {
			t.Error(err)
		}
		if len(list) != 4 {
			t.Error("err SUnionByTwoBuckets")
		}
	}
	// SPop
	{
		_, err := testDB.SPop(bucket, key)
		if err != nil {
			t.Error(err)
		}
		num, err := testDB.SCard(bucket, key)
		if err != nil {
			t.Error(err)
		}
		if num != 1 {
			t.Errorf("expect %d, but get %d", 1, num)
		}
	}
}
