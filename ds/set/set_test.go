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

package set

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var mySet = New()

func TestSet_SAdd(t *testing.T) {
	key := "mySet0"

	mySet.SAdd(key, []byte("Hello"))
	mySet.SAdd(key, []byte("World"))
	mySet.SAdd(key, []byte("hello1"), []byte("hello2"))

	if ok, _ := mySet.SAreMembers(key, []byte("Hello"), []byte("World"), []byte("hello1"), []byte("hello2")); !ok {
		t.Error("TestSet_SAdd err")
	}
}

func TestSet_SDiff(t *testing.T) {
	mySet := New()
	key1 := "mySet1"
	key2 := "mySet2"
	key3 := "mySet3"
	key4 := "mySet4"
	key5 := "mySet5"

	mySet.SAdd(key1, []byte("a"))
	mySet.SAdd(key1, []byte("b"))
	mySet.SAdd(key1, []byte("c"))

	mySet.SAdd(key2, []byte("d"))
	mySet.SAdd(key2, []byte("c"))
	mySet.SAdd(key2, []byte("e"))

	mySet.SAdd(key3, []byte("a"))
	mySet.SAdd(key3, []byte("b"))
	mySet.SAdd(key3, []byte("c"))

	mySet.SAdd(key4, []byte("a"))
	mySet.SAdd(key4, []byte("b"))
	mySet.SAdd(key4, []byte("c"))

	mySet.SAdd(key4, []byte("d"))
	mySet.SAdd(key4, []byte("e"))
	mySet.SAdd(key4, []byte("f"))

	mySet.SAdd(key5, []byte("b"))

	type args struct {
		key1 string
		key2 string
	}

	tests := []struct {
		name    string
		args    args
		set     *Set
		want    [][]byte
		wantErr bool
	}{
		{
			"normal set diff",
			args{key1, key2},
			mySet,
			[][]byte{[]byte("b"), []byte("a")},
			false,
		},
		{
			"normal set diff",
			args{key1, key3},
			mySet,
			nil,
			false,
		},
		{
			"bigger set diff", // the order of elements is not fixed in diff result
			args{key4, key5},
			mySet,
			[][]byte{[]byte("a"), []byte("c"), []byte("d"), []byte("e"), []byte("f")},
			false,
		},
		{
			"first fake set",
			args{"fake_key1", key2},
			mySet,
			nil,
			true,
		},
		{
			"second fake set",
			args{key1, "fake_key2"},
			mySet,
			nil,
			true,
		},
		{
			"two fake set",
			args{"fake_key1", "fake_key2"},
			mySet,
			nil,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.set.SDiff(tt.args.key1, tt.args.key2)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.ElementsMatchf(t, got, tt.want, "Get() got = %v, want %v", got, tt.want)
		})
	}

}

func TestSet_SCard(t *testing.T) {
	mySet := New()
	key1 := "mySet1"
	key2 := "mySet2"
	key3 := "mySet3"

	mySet.SAdd(key1, []byte("1"))
	mySet.SAdd(key1, []byte("2"))
	mySet.SAdd(key1, []byte("3"))

	mySet.SAdd(key2, []byte("1"))
	mySet.SAdd(key2, []byte("2"))
	mySet.SAdd(key2, []byte("3"))

	mySet.SAdd(key2, []byte("4"))
	mySet.SAdd(key2, []byte("5"))
	mySet.SAdd(key2, []byte("6"))

	mySet.SAdd(key3, []byte("1"))

	tests := []struct {
		name string
		key  string
		set  *Set
		want int
	}{
		{"normal set", key1, mySet, 3},
		{"normal set", key2, mySet, 6},
		{"normal set", key3, mySet, 1},
		{"fake key", "key_fake", mySet, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set.SCard(tt.key)
			assert.Equalf(t, tt.want, got, "TestSet_SCard err")
		})
	}

	//if mySet.SCard(key) != 3 {
	//	t.Error("TestSet_SCard err")
	//}
	//
	//if mySet.SCard("key_fake") != 0 {
	//	t.Error("TestSet_SCard err")
	//}
}

func TestSet_SInter(t *testing.T) {
	key1 := "mySet5"
	key2 := "mySet6"
	mySet.SAdd(key1, []byte("a"))
	mySet.SAdd(key1, []byte("b"))
	mySet.SAdd(key1, []byte("c"))

	mySet.SAdd(key2, []byte("d"))
	mySet.SAdd(key2, []byte("c"))
	mySet.SAdd(key2, []byte("e"))

	list, err := mySet.SInter(key1, key2)
	if err != nil {
		t.Error("TestSet_SInter err", err)
	}

	key3 := "mySet7"
	mySet.SAdd(key3, []byte("c"))

	for _, item := range list {
		if !mySet.SIsMember(key3, item) {
			t.Error("TestSet_SInter err")
		}
	}

	_, err = mySet.SInter("fake_key1", key2)
	if err == nil {
		t.Error("TestSet_SInter err")
	}

	_, err = mySet.SInter(key1, "fake_key2")
	if err == nil {
		t.Error("TestSet_SInter err")
	}

	_, err = mySet.SInter("fake_key1", "fake_key2")
	if err == nil {
		t.Error("TestSet_SInter err")
	}
}

func TestSet_SMembers(t *testing.T) {
	key := "mySet8"

	mySet.SAdd(key, []byte("Hello"))
	mySet.SAdd(key, []byte("World"))

	list, err := mySet.SMembers("fake_key")
	if err == nil || list != nil {
		t.Error("TestSet_SMembers err", err)
	}

	list, err = mySet.SMembers(key)
	if err != nil {
		t.Error("TestSet_SMembers err", err)
	}

	if len(list) != 2 {
		t.Error("TestSet_SMembers err")
	}

	if !mySet.SIsMember(key, []byte("Hello")) {
		t.Error("TestSet_SMembers err")
	}

	if !mySet.SIsMember(key, []byte("World")) {
		t.Error("TestSet_SMembers err")
	}

	if mySet.SIsMember("fake_key", []byte("World")) {
		t.Error("TestSet_SMembers err")
	}
}

func TestSet_SMove(t *testing.T) {
	key1 := "mySet9"

	mySet.SAdd(key1, []byte("one"))
	mySet.SAdd(key1, []byte("two"))

	key2 := "mySet10"
	mySet.SAdd(key2, []byte("three"))

	mySet.SMove(key1, key2, []byte("two"))

	list1, err := mySet.SMembers(key1)
	if err != nil {
		t.Error("TestSet_SPop err", err)
	}
	if len(list1) != 1 {
		t.Error("TestSet_SMove err")
	}

	list2, err := mySet.SMembers(key2)
	if err != nil {
		t.Error("TestSet_SPop err", err)
	}

	if len(list2) != 2 {
		t.Error("TestSet_SMove err")
	}

	ok, err := mySet.SMove("fake_key1", key2, []byte("two"))
	if ok && err == nil {
		t.Error("TestSet_SMove err")
	}

	ok, err = mySet.SMove(key1, "fake_key2", []byte("two"))
	if ok && err == nil {
		t.Error("TestSet_SMove err")
	}
}

func TestSet_SPop(t *testing.T) {
	key := "mySet10"

	mySet.SAdd(key, []byte("one"))
	mySet.SAdd(key, []byte("two"))
	mySet.SAdd(key, []byte("three"))

	list, err := mySet.SMembers(key)
	if err != nil {
		t.Error("TestSet_SPop err", err)
	}

	if len(list) != 3 {
		t.Error("TestSet_SPop err")
	}

	item := mySet.SPop(key)

	list, err = mySet.SMembers(key)
	if err != nil {
		t.Error("TestSet_SPop err")
	}

	if len(list) != 2 {
		t.Error("TestSet_SPop err")
	}

	if mySet.SIsMember(key, item) {
		t.Error("TestSet_SPop err")
	}

	item = mySet.SPop("mySet_fake")
	if item != nil {
		t.Error("TestSet_SPop err")
	}
}

func TestSet_SRem(t *testing.T) {
	key := "mySet11"

	mySet.SAdd(key, []byte("one"))
	mySet.SAdd(key, []byte("two"))
	mySet.SAdd(key, []byte("three"))

	mySet.SRem(key, []byte("one"))
	mySet.SRem(key, []byte("two"))

	if mySet.SIsMember(key, []byte("one")) {
		t.Error("TestSet_SRem err")
	}

	if mySet.SIsMember(key, []byte("two")) {
		t.Error("TestSet_SRem err")
	}

	if err := mySet.SRem("key_fake", []byte("two")); err == nil {
		t.Error("TestSet_SRem err")
	}

	if err := mySet.SRem(key, []byte("")); err == nil {
		t.Error("TestSet_SRem err")
	}
}

func TestSet_SUnion(t *testing.T) {
	key1 := "mySet12"
	mySet.SAdd(key1, []byte("a"))
	mySet.SAdd(key1, []byte("b"))
	mySet.SAdd(key1, []byte("c"))

	key2 := "mySet12"
	mySet.SAdd(key2, []byte("c"))
	mySet.SAdd(key2, []byte("d"))
	mySet.SAdd(key2, []byte("e"))

	list, err := mySet.SUnion("fake_key", key2)
	if err == nil || list != nil {
		t.Error("TestSet_SUnion err")
	}

	list, err = mySet.SUnion(key1, key2)
	if err != nil {
		t.Error("TestSet_SUnion err")
	}

	key3 := "mySet13"
	mySet.SAdd(key3, []byte("a"))
	mySet.SAdd(key3, []byte("b"))
	mySet.SAdd(key3, []byte("c"))
	mySet.SAdd(key3, []byte("d"))
	mySet.SAdd(key3, []byte("e"))

	for _, item := range list {
		if !mySet.SIsMember(key3, item) {
			t.Error("TestSet_SMembers err")
		}
	}
}
