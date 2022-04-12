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

package list

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func InitListData() (list *List, key string) {
	list = New()
	key = "myList"
	list.RPush(key, []byte("a"))
	list.RPush(key, []byte("b"))
	list.RPush(key, []byte("c"))
	list.RPush(key, []byte("d"))

	return
}

func TestList_RPush(t *testing.T) {
	list, key := InitListData()

	expectResult := []string{"a", "b", "c", "d"}
	for i := 0; i < len(expectResult); i++ {
		item, err := list.LPop(key)
		if err == nil && string(item) != expectResult[i] {
			t.Error("TestList_LPush err")
		}
	}
}

func TestList_LPush(t *testing.T) {
	list := New()

	key := "myList"

	list.LPush(key, []byte("a"))
	list.LPush(key, []byte("b"))
	list.LPush(key, []byte("c"))
	list.LPush(key, []byte("d"), []byte("e"), []byte("f"))
	list.LPush(key, [][]byte{[]byte("d"), []byte("e"), []byte("f")}...)

	expectResult := []string{"f", "e", "d", "f", "e", "d", "c", "b", "a"}
	for i := 0; i < len(expectResult); i++ {
		item, err := list.LPop(key)
		if err == nil && string(item) != expectResult[i] {
			t.Error("TestList_LPush err")
		}
	}
}

func TestList_RPushAndLPush(t *testing.T) {
	list, key := InitListData()
	list.LPush(key, []byte("e"))
	list.LPush(key, []byte("f"))
	list.LPush(key, []byte("g"))

	expectResult := []string{"g", "f", "e", "a", "b", "c", "d"}
	for i := 0; i < len(expectResult); i++ {
		item, err := list.LPop(key)
		if err == nil && string(item) != expectResult[i] {
			t.Error("TestList_RPushAndLPush err")
		}
	}
}

func TestList_LPop(t *testing.T) {
	list, key := InitListData()

	item, err := list.LPop(key)

	assert.Nil(t, err, "TestList_LPop err")
	assert.Equal(t, "a", string(item), "TestList_LPop wrong value")

	item, err = list.LPop("key_fake")
	assert.NotNil(t, err, "TestList_LPop err")
	assert.Nil(t, item, "TestList_LPop err")

	item, err = list.LPop(key)
	assert.Nil(t, err, "TestList_LPop err")
	assert.Equal(t, "b", string(item), "TestList_LPop wrong value")

	item, err = list.LPop(key)
	assert.Nil(t, err, "TestList_LPop err")
	assert.Equal(t, "c", string(item), "TestList_LPop wrong value")

	item, err = list.LPop(key)
	assert.Nil(t, err, "TestList_LPop err")
	assert.Equal(t, "d", string(item), "TestList_LPop wrong value")

	item, err = list.LPop(key)
	assert.NotNilf(t, err, "TestList_LPop err")
	assert.Nil(t, item, "TestList_LPop err")
}

func TestList_RPop(t *testing.T) {
	list, key := InitListData()
	size, err := list.Size(key)
	assert.Nil(t, err, "TestList_RPop err")
	assert.Equalf(t, 4, size, "TestList_RPop wrong size")

	item, err := list.RPop(key)
	size, err = list.Size(key)
	assert.Nil(t, err, "TestList_RPop err")
	assert.Equal(t, 3, size, "TestList_RPop wrong size")
	assert.Equal(t, "d", string(item), "TestList_RPop wrong value")

	item, err = list.RPop("key_fake")
	assert.NotNil(t, err, "TestList_RPop err")
	assert.Nil(t, item, "TestList_RPop err")
}

func TestList_LRange(t *testing.T) {
	list, key := InitListData()
	list.RPush(key, []byte("e"))
	list.RPush(key, []byte("f"))

	type args struct {
		key   string
		start int
		end   int
	}

	tests := []struct {
		name    string
		list    *List
		args    args
		want    [][]byte
		wantErr bool
	}{
		{
			"normal-1",
			list,
			args{key, 0, 2},
			[][]byte{[]byte("a"), []byte("b"), []byte("c")},
			false,
		},
		{
			"normal-1",
			list,
			args{key, 4, 8},
			[][]byte{[]byte("e"), []byte("f")},
			false,
		},
		{
			"start + size > end",
			list,
			args{key, -1, 2},
			nil,
			true,
		},
		{
			"wrong start index ",
			list,
			args{key, -3, -1},
			[][]byte{[]byte("d"), []byte("e"), []byte("f")},
			false,
		},
		{
			"start > end",
			list,
			args{key, -1, -2},
			nil,
			true,
		},
		{
			"fake key",
			list,
			args{"key_fake", -2, -1},
			nil,
			true,
		},
		{
			"start == end",
			list,
			args{key, 0, 0},
			[][]byte{[]byte("a")},
			false,
		},
		{
			"all values",
			list,
			args{key, 0, -1},
			[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f")},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.list.LRange(tt.args.key, tt.args.start, tt.args.end)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}

}

func TestList_LRem_D(t *testing.T) {
	//list, key := InitListData()

}

func TestList_LRem(t *testing.T) {
	list, key := InitListData()

	if num, err := list.LRem("key_fake", 1, []byte("a")); err == nil || num != 0 {
		t.Error("TestList_LRem err")
	}

	if num, err := list.LRem(key, 1, []byte("a")); err != nil && num != 1 {
		t.Error("TestList_LRem err")
	}

	expectResult := getExpectResult("b", "c", "d")

	if items, err := list.LRange(key, 0, -1); err != nil {
		t.Error("TestList_LRem err")
	} else {
		for _, item := range items {
			if _, ok := expectResult[string(item)]; !ok {
				t.Error("TestList_LRem err")
			}
		}
	}
}

func TestList_LRem2(t *testing.T) {
	list, key := InitListData()

	if num, err := list.LRem(key, -1, []byte("d")); err != nil || num == 0 {
		t.Error("TestList_LRem err")
	}

	expectResult := getExpectResult("a", "b", "c")

	if items, err := list.LRange(key, 0, -1); err != nil || items == nil {
		t.Error("TestList_LRem err")
	} else {
		for _, item := range items {
			if _, ok := expectResult[string(item)]; !ok {
				t.Error("TestList_LRem err")
			}
		}
	}
}

func TestList_LRem3(t *testing.T) {
	list, key := InitListData()
	list.RPush(key, []byte("b"))
	if num, err := list.LRem(key, -2, []byte("b")); num != 2 || err != nil {
		t.Error("TestList_LRem err")
	}

	if items, err := list.LRange(key, 0, -1); err != nil {
		t.Error("TestList_LRem err")
	} else {
		expectResult := getExpectResult("a", "c", "d")
		for _, item := range items {
			if _, ok := expectResult[string(item)]; !ok {
				t.Error("TestList_LRem err")
			}
		}
	}
}

func TestList_LRem4(t *testing.T) {
	{
		list, key := InitListData()
		num, err := list.LRem(key, -10, []byte("b"))
		if err != nil || num != 1 {
			t.Error("TestList_LRem err")
		}
	}
	{
		list, key := InitListData()
		list.RPush(key, []byte("b"))
		num, err := list.LRem(key, 4, []byte("b"))
		if err != nil || num != 2 {
			t.Error("TestList_LRem err")
		}
	}

	{
		list, key := InitListData()
		list.RPush(key, []byte("b"))
		num, err := list.LRem(key, 2, []byte("b"))
		if err != nil || num != 2 {
			t.Error(err)
		}
	}
}

func TestList_LRem5(t *testing.T) {
	list, key := InitListData()
	list.RPush(key, []byte("b"))
	list.RPush(key, []byte("b"))
	num, err := list.LRem(key, 0, []byte("b"))

	if err != nil && num != 3 {
		t.Error("TestList_LRem err")
	}
	size, err := list.Size(key)
	if err != nil || size != 3 {
		t.Error("TestList_LRem err")
	}
}

func TestList_LRem6(t *testing.T) {
	list, key := InitListData()
	list.RPush(key, []byte("b"))
	list.RPush(key, []byte("b"))
	num, err := list.LRem(key, -3, []byte("b"))

	if err != nil && num != 3 {
		t.Error("TestList_LRem err")
	}

	size, err := list.Size(key)
	if err != nil || size != 3 {
		t.Error("TestList_LRem err")
	}
}

func TestList_LRem7(t *testing.T) {
	list, key := InitListData()
	list.RPush(key, []byte("b"))
	list.RPush(key, []byte("b"))
	num, err := list.LRem(key, 0, []byte("b"))

	if err != nil && num != 3 {
		t.Error("TestList_LRem err")
	}

	size, err := list.Size(key)
	if err != nil || size != 3 {
		t.Error("TestList_LRem err")
	}
}

func TestList_LSet(t *testing.T) {
	list, key := InitListData()

	if string(list.Items[key][0]) != "a" {
		t.Error("TestList_LSet err")
	}

	list.LSet(key, 0, []byte("a1"))

	if string(list.Items[key][0]) != "a1" {
		t.Error("TestList_LSet err")
	}

	err := list.LSet("key_fake", 0, []byte("a1"))
	if err == nil {
		t.Error("TestList_LSet err")
	}

	err = list.LSet(key, 4, []byte("a1"))
	if err == nil {
		t.Error("TestList_LSet err")
	}

	err = list.LSet(key, -1, []byte("a1"))
	if err == nil {
		t.Error("TestList_LSet err")
	}
}

func TestList_Ltrim(t *testing.T) {
	list, key := InitListData()

	expectResult := getExpectResult("a", "b", "c", "d")

	for _, item := range list.Items[key] {
		if _, ok := expectResult[string(item)]; !ok {
			t.Error("TestList_Ltrim err")
		}
	}

	if err := list.Ltrim(key, 0, 2); err != nil {
		t.Error("TestList_Ltrim err")
	}

	expectResult = getExpectResult("a", "b", "c")
	for _, item := range list.Items[key] {
		if _, ok := expectResult[string(item)]; !ok {
			t.Error("TestList_Ltrim err")
		}
	}

	if err := list.Ltrim("key_fake", 0, 2); err == nil {
		t.Error("TestList_Ltrim err")
	}

	if err := list.Ltrim(key, -1, -2); err == nil {
		t.Error("TestList_Ltrim err")
	}
}

func getExpectResult(items ...string) map[string]struct{} {
	expectResult := make(map[string]struct{}, len(items))
	for _, item := range items {
		expectResult[item] = struct{}{}
	}

	return expectResult
}
