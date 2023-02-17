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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xujiajun/utils/math2"
)

func InitListData() (list *List, key string) {
	list = New()
	key = "myList"
	_, _ = list.RPush(key, []byte("a"))
	_, _ = list.RPush(key, []byte("b"))
	_, _ = list.RPush(key, []byte("c"))
	_, _ = list.RPush(key, []byte("d"))
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

func TestList_RPeek(t *testing.T) {
	list, key := InitListData()
	assertions := assert.New(t)
	expectResult := []string{"d", "c", "b", "a"}
	for _, item := range expectResult {
		rpk, _, _ := list.RPeek(key)
		assertions.Equal(item, string(rpk))
		rpp, _ := list.RPop(key)
		assertions.Equal(item, string(rpp))
	}
	_, _, err := list.RPeek(key)
	assertions.EqualError(err, ErrListNotFound.Error())
}

func TestList_LPush(t *testing.T) {
	list := New()

	key := "myList"
	assertions := assert.New(t)
	_, err := list.LPush(key, []byte("a"))
	assertions.NoError(err)

	_, err = list.LPush(key, []byte("b"))
	assertions.NoError(err)

	_, err = list.LPush(key, []byte("c"))
	assertions.NoError(err)

	_, err = list.LPush(key, []byte("d"), []byte("e"), []byte("f"))
	assertions.NoError(err)

	_, err = list.LPush(key, [][]byte{[]byte("d"), []byte("e"), []byte("f")}...)
	assertions.NoError(err)

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
	assertions := assert.New(t)
	_, err := list.LPush(key, []byte("e"))
	assertions.NoError(err)

	_, err = list.LPush(key, []byte("f"))
	assertions.NoError(err)

	_, err = list.LPush(key, []byte("g"))
	assertions.NoError(err)

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
	assertions := assert.New(t)
	item, err := list.LPop(key)

	assertions.Nil(err, "TestList_LPop err")
	assertions.Equal("a", string(item), "TestList_LPop wrong value")

	item, err = list.LPop("key_fake")
	assertions.NotNil(err, "TestList_LPop err")
	assertions.Nil(item, "TestList_LPop err")

	item, err = list.LPop(key)
	assertions.Nil(err, "TestList_LPop err")
	assertions.Equal("b", string(item), "TestList_LPop wrong value")

	item, err = list.LPop(key)
	assertions.Nil(err, "TestList_LPop err")
	assertions.Equal("c", string(item), "TestList_LPop wrong value")

	item, err = list.LPop(key)
	assertions.Nil(err, "TestList_LPop err")
	assertions.Equal("d", string(item), "TestList_LPop wrong value")

	item, err = list.LPop(key)
	assertions.NotNilf(err, "TestList_LPop err")
	assertions.Nil(item, "TestList_LPop err")
}

func TestList_RPop(t *testing.T) {
	list, key := InitListData()
	assertions := assert.New(t)
	size, err := list.Size(key)
	assertions.NoError(err, "TestList_RPop err")
	assertions.Equalf(4, size, "TestList_RPop wrong size")

	item, err := list.RPop(key)
	size, err = list.Size(key)
	assertions.NoError(err, "TestList_RPop err")
	assertions.Equal(3, size, "TestList_RPop wrong size")
	assertions.Equal("d", string(item), "TestList_RPop wrong value")

	item, err = list.RPop("key_fake")
	assertions.NotNil(err, "TestList_RPop err")
	assertions.Nil(item, "TestList_RPop err")
}

func TestList_LRange(t *testing.T) {
	list, key := InitListData()
	assertions := assert.New(t)
	_, err := list.RPush(key, []byte("e"))
	assertions.NoError(err)

	_, err = list.RPush(key, []byte("f"))
	assertions.NoError(err)

	emptyKeyList := New()
	_, err = emptyKeyList.RPush(key, []byte("g"))
	assertions.NoError(err)

	_, err = emptyKeyList.LPop(key)
	assertions.NoError(err)

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
		{
			"empty list",
			New(),
			args{key, 0, -1},
			nil,
			true,
		},
		{
			"list with empty key",
			emptyKeyList,
			args{key, 0, -1},
			nil,
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

func TestList_LRem(t *testing.T) {
	list, key := InitListData()
	assertions := assert.New(t)
	num, err := list.LRem("key_fake", 1, []byte("a"))
	assertions.Error(err, "TestList_LRem err")
	assertions.Equal(0, num, "TestList_LRem err")

	num, err = list.LRem(key, 1, []byte("a"))
	assertions.NoError(err, "TestList_LRem err")
	assertions.Equal(1, num, "TestList_LRem err")

	expectResult := [][]byte{[]byte("b"), []byte("c"), []byte("d")}
	items, err := list.LRange(key, 0, -1)
	assertions.NoError(err, "TestList_LRem err")
	assertions.Equal(expectResult, items, "")

}

func TestList_LRem2(t *testing.T) {
	list, key := InitListData()
	assertions := assert.New(t)
	num, err := list.LRem(key, -1, []byte("d"))
	assertions.NoError(err, "TestList_LRem err")
	assertions.Equal(1, num, "TestList_LRem err")

	expectResult := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	items, err := list.LRange(key, 0, -1)
	assertions.NoError(err, "TestList_LRem err")
	assertions.Equal(expectResult, items, "TestList_LRem err")
}

func TestList_LRem3(t *testing.T) {
	list, key := InitListData()
	assertions := assert.New(t)
	_, err := list.RPush(key, []byte("b"))
	assertions.NoError(err)

	num, err := list.LRem(key, -2, []byte("b"))
	assertions.NoError(err, "TestList_LRem err")
	assertions.Equal(2, num, "TestList_LRem err")

	expectResult := [][]byte{[]byte("a"), []byte("c"), []byte("d")}
	items, err := list.LRange(key, 0, -1)
	assertions.NoError(err, "TestList_LRem err")
	assertions.Equal(expectResult, items, "TestList_LRem err")
}

func TestList_LRem4(t *testing.T) {
	assertions := assert.New(t)
	{
		list, key := InitListData()
		num, err := list.LRem(key, -10, []byte("b"))
		assertions.NoError(err, "TestList_LRem err")
		assertions.Equal(1, num, "TestList_LRem err")
	}
	{
		list, key := InitListData()
		_, err := list.RPush(key, []byte("b"))
		assertions.NoError(err)

		num, err := list.LRem(key, 4, []byte("b"))
		assertions.NoError(err, "TestList_LRem err")
		assertions.Equal(2, num, "TestList_LRem err")
	}

	{
		list, key := InitListData()
		_, err := list.RPush(key, []byte("b"))
		assertions.NoError(err)
		num, err := list.LRem(key, 2, []byte("b"))
		assertions.NoError(err, "TestList_LRem err")
		assertions.Equal(2, num, "TestList_LRem err")
	}
}

func TestList_LRem5(t *testing.T) {
	list, key := InitListData()
	assertions := assert.New(t)
	_, err := list.RPush(key, []byte("b"))
	assertions.NoError(err)

	_, err = list.RPush(key, []byte("b"))
	assertions.NoError(err)
	num, err := list.LRem(key, 0, []byte("b"))
	assertions.NoError(err, "TestList_LRem err")
	assertions.Equal(3, num, "TestList_LRem err")

	size, err := list.Size(key)
	assertions.NoError(err, "TestList_LRem err")
	assertions.Equal(3, size, "TestList_LRem err")
}

func TestList_LRem6(t *testing.T) {
	list, key := InitListData()
	assertions := assert.New(t)
	_, err := list.RPush(key, []byte("b"))
	assertions.NoError(err)
	_, err = list.RPush(key, []byte("b"))
	assertions.NoError(err)
	num, err := list.LRem(key, -3, []byte("b"))
	assertions.NoError(err, "TestList_LRem err")
	assertions.Equal(3, num, "TestList_LRem err")

	size, err := list.Size(key)
	assertions.NoError(err, "TestList_LRem err")
	assertions.Equal(3, size, "TestList_LRem err")
}

func TestList_LRem7(t *testing.T) {
	list, key := InitListData()
	assertions := assert.New(t)
	_, err := list.RPush(key, []byte("b"))
	assertions.NoError(err)

	_, err = list.RPush(key, []byte("b"))
	assertions.NoError(err)
	num, err := list.LRem(key, 0, []byte("b"))
	assertions.NoError(err, "TestList_LRem err")
	assertions.Equal(3, num, "TestList_LRem err")

	size, err := list.Size(key)
	assertions.NoError(err, "TestList_LRem err")
	assertions.Equal(3, size, "TestList_LRem err")

	num, err = list.LRem(key, math2.MinInt, []byte("b"))
	assertions.EqualError(err, ErrMinInt.Error())
	assertions.Equal(0, num)

	num, _ = list.LRem(key, 0, []byte("item_not_exists"))
	assertions.Equal(0, num)
}

func TestList_LRemNum(t *testing.T) {
	list, key := InitListData()
	assertions := assert.New(t)

	num, err := list.LRemNum("key_not_exists", 0, nil)
	assertions.Equal(0, num)
	assertions.Error(err)

	num, err = list.LRemNum(key, math2.MaxInt, nil)
	assertions.Equal(0, num)
	assertions.Error(err)
}

func TestList_LRemByIndex(t *testing.T) {
	list, key := InitListData()
	assertions := assert.New(t)

	tests := []struct {
		name           string
		key            string
		item           [][]byte
		indexes        []int
		wantItem       [][]byte
		wantRemovedNum int
		wantErr        bool
	}{
		{
			"exception-1",
			"key2",
			[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
			[]int{},
			[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
			0,
			true,
		},
		{
			"normal-1",
			key,
			[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
			[]int{},
			[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
			0,
			false,
		},
		{
			"normal-2",
			key,
			[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
			[]int{0},
			[][]byte{[]byte("b"), []byte("c"), []byte("d")},
			1,
			false,
		},
		{
			"normal-3",
			key,
			[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
			[]int{0, 0, 0},
			[][]byte{[]byte("b"), []byte("c"), []byte("d")},
			1,
			false,
		},
		{
			"normal-4",
			key,
			[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
			[]int{0, 0, 2, 8},
			[][]byte{[]byte("b"), []byte("d")},
			2,
			false,
		},
		{
			"normal-5",
			key,
			[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
			[]int{-1, 0, 1, 2, 3, 4, 5},
			[][]byte{},
			4,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list.Items[key] = tt.item
			removedNum, err := list.LRemByIndexPreCheck(tt.key, tt.indexes)
			assertions.Equal(tt.wantErr, err != nil, "TestList_LRemByIndexNum err")
			assertions.Equal(tt.wantRemovedNum, removedNum, "TestList_LRemByIndexNum %v err", tt.name)

			removedNum, err = list.LRemByIndex(tt.key, tt.indexes)
			assertions.Equal(tt.wantErr, err != nil, "TestList_LRemByIndex err")
			assertions.ElementsMatchf(tt.wantItem, list.Items[key], "TestList_LRemByIndex %v err", tt.name)
			assertions.Equal(tt.wantRemovedNum, removedNum, "TestList_LRemByIndex %v err", tt.name)
		})
	}
}

func TestList_LSet(t *testing.T) {
	list, key := InitListData()
	assertions := assert.New(t)
	assertions.Equal("a", string(list.Items[key][0]), "TestList_LSet err")

	err := list.LSet(key, 0, []byte("a1"))
	assertions.NoError(err)
	assertions.Equal("a1", string(list.Items[key][0]), "TestList_LSet err")

	err = list.LSet("key_fake", 0, []byte("a1"))
	assertions.Error(err, "TestList_LSet err")

	err = list.LSet(key, 4, []byte("a1"))
	assertions.Error(err, "TestList_LSet err")

	err = list.LSet(key, -1, []byte("a1"))
	assertions.Error(err, "TestList_LSet err")
}

func TestList_Ltrim(t *testing.T) {
	list, key := InitListData()
	assertions := assert.New(t)
	expectResult := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	assertions.ElementsMatchf(expectResult, list.Items[key], "TestList_Ltrim err")

	err := list.Ltrim(key, 0, 2)
	expectResult = [][]byte{[]byte("a"), []byte("b"), []byte("c")}

	assertions.Nil(err, "TestList_Ltrim err")
	assertions.ElementsMatchf(expectResult, list.Items[key], "TestList_Ltrim err")

	err = list.Ltrim("key_fake", 0, 2)
	assertions.Error(err, "TestList_Ltrim err")

	err = list.Ltrim(key, -1, -2)
	assertions.Error(err, "TestList_Ltrim err")
}

func TestList_IsEmpty(t *testing.T) {
	nonEmptyList, nonEmptyKey := InitListData()
	emptyList, emptyKey := New(), "empty"
	assertions := assert.New(t)

	r, err := nonEmptyList.IsEmpty(nonEmptyKey)
	assertions.Nil(err, "TestList_IsEmpty non-empty err")
	assertions.Equal(false, r, "IsEmpty failed on non-empty list")

	r, err = emptyList.IsEmpty(emptyKey)
	assertions.Equal(ErrListNotFound, err, "TestList_IsEmpty got no err")
	assertions.Equal(false, r, "IsEmpty expect an error here")

	_, err = emptyList.RPush(emptyKey, []byte("a"))
	_, err = emptyList.LPop(emptyKey)
	r, err = emptyList.IsEmpty(emptyKey)
	assertions.Nil(err, "TestList_IsEmpty empty err")
	assertions.Equal(true, r, "IsEmpty failed on empty list")
}
