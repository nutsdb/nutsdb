// Copyright 2019 The nutsdb Authors. All rights reserved.
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

	expectResult := []string{"f", "e", "d", "c", "b", "a"}
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

	if err != nil || string(item) != "a" {
		t.Error("TestList_LPop err")
	}

	item, err = list.LPop("key_fake")
	if err == nil || item != nil {
		t.Error("TestList_LPop err")
	}

	item, err = list.LPop(key)
	if err != nil || item == nil {
		t.Error("TestList_LPop err")
	}

	item, err = list.LPop(key)
	if err != nil || item == nil {
		t.Error("TestList_LPop err")
	}

	item, err = list.LPop(key)
	if err != nil || item == nil {
		t.Error("TestList_LPop err")
	}

	item, err = list.LPop(key)
	if err == nil || item != nil {
		t.Error("TestList_LPop err")
	}
}

func TestList_RPop(t *testing.T) {
	list, key := InitListData()
	if size, err := list.Size(key); err != nil && size != 4 {
		t.Error("TestList_RPop err")
	}

	item, err := list.RPop(key)
	if size, err := list.Size(key); err != nil && size != 3 {
		t.Error("TestList_RPop err")
	}
	if err != nil || string(item) != "d" {
		t.Error("TestList_RPop err")
	}

	item, err = list.RPop("key_fake")
	if err == nil || item != nil {
		t.Error("TestList_RPop err")
	}
}

func TestList_LRange(t *testing.T) {
	list, key := InitListData()
	list.RPush(key, []byte("e"))
	list.RPush(key, []byte("f"))

	items, err := list.LRange(key, 0, 2)
	if err != nil {
		t.Error("TestList_LRange err")
	}

	expectResult := make(map[string]struct{}, 3)
	expectResult["a"] = struct{}{}
	expectResult["b"] = struct{}{}
	expectResult["c"] = struct{}{}
	for _, item := range items {
		if _, ok := expectResult[string(item)]; !ok {
			t.Error("TestList_LRange err")
		}
	}

	items, err = list.LRange(key, 4, 8)
	if err != nil {
		t.Error("TestList_LRange err")
	}

	expectResult = make(map[string]struct{}, 2)
	expectResult["e"] = struct{}{}
	expectResult["f"] = struct{}{}
	for _, item := range items {
		if _, ok := expectResult[string(item)]; !ok {
			t.Error("TestList_LRange err")
		}
	}

	items, err = list.LRange(key, -1, 2)
	if err == nil || items != nil {
		t.Error("TestList_LRange err")
	}

	items, err = list.LRange(key, -3, -1)
	if err != nil {
		t.Error("TestList_LRange err")
	}

	expectResult = make(map[string]struct{}, 2)
	expectResult["d"] = struct{}{}
	expectResult["e"] = struct{}{}
	expectResult["f"] = struct{}{}
	for _, item := range items {
		if _, ok := expectResult[string(item)]; !ok {
			t.Error("TestList_LRange err")
		}
	}

	items, err = list.LRange(key, -1, -2)
	if err == nil || items != nil {
		t.Error("TestList_LRange err")
	}

	items, err = list.LRange("key_fake", -2, -1)
	if err == nil || items != nil {
		t.Error("TestList_LRange err")
	}

	items, err = list.LRange(key, 0, 0)
	if string(items[0]) != "a" || err != nil {
		t.Error("TestList_LRange err")
	}

	items, err = list.LRange(key, 0, -1)
	if len(items) != 6 || err != nil {
		t.Error("TestList_LRange err")
	}
}

func TestList_LRem(t *testing.T) {
	list, key := InitListData()

	num, err := list.LRem("key_fake", 1)
	if err == nil || num != 0 {
		t.Error("TestList_LRem err")
	}

	num, err = list.LRem(key, 1)
	if err != nil && num != 1 {
		t.Error("TestList_LRem err")
	}
	expectResult := make(map[string]struct{}, 2)
	expectResult["b"] = struct{}{}
	expectResult["c"] = struct{}{}
	expectResult["d"] = struct{}{}

	items, err := list.LRange(key, 0, -1)
	if err != nil {
		t.Error("TestList_LRem err")
	}
	for _, item := range items {
		if _, ok := expectResult[string(item)]; !ok {
			t.Error("TestList_LRem err")
		}
	}

	num, err = list.LRem(key, 0)
	if err != nil && num != 3 {
		t.Error("TestList_LRem err")
	}
	size, err := list.Size(key)
	if err == nil || size != 0 {
		t.Error("TestList_LRem err")
	}

	list, key = InitListData()

	num, err = list.LRem(key, -1)
	items, err = list.LRange(key, 0, -1)
	if err != nil || items == nil {
		t.Error("TestList_LRem err")
	}

	expectResult = make(map[string]struct{}, 3)
	expectResult["a"] = struct{}{}
	expectResult["b"] = struct{}{}
	expectResult["c"] = struct{}{}
	for _, item := range items {
		if _, ok := expectResult[string(item)]; !ok {
			t.Error("TestList_LRem err")
		}
	}
	num, err = list.LRem(key, -2)
	if num != 2 || err != nil {
		t.Error("TestList_LRem err")
	}
	items, err = list.LRange(key, 0, -1)
	if err != nil {
		t.Error("TestList_LRem err")
	}

	expectResult = make(map[string]struct{}, 1)
	expectResult["a"] = struct{}{}
	for _, item := range items {
		if _, ok := expectResult[string(item)]; !ok {
			t.Error("TestList_LRem err")
		}
	}

	list, key = InitListData()
	num, err = list.LRem(key, -10)
	if err == nil || num != 0 {
		t.Error("TestList_LRem err")
	}

	list, key = InitListData()
	num, err = list.LRem(key, 4)
	if err == nil || num != 0 {
		t.Error("TestList_LRem err")
	}

	list, key = InitListData()
	num, err = list.LRem(key, 3)
	if err != nil || num == 0 {
		t.Error(err)
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

	expectResult := make(map[string]struct{}, 1)
	expectResult["a"] = struct{}{}
	expectResult["b"] = struct{}{}
	expectResult["c"] = struct{}{}
	expectResult["d"] = struct{}{}

	for _, item := range list.Items[key] {
		if _, ok := expectResult[string(item)]; !ok {
			t.Error("TestList_Ltrim err")
		}
	}

	err := list.Ltrim(key, 0, 2)
	if err != nil {
		t.Error("TestList_Ltrim err")
	}

	expectResult = make(map[string]struct{}, 1)
	expectResult["a"] = struct{}{}
	expectResult["b"] = struct{}{}
	expectResult["c"] = struct{}{}
	for _, item := range list.Items[key] {
		if _, ok := expectResult[string(item)]; !ok {
			t.Error("TestList_Ltrim err")
		}
	}

	err = list.Ltrim("key_fake", 0, 2)
	if err == nil {
		t.Error("TestList_Ltrim err")
	}

	err = list.Ltrim(key, -1, -2)
	if err == nil {
		t.Error("TestList_Ltrim err")
	}
}
