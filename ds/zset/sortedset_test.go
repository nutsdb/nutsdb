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
package zset

import (
	"testing"
)

var ss *SortedSet

func InitData() {
	ss = New()
	ss.Put("key1", 1, []byte("a"))
	ss.Put("key2", 10, []byte("b"))
	ss.Put("key3", 99.9, []byte("c"))
	ss.Put("key4", 100, []byte("d"))
	ss.Put("key5", 100, []byte("b1"))
}

func TestSortedSet_Put(t *testing.T) {
	InitData()

	if ss.Size() != 5 {
		t.Error("TestSortedSet_Put err")
	}

	// update
	ss.Put("key5", 100, []byte("b2"))
	n := ss.GetByKey("key5")
	if string(n.Value) != "b2" {
		t.Error("TestSortedSet_Put err")
	}

	ss.Put("key5", 91, []byte("b2"))

	n = ss.GetByKey("key5")
	if n.score != 91 {
		t.Error("TestSortedSet_Put err")
	}
}

func TestSortedSet_GetByKey(t *testing.T) {
	InitData()
	n := ss.GetByKey("key1")
	if string(n.Value) != "a" {
		t.Error("TestSortedSet_GetByKey err")
	}
}

func TestSortedSet_GetByRank(t *testing.T) {
	InitData()
	n := ss.GetByRank(1, false)
	if string(n.Value) != "a" {
		t.Error("TestSortedSet_GetByRank err")
	}

	n = ss.GetByRank(4, false)
	if string(n.Value) != "d" {
		t.Error("TestSortedSet_GetByRank err")
	}

	n = ss.GetByRank(5, false)
	if string(n.Value) != "b1" {
		t.Error("TestSortedSet_GetByRank err")
	}

	if ss.Size() != 5 {
		t.Error("TestSortedSet_GetByRank err")
	}

	// remove
	n = ss.GetByRank(5, true)
	if string(n.Value) != "b1" {
		t.Error("TestSortedSet_GetByRank err")
	}
	if ss.Size() != 4 {
		t.Error("TestSortedSet_GetByRank err")
	}

	n = ss.GetByRank(-1, false)
	if n.key != "key4" {
		t.Error("TestSortedSet_GetByRank err")
	}

	n = ss.GetByRank(-3, false)
	if n.key != "key2" {
		t.Error("TestSortedSet_GetByRank err")
	}
}

func TestSortedSet_GetByRankRange(t *testing.T) {
	resultSet := getResultSet("key1", "key2")
	InitData()

	for _, n := range ss.GetByRankRange(1, 2, false) {
		if _, ok := resultSet[n.key]; !ok {
			t.Error("TestSortedSet_GetByRankRange err")
		}
	}

	resultSet = getResultSet("key5", "key4")

	for _, n := range ss.GetByRankRange(-1, -2, false) {
		if _, ok := resultSet[n.key]; !ok {
			t.Error("TestSortedSet_GetByRankRange err")
		}
	}

	resultSet = getResultSet("key5", "key4", "key3", "key2", "key1")

	for _, n := range ss.GetByRankRange(-1, 1, false) {
		if _, ok := resultSet[n.key]; !ok {
			t.Error("TestSortedSet_GetByRankRange err")
		}
	}

	for _, n := range ss.GetByRankRange(1, -1, false) {
		if _, ok := resultSet[n.key]; !ok {
			t.Error("TestSortedSet_GetByRankRange err")
		}
	}

	ss = New()
	ss.Put("key1", 1, []byte("a"))

	resultSet = getResultSet("key1")
	for _, n := range ss.GetByRankRange(-1, -2, false) {
		if _, ok := resultSet[n.key]; !ok {
			t.Error("TestSortedSet_GetByRankRange err")
		}
	}

	for _, n := range ss.GetByRankRange(-2, -1, false) {
		if _, ok := resultSet[n.key]; !ok {
			t.Error("TestSortedSet_GetByRankRange err")
		}
	}
}

func TestSortedSet_FindRank(t *testing.T) {
	var rank int
	ss = New()
	ss.Put("key0", 0, []byte("a0"))
	if rank = ss.FindRank("key1"); rank != 0 {
		t.Error("TestSortedSet_FindRank err")
	}

	InitData()

	if rank = ss.FindRank("key1"); rank != 1 {
		t.Error("TestSortedSet_FindRank err")
	}

	if rank = ss.FindRank("key4"); rank != 4 {
		t.Error("TestSortedSet_FindRank err")
	}

	if rank = ss.FindRank("key5"); rank != 5 {
		t.Error("TestSortedSet_FindRank err")
	}
}

func TestSortedSet_FindRevRank(t *testing.T) {
	var rank int
	ss = New()

	if rank = ss.FindRevRank("key1"); rank != 0 {
		t.Error("TestSortedSet_FindRevRank err")
	}
	ss.Put("key0", 0, []byte("a0"))

	if rank = ss.FindRevRank("key1"); rank != 0 {
		t.Error("TestSortedSet_FindRevRank err")
	}

	InitData()

	if rank = ss.FindRevRank("key1"); rank != 5 {
		t.Error("TestSortedSet_FindRank err")
	}

	if rank = ss.FindRevRank("key2"); rank != 4 {
		t.Error("TestSortedSet_FindRank err")
	}

	if rank = ss.FindRevRank("key3"); rank != 3 {
		t.Error("TestSortedSet_FindRank err")
	}

	if rank = ss.FindRevRank("key5"); rank != 1 {
		t.Error("TestSortedSet_FindRank err")
	}
}

func TestSortedSet_GetByScoreRange(t *testing.T) {
	InitData()
	resultSet := getResultSet("key5", "key4", "key3")

	for _, n := range ss.GetByScoreRange(90, 100, nil) {
		if _, ok := resultSet[n.key]; !ok {
			t.Error("TestSortedSet_GetByScoreRange err")
		}
	}

	resultSet = getResultSet("key3")
	for _, n := range ss.GetByScoreRange(90, 100, &GetByScoreRangeOptions{
		ExcludeEnd: true,
	}) {
		if _, ok := resultSet[n.key]; !ok {
			t.Error("TestSortedSet_GetByScoreRange err")
		}
	}

	resultSet = getResultSet("key5", "key4", "key3")

	for _, n := range ss.GetByScoreRange(10, 100, &GetByScoreRangeOptions{
		ExcludeStart: true,
	}) {
		if _, ok := resultSet[n.key]; !ok {
			t.Error("TestSortedSet_GetByScoreRange err")
		}
	}

	resultSet = getResultSet("key3")
	for _, n := range ss.GetByScoreRange(10, 100, &GetByScoreRangeOptions{
		ExcludeStart: true,
		ExcludeEnd:   true,
	}) {
		if _, ok := resultSet[n.key]; !ok {
			t.Error("TestSortedSet_GetByScoreRange err")
		}
	}
}

func TestSortedSet_GetByScoreRange2(t *testing.T) {
	InitData()
	resultSet := getResultSet("key2", "key3", "key4")
	for _, n := range ss.GetByScoreRange(10, 100, &GetByScoreRangeOptions{
		Limit: 3,
	}) {
		if _, ok := resultSet[n.key]; !ok {
			t.Error("TestSortedSet_GetByScoreRange err")
		}
	}

	resultSet = getResultSet("key4", "key5")
	for _, n := range ss.GetByScoreRange(100, 99.999, nil) {
		if _, ok := resultSet[n.key]; !ok {
			t.Error("TestSortedSet_GetByScoreRange err")
		}
	}

	resultSet = getResultSet("key4", "key5")
	for _, n := range ss.GetByScoreRange(100, 99.9, &GetByScoreRangeOptions{
		ExcludeEnd: true,
	}) {
		if _, ok := resultSet[n.key]; !ok {
			t.Error("TestSortedSet_GetByScoreRange err")
		}
	}

	resultSet = getResultSet("key3")

	for _, n := range ss.GetByScoreRange(100, 99.9, &GetByScoreRangeOptions{
		ExcludeStart: true,
	}) {
		if _, ok := resultSet[n.key]; !ok {
			t.Error("TestSortedSet_GetByScoreRange err")
		}
	}
}

func TestSortedSet_PeekMax(t *testing.T) {
	InitData()

	n := ss.PeekMax()
	if n.Key() != "key5" || n.Score() != 100 {
		t.Error("TestSortedSet_PeekMax err")
	}
}

func TestSortedSet_PeekMin(t *testing.T) {
	InitData()

	n := ss.PeekMin()
	if n.key != "key1" {
		t.Error("TestSortedSet_PeekMax err")
	}
}

func TestSortedSet_PopMin(t *testing.T) {
	InitData()

	n := ss.PopMin()
	if n.key != "key1" {
		t.Error("TestSortedSet_PopMin err")
	}

	resultSet := getResultSet("key2", "key3", "key4", "key5")

	for key := range ss.Dict {
		if _, ok := resultSet[key]; !ok {
			t.Error("TestSortedSet_PopMin err")
		}
	}
}

func TestSortedSet_PopMax(t *testing.T) {
	InitData()

	n := ss.PopMax()
	if n.key != "key5" {
		t.Error("TestSortedSet_PopMax err")
	}

	resultSet := getResultSet("key1", "key2", "key3", "key4")
	for key := range ss.Dict {
		if _, ok := resultSet[key]; !ok {
			t.Error("TestSortedSet_PopMax err")
		}
	}
}

func TestSortedSet_Remove(t *testing.T) {
	InitData()

	ss.Remove("key1")

	resultSet := getResultSet("key2", "key3", "key4", "key5")

	for key := range ss.Dict {
		if _, ok := resultSet[key]; !ok {
			t.Error("TestSortedSet_Remove err")
		}
	}
}

func TestSortedSet_Size(t *testing.T) {
	InitData()

	if ss.Size() != 5 {
		t.Error("TestSortedSet_Size err")
	}
}

func getResultSet(items ...string) map[string]struct{} {
	resultSet := make(map[string]struct{}, len(items))

	for _, item := range items {
		resultSet[item] = struct{}{}
	}

	return resultSet
}
