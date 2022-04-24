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

	"github.com/stretchr/testify/assert"
)

var ss *SortedSet

func InitData(t *testing.T) {
	ss = New()
	assertions := assert.New(t)
	assertions.NoError(ss.Put("key1", 1, []byte("a")))
	assertions.NoError(ss.Put("key2", 10, []byte("b")))
	assertions.NoError(ss.Put("key3", 99.9, []byte("c")))
	assertions.NoError(ss.Put("key4", 100, []byte("d")))
	assertions.NoError(ss.Put("key5", 100, []byte("b1")))
}

func TestSortedSet_Put(t *testing.T) {
	InitData(t)
	assertions := assert.New(t)
	assertions.Equal(5, ss.Size(), "TestSortedSet_Put err")

	// update
	assertions.NoError(ss.Put("key5", 100, []byte("b2")))
	n := ss.GetByKey("key5")
	assertions.Equal("b2", string(n.Value), "TestSortedSet_Put err")

	assertions.NoError(ss.Put("key5", 91, []byte("b2")))

	n = ss.GetByKey("key5")
	assertions.EqualValues(91, n.score, "TestSortedSet_Put err") // socre is zset.Score
}

func TestSortedSet_GetByKey(t *testing.T) {
	InitData(t)
	assertions := assert.New(t)
	n := ss.GetByKey("key1")
	assertions.Equal("a", string(n.Value), "TestSortedSet_GetByKey err")
}

func TestSortedSet_GetByRank(t *testing.T) {
	InitData(t)
	assertions := assert.New(t)
	n := ss.GetByRank(1, false)
	assertions.Equal("a", string(n.Value), "TestSortedSet_GetByRank err")

	n = ss.GetByRank(4, false)

	assertions.Equal("d", string(n.Value), "TestSortedSet_GetByRank err")

	n = ss.GetByRank(5, false)

	assertions.Equal("b1", string(n.Value), "TestSortedSet_GetByRank err")
	assertions.Equal(5, ss.Size(), "TestSortedSet_GetByRank err")

	// remove
	n = ss.GetByRank(5, true)

	assertions.Equal("b1", string(n.Value), "TestSortedSet_GetByRank err")
	assertions.Equal(4, ss.Size(), "TestSortedSet_GetByRank err")

	n = ss.GetByRank(-1, false)

	assertions.Equal("key4", n.key, "TestSortedSet_GetByRank err")

	n = ss.GetByRank(-3, false)

	assertions.Equal("key2", n.key, "TestSortedSet_GetByRank err")
}

func TestSortedSet_GetByRankRange(t *testing.T) {
	assertions := assert.New(t)

	InitData(t)

	type args struct {
		start  int
		end    int
		remove bool
	}

	tests := []struct {
		name string
		args *args
		ss   *SortedSet
		want []string
	}{
		{
			"normal-1",
			&args{1, 2, false},
			ss,
			[]string{"key1", "key2"},
		},
		{
			"normal-2 reverse",
			&args{-1, -2, false},
			ss,
			[]string{"key5", "key4"},
		},
		{
			"normal-1",
			&args{-2, -1, false},
			ss,
			[]string{"key4", "key5"},
		},
		{
			"normal-1 reverse",
			&args{-1, 1, false},
			ss,
			[]string{"key5", "key4", "key3", "key2", "key1"},
		},
		{
			"normal",
			&args{1, -1, false},
			ss,
			[]string{"key1", "key2", "key3", "key4", "key5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := make([]string, 0)
			for _, n := range ss.GetByRankRange(tt.args.start, tt.args.end, tt.args.remove) {
				got = append(got, n.key)
			}
			assertions.Equal(tt.want, got, "TestSortedSet_GetByScoreRange err")
		})
	}
}

func TestSortedSet_FindRank(t *testing.T) {
	//var rank int
	ss = New()
	assertions := assert.New(t)
	assertions.NoError(ss.Put("key0", 0, []byte("a0")))

	assertions.Equal(0, ss.FindRank("key1"), "TestSortedSet_FindRank err")

	InitData(t)

	assertions.Equal(1, ss.FindRank("key1"), "TestSortedSet_FindRank err")

	assertions.Equal(4, ss.FindRank("key4"), "TestSortedSet_FindRank err")

	assertions.Equal(5, ss.FindRank("key5"), "TestSortedSet_FindRank err")

}

func TestSortedSet_FindRevRank(t *testing.T) {

	ss = New()
	assertions := assert.New(t)

	assertions.Equal(0, ss.FindRevRank("key1"), "TestSortedSet_FindRevRank err")

	assertions.NoError(ss.Put("key0", 0, []byte("a0")))

	assertions.Equal(0, ss.FindRevRank("key1"), "TestSortedSet_FindRevRank err")

	InitData(t)

	assertions.Equal(5, ss.FindRevRank("key1"), "TestSortedSet_FindRevRank err")

	assertions.Equal(4, ss.FindRevRank("key2"), "TestSortedSet_FindRevRank err")

	assertions.Equal(3, ss.FindRevRank("key3"), "TestSortedSet_FindRevRank err")

	assertions.Equal(1, ss.FindRevRank("key5"), "TestSortedSet_FindRevRank err")
}

func TestSortedSet_GetByScoreRange(t *testing.T) {
	InitData(t)
	assertions := assert.New(t)

	type args struct {
		start   SCORE
		end     SCORE
		options *GetByScoreRangeOptions
	}

	tests := []struct {
		name string
		args *args
		ss   *SortedSet
		want []string
	}{
		{
			"normal-1",
			&args{90, 100, nil},
			ss,
			[]string{"key5", "key4", "key3"},
		},
		{
			"normal-2 options",
			&args{90, 100, &GetByScoreRangeOptions{ExcludeEnd: true}},
			ss,
			[]string{"key3"},
		},
		{
			"normal-3 options",
			&args{10, 100, &GetByScoreRangeOptions{ExcludeStart: true}},
			ss,
			[]string{"key5", "key4", "key3"},
		},
		{
			"normal-4 options",
			&args{10, 100, &GetByScoreRangeOptions{ExcludeStart: true, ExcludeEnd: true}},
			ss,
			[]string{"key3"},
		},
		{
			"normal-5 options",
			&args{10, 100, &GetByScoreRangeOptions{Limit: 3}},
			ss,
			[]string{"key2", "key3", "key4"},
		},
		{
			"normal-6 reverse",
			&args{100, 99.999, nil},
			ss,
			[]string{"key4", "key5"},
		},
		{
			"normal-7 reverse",
			&args{100, 99.9, &GetByScoreRangeOptions{
				ExcludeEnd: true,
			}},
			ss,
			[]string{"key5", "key4"},
		},
		{
			"normal-8 reverse",
			&args{100, 99.9, &GetByScoreRangeOptions{
				ExcludeStart: true,
			}},
			ss,
			[]string{"key3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := make([]string, 0)
			for _, n := range ss.GetByScoreRange(tt.args.start, tt.args.end, tt.args.options) {
				got = append(got, n.key)
			}
			assertions.ElementsMatch(tt.want, got, "TestSortedSet_GetByScoreRange err")
		})
	}
}

func TestSortedSet_PeekMax(t *testing.T) {
	InitData(t)
	assertions := assert.New(t)

	n := ss.PeekMax()

	assertions.Equal("key5", n.Key(), "TestSortedSet_PeekMax err")
	assertions.EqualValues(100, n.Score(), "TestSortedSet_PeekMax err")
}

func TestSortedSet_PeekMin(t *testing.T) {
	InitData(t)
	assertions := assert.New(t)

	n := ss.PeekMin()

	assertions.Equal("key1", n.key, "TestSortedSet_PeekMax err")
}

func TestSortedSet_PopMin(t *testing.T) {
	InitData(t)
	assertions := assert.New(t)

	n := ss.PopMin()

	assertions.Equal("key1", n.key, "TestSortedSet_PopMin err")
	resultSet := getResultSet("key2", "key3", "key4", "key5")

	for key := range ss.Dict {
		if _, ok := resultSet[key]; !ok {
			t.Error("TestSortedSet_PopMin err")
		}
	}

}

func TestSortedSet_PopMax(t *testing.T) {
	InitData(t)
	assertions := assert.New(t)

	n := ss.PopMax()

	assertions.Equal("key5", n.key, "TestSortedSet_PopMax err")
	resultSet := getResultSet("key1", "key2", "key3", "key4")
	for key := range ss.Dict {
		if _, ok := resultSet[key]; !ok {
			t.Error("TestSortedSet_PopMax err")
		}
	}
}

func TestSortedSet_Remove(t *testing.T) {
	InitData(t)

	ss.Remove("key1")

	resultSet := getResultSet("key2", "key3", "key4", "key5")

	for key := range ss.Dict {
		if _, ok := resultSet[key]; !ok {
			t.Error("TestSortedSet_Remove err")
		}
	}
}

func TestSortedSet_Size(t *testing.T) {
	InitData(t)
	assertions := assert.New(t)
	assertions.Equal(5, ss.Size(), "TestSortedSet_Size err")
}

func getResultSet(items ...string) map[string]struct{} {
	resultSet := make(map[string]struct{}, len(items))

	for _, item := range items {
		resultSet[item] = struct{}{}
	}

	return resultSet
}
