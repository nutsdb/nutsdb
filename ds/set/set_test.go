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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet_SAdd(t *testing.T) {
	mySet := New()
	assertions := assert.New(t)
	key := "mySet0"

	assertions.NoError(mySet.SAdd(key, []byte("Hello")))
	assertions.NoError(mySet.SAdd(key, []byte("World")))
	assertions.NoError(mySet.SAdd(key, []byte("hello1"), []byte("hello2")))

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

	assertions := assert.New(t)

	assertions.NoError(mySet.SAdd(key1, []byte("a")))

	assertions.NoError(mySet.SAdd(key1, []byte("b")))

	assertions.NoError(mySet.SAdd(key1, []byte("c")))

	assertions.NoError(mySet.SAdd(key2, []byte("d")))

	assertions.NoError(mySet.SAdd(key2, []byte("c")))
	assertions.NoError(mySet.SAdd(key2, []byte("e")))

	assertions.NoError(mySet.SAdd(key3, []byte("a")))

	assertions.NoError(mySet.SAdd(key3, []byte("b")))
	assertions.NoError(mySet.SAdd(key3, []byte("c")))

	assertions.NoError(mySet.SAdd(key4, []byte("a")))
	assertions.NoError(mySet.SAdd(key4, []byte("b")))
	assertions.NoError(mySet.SAdd(key4, []byte("c")))

	assertions.NoError(mySet.SAdd(key4, []byte("d")))
	assertions.NoError(mySet.SAdd(key4, []byte("e")))
	assertions.NoError(mySet.SAdd(key4, []byte("f")))

	assertions.NoError(mySet.SAdd(key5, []byte("b")))

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
			assertions.ElementsMatchf(got, tt.want, "Get() got = %v, want %v", got, tt.want)
		})
	}

}

func TestSet_SCard(t *testing.T) {
	mySet := New()
	key1 := "mySet1"
	key2 := "mySet2"
	key3 := "mySet3"
	assertions := assert.New(t)
	assertions.NoError(mySet.SAdd(key1, []byte("1")))
	assertions.NoError(mySet.SAdd(key1, []byte("2")))
	assertions.NoError(mySet.SAdd(key1, []byte("3")))

	assertions.NoError(mySet.SAdd(key2, []byte("1")))
	assertions.NoError(mySet.SAdd(key2, []byte("2")))
	assertions.NoError(mySet.SAdd(key2, []byte("3")))

	assertions.NoError(mySet.SAdd(key2, []byte("4")))
	assertions.NoError(mySet.SAdd(key2, []byte("5")))
	assertions.NoError(mySet.SAdd(key2, []byte("6")))

	assertions.NoError(mySet.SAdd(key3, []byte("1")))

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
			assertions.Equalf(tt.want, got, "TestSet_SCard err")
		})
	}

}

func TestSet_SInter(t *testing.T) {
	mySet := New()

	key1 := "mySet5"
	key2 := "mySet6"

	assertions := assert.New(t)

	assertions.NoError(mySet.SAdd(key1, []byte("a")))
	assertions.NoError(mySet.SAdd(key1, []byte("b")))
	assertions.NoError(mySet.SAdd(key1, []byte("c")))

	assertions.NoError(mySet.SAdd(key2, []byte("d")))
	assertions.NoError(mySet.SAdd(key2, []byte("c")))
	assertions.NoError(mySet.SAdd(key2, []byte("e")))

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
			"normal inter",
			args{key1, key2},
			mySet,
			[][]byte{[]byte("c")},
			false,
		},
		{
			"first fake key",
			args{"fake_key1", key2},
			mySet,
			nil,
			true,
		},
		{
			"second fake key",
			args{key1, "fake_key2"},
			mySet,
			nil,
			true,
		},
		{
			"two fake key",
			args{"fake_key1", "fake_key2"},
			mySet,
			nil,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.set.SInter(tt.args.key1, tt.args.key2)
			if (err != nil) != tt.wantErr {
				t.Errorf("SInter() err = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assertions.ElementsMatchf(tt.want, got, "SInter() want = %, got = %v", tt.want, got)
		})
	}
}

func TestSet_SMembers(t *testing.T) {

	mySet := New()

	key := "mySet8"
	assertions := assert.New(t)
	assertions.NoError(mySet.SAdd(key, []byte("v-1")))
	assertions.NoError(mySet.SAdd(key, []byte("v-2")))

	tests := []struct {
		name    string
		key     string
		set     *Set
		want    [][]byte
		wantErr bool
	}{
		{
			"normal SMembers",
			key,
			mySet,
			[][]byte{[]byte("v-2")},
			false,
		},
		{
			"normal SMembers",
			key,
			mySet,
			[][]byte{[]byte("v-2")},
			false,
		},
		{
			"normal SMembers",
			key,
			mySet,
			[][]byte{[]byte("v-2"), []byte("v-2")},
			false,
		},
		{
			"fake key",
			"fake_key",
			mySet,
			nil,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.set.SMembers(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("SInter() err = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assertions.Subsetf(got, tt.want, "SInter() got = %v, want = %v", got, tt.want)
		})
	}
}

func TestSet_SMove(t *testing.T) {

	mySet := New()

	key1 := "mySet9"
	assertions := assert.New(t)
	assertions.NoError(mySet.SAdd(key1, []byte("a")))
	assertions.NoError(mySet.SAdd(key1, []byte("b")))

	key2 := "mySet10"
	assertions.NoError(mySet.SAdd(key2, []byte("c")))

	type args struct {
		key1 string
		key2 string
		item []byte
	}

	tests := []struct {
		name    string
		args    args
		set     *Set
		want    bool
		want1   [][]byte
		want2   [][]byte
		wantErr bool
	}{
		{
			"normal SMove",
			args{key1, key2, []byte("b")},
			mySet,
			true,
			[][]byte{[]byte("a")},
			[][]byte{[]byte("b"), []byte("c")},
			false,
		},
		{
			"first fake key",
			args{"fake_key1", key2, []byte("b")},
			mySet,
			false,
			nil,
			[][]byte{[]byte("b"), []byte("c")},
			true,
		},
		{
			"second fake key",
			args{key1, "fake_key2", []byte("b")},
			mySet,
			false,
			[][]byte{[]byte("a")},
			nil,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.set.SMove(tt.args.key1, tt.args.key2, tt.args.item)
			if (err != nil) != tt.wantErr {
				t.Errorf("SMove() err = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == tt.want {
				got1, _ := tt.set.SMembers(tt.args.key1)
				got2, _ := tt.set.SMembers(tt.args.key2)
				assertions.ElementsMatchf(got1, tt.want1, "SMove() got = %v, want = %v", got1, tt.want1)
				assertions.ElementsMatchf(got2, tt.want2, "SMove() got = %v, want = %v", got2, tt.want2)
			} else {
				t.Errorf("SMove() got = %v, want = %v", tt.want, got)
			}
		})
	}
}

func TestSet_SPop(t *testing.T) {

	mySet := New()
	assertions := assert.New(t)

	key := "mySet10"

	assertions.NoError(mySet.SAdd(key, []byte("a")))
	assertions.NoError(mySet.SAdd(key, []byte("b")))
	assertions.NoError(mySet.SAdd(key, []byte("c")))

	members, _ := mySet.SMembers(key)

	type args struct {
		key      string
		popCount int
	}

	tests := []struct {
		name    string
		args    args
		set     *Set
		members [][]byte
	}{
		{
			"normal pop",
			args{key, 1},
			mySet,
			members,
		},
		{
			"normal pop",
			args{key, 1},
			mySet,
			members,
		},
		{
			"fake key",
			args{key, 1},
			mySet,
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set.SPop(tt.args.key)
			if got != nil {
				assertions.Containsf(members, got, "SPop() got = %v, not in = %v", got, tt.members)
			}
		})
	}
}

func TestSet_SRem(t *testing.T) {

	mySet := New()
	key := "mySet11"
	assertions := assert.New(t)

	assertions.NoError(mySet.SAdd(key, []byte("a")))
	assertions.NoError(mySet.SAdd(key, []byte("b")))
	assertions.NoError(mySet.SAdd(key, []byte("c")))

	assertions.NoError(mySet.SRem(key, []byte("a")))
	assertions.NoError(mySet.SRem(key, []byte("b")))

	assertions.False(mySet.SIsMember(key, []byte("a")), "TestSet_SRem err")

	assertions.False(mySet.SIsMember(key, []byte("b")), "TestSet_SRem err")

	assertions.Error(mySet.SRem("key_fake", []byte("b")), "TestSet_SRem err")

	assertions.Error(mySet.SRem(key, []byte("")), "TestSet_SRem err")
}

func TestSet_SUnion(t *testing.T) {
	mySet := New()
	key1 := "mySet12"
	assertions := assert.New(t)
	assertions.NoError(mySet.SAdd(key1, []byte("a")))
	assertions.NoError(mySet.SAdd(key1, []byte("b")))
	assertions.NoError(mySet.SAdd(key1, []byte("c")))

	key2 := "mySet12"
	assertions.NoError(mySet.SAdd(key2, []byte("c")))
	assertions.NoError(mySet.SAdd(key2, []byte("d")))
	assertions.NoError(mySet.SAdd(key2, []byte("e")))

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
			"normal",
			args{key1, key2},
			mySet,
			[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")},
			false,
		},
		{
			"fake key",
			args{"fake key", key2},
			mySet,
			nil,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.set.SUnion(tt.args.key1, tt.args.key2)
			if (err != nil) != tt.wantErr {
				t.Errorf("SUnion() err = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assertions.ElementsMatchf(got, tt.want, "SUnion() got = %v, want = %v", got, tt.want)
		})
	}
}
