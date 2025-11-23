// Copyright 2023 The nutsdb Author. All rights reserved.
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

package data

import (
	"testing"

	"github.com/nutsdb/nutsdb/internal/testutils"
	"github.com/stretchr/testify/require"
)

func TestSet_SAdd(t *testing.T) {
	set := NewSet()
	key := "key"
	expectRecords := GenerateRecords(3)

	values := make([][]byte, 3)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, set.SAdd(key, values, expectRecords))

	ok, err := set.SAreMembers(key, values...)
	require.True(t, ok)
	require.NoError(t, err)
}

func TestSet_SRem(t *testing.T) {
	set := NewSet()
	key := "key"
	expectRecords := GenerateRecords(4)
	values := make([][]byte, 4)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, set.SAdd(key, values, expectRecords))

	require.NoError(t, set.SRem(key, values[0]))
	require.NoError(t, set.SRem(key, values[1:]...))

	require.NoError(t, set.SAdd(key, values, expectRecords))
	require.NoError(t, set.SRem(key, values...))

	require.Error(t, set.SRem("fake key", values...))
	require.Error(t, set.SRem(key, nil))
}

func TestSet_SDiff(t *testing.T) {
	set := NewSet()

	key1 := "set1"
	key2 := "set2"
	key3 := "set3"
	key4 := "set4"

	expectRecords := GenerateRecords(10)

	values := make([][]byte, 10)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, set.SAdd(key1, values[:5], expectRecords[:5]))
	require.NoError(t, set.SAdd(key2, values[2:6], expectRecords[2:6]))
	require.NoError(t, set.SAdd(key3, values[2:7], expectRecords[2:7]))
	require.NoError(t, set.SAdd(key4, values[7:], expectRecords[7:]))

	type args struct {
		key1 string
		key2 string
	}

	tests := []struct {
		name    string
		args    args
		set     *Set
		want    []*Record
		wantErr bool
	}{
		{"normal set diff1", args{key1, key2}, set, expectRecords[:2], false},
		{"normal set diff2", args{key1, key4}, set, expectRecords[:5], false},
		{"normal set diff3", args{key3, key2}, set, expectRecords[6:7], false},
		{"first fake set", args{"fake_key1", key2}, set, nil, true},
		{"second fake set", args{key1, "fake_key2"}, set, nil, true},
		{"two fake set", args{"fake_key1", "fake_key2"}, set, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.set.SDiff(tt.args.key1, tt.args.key2)
			if (err != nil) != tt.wantErr {
				require.Errorf(t, err, "Get() error = %v, wantErr %v", tt.wantErr)
			}
			require.ElementsMatchf(t, tt.want, got, "Get() got = %v, want %v", got, tt.want)
		})
	}
}

func TestSet_SCard(t *testing.T) {
	set := NewSet()

	key1 := "set1"
	key2 := "set2"
	key3 := "set3"
	key4 := "set4"

	expectRecords := GenerateRecords(10)

	values := make([][]byte, 10)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, set.SAdd(key1, values[:5], expectRecords[:5]))
	require.NoError(t, set.SAdd(key2, values[2:6], expectRecords[2:6]))
	require.NoError(t, set.SAdd(key3, values[2:7], expectRecords[2:7]))
	require.NoError(t, set.SAdd(key4, values[7:], expectRecords[7:]))

	tests := []struct {
		name string
		key  string
		set  *Set
		want int
	}{
		{"normal set", key1, set, 5},
		{"normal set", key2, set, 4},
		{"normal set", key3, set, 5},
		{"fake key", "key_fake", set, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set.SCard(tt.key)
			require.Equalf(t, tt.want, got, "TestSet_SCard err")
		})
	}
}

func TestSet_SInter(t *testing.T) {
	set := NewSet()

	key1 := "set1"
	key2 := "set2"
	key3 := "set3"
	key4 := "set4"

	expectRecords := GenerateRecords(10)

	values := make([][]byte, 10)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, set.SAdd(key1, values[:5], expectRecords[:5]))
	require.NoError(t, set.SAdd(key2, values[2:6], expectRecords[2:6]))
	require.NoError(t, set.SAdd(key3, values[2:7], expectRecords[2:7]))
	require.NoError(t, set.SAdd(key4, values[7:], expectRecords[7:]))

	type args struct {
		key1 string
		key2 string
	}

	tests := []struct {
		name    string
		args    args
		set     *Set
		want    []*Record
		wantErr bool
	}{
		{"normal set inter1", args{key1, key2}, set, []*Record{expectRecords[2], expectRecords[3], expectRecords[4]}, false},
		{"normal set inter1", args{key2, key3}, set, []*Record{expectRecords[2], expectRecords[3], expectRecords[4], expectRecords[5]}, false},
		{"normal set inter2", args{key1, key4}, set, nil, false},
		{"first fake set", args{"fake_key1", key2}, set, nil, true},
		{"second fake set", args{key1, "fake_key2"}, set, nil, true},
		{"two fake set", args{"fake_key1", "fake_key2"}, set, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.set.SInter(tt.args.key1, tt.args.key2)
			if (err != nil) != tt.wantErr {
				require.Errorf(t, err, "Get() error = %v, wantErr %v", tt.wantErr)
			}
			require.ElementsMatchf(t, tt.want, got, "Get() got = %v, want %v", got, tt.want)
		})
	}
}

func TestSet_SMembers(t *testing.T) {
	set := NewSet()

	key := "set"

	expectRecords := GenerateRecords(3)

	values := make([][]byte, 3)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, set.SAdd(key, values, expectRecords))

	tests := []struct {
		name    string
		key     string
		set     *Set
		want    []*Record
		wantErr bool
	}{
		{"normal SMembers", key, set, expectRecords[0:1], false},
		{"normal SMembers", key, set, expectRecords[1:], false},
		{"normal SMembers", key, set, expectRecords, false},
		{"fake key", "fake_key", set, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.set.SMembers(tt.key)
			if (err != nil) != tt.wantErr {
				require.Errorf(t, err, "Get() error = %v, wantErr %v", tt.wantErr)
			}
			require.Subsetf(t, got, tt.want, "SInter() got = %v, want = %v", got, tt.want)
		})
	}
}

func TestSet_SMove(t *testing.T) {
	set := NewSet()

	key1 := "set1"
	key2 := "set2"

	expectRecords := GenerateRecords(3)
	values := make([][]byte, 3)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, set.SAdd(key1, values[:2], expectRecords[:2]))
	require.NoError(t, set.SAdd(key2, values[2:], expectRecords[2:]))

	type args struct {
		key1 string
		key2 string
		item []byte
	}

	tests := []struct {
		name      string
		args      args
		set       *Set
		want1     []*Record
		want2     []*Record
		expectErr error
	}{
		{"normal SMove", args{key1, key2, values[1]}, set, expectRecords[0:1], expectRecords[1:], nil},
		{"not exist member SMove", args{key1, key2, values[2]}, set, nil, nil, ErrSetMemberNotExist},
		{"fake key SMove1", args{"fake key", key2, values[2]}, set, nil, nil, ErrSetNotExist},
		{"fake key SMove", args{key1, "fake key", values[2]}, set, nil, nil, ErrSetNotExist},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.set.SMove(tt.args.key1, tt.args.key2, tt.args.item)
			if tt.expectErr != nil {
				require.Error(t, err)
				require.Equal(t, tt.expectErr, err)
			} else {
				got1, _ := tt.set.SMembers(tt.args.key1)
				got2, _ := tt.set.SMembers(tt.args.key2)
				require.ElementsMatchf(t, got1, tt.want1, "SMove() got = %v, want = %v", got1, tt.want1)
				require.ElementsMatchf(t, got2, tt.want2, "SMove() got = %v, want = %v", got2, tt.want2)
			}
		})
	}
}

func TestSet_SPop(t *testing.T) {
	set := NewSet()

	key := "set"

	expectRecords := GenerateRecords(2)
	values := make([][]byte, 2)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}
	m := map[*Record]struct{}{}
	for _, expectRecord := range expectRecords {
		m[expectRecord] = struct{}{}
	}

	require.NoError(t, set.SAdd(key, values, expectRecords))

	tests := []struct {
		name string
		key  string
		set  *Set
		ok   bool
	}{
		{"normal set SPop", key, set, true},
		{"normal set SPop", key, set, true},
		{"normal set SPop", key, set, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := tt.set.SPop(tt.key)
			_, ok := m[record]
			require.Equal(t, tt.ok, ok)
		})
	}
}

func TestSet_SIsMember(t *testing.T) {
	set := NewSet()

	key := "key"

	expectRecords := GenerateRecords(1)
	values := [][]byte{expectRecords[0].Value}

	require.NoError(t, set.SAdd(key, values, expectRecords))
	tests := []struct {
		key       string
		val       []byte
		ok        bool
		expectErr error
	}{
		{key, values[0], true, nil},
		{key, testutils.GetRandomBytes(24), false, nil},
		{"fake key", testutils.GetRandomBytes(24), false, ErrSetNotExist},
	}
	for _, tt := range tests {
		ok, err := set.SIsMember(tt.key, tt.val)
		if tt.expectErr != nil {
			require.Equal(t, tt.expectErr, err)
		} else {
			require.Equal(t, tt.ok, ok)
		}
	}
}

func TestSet_SAreMembers(t *testing.T) {
	set := NewSet()

	key := "set"

	expectRecords := GenerateRecords(4)
	values := make([][]byte, 4)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, set.SAdd(key, values, expectRecords))
	tests := []struct {
		key       string
		val       [][]byte
		ok        bool
		expectErr error
	}{
		{key, values[0:2], true, nil},
		{key, values[2:], true, nil},
		{key, values, true, nil},
		{key, [][]byte{testutils.GetRandomBytes(24)}, false, nil},
		{"fake key", values, true, ErrSetNotExist},
	}
	for _, tt := range tests {
		ok, err := set.SAreMembers(tt.key, tt.val...)
		if tt.expectErr != nil {
			require.Equal(t, tt.expectErr, err)
		} else {
			require.Equal(t, tt.ok, ok)
		}
	}
}

func TestSet_SUnion(t *testing.T) {
	set := NewSet()

	key1 := "set1"
	key2 := "set2"
	key3 := "set3"
	key4 := "set4"

	expectRecords := GenerateRecords(10)

	values := make([][]byte, 10)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, set.SAdd(key1, values[:5], expectRecords[:5]))
	require.NoError(t, set.SAdd(key2, values[2:6], expectRecords[2:6]))
	require.NoError(t, set.SAdd(key3, values[2:7], expectRecords[2:7]))
	require.NoError(t, set.SAdd(key4, values[7:], expectRecords[7:]))

	type args struct {
		key1 string
		key2 string
	}

	tests := []struct {
		name    string
		args    args
		set     *Set
		want    []*Record
		wantErr bool
	}{
		{"normal set Union1", args{key1, key4}, set,
			[]*Record{expectRecords[0], expectRecords[1], expectRecords[2], expectRecords[3], expectRecords[4], expectRecords[7], expectRecords[8], expectRecords[9]},
			false},
		{
			"normal set Union2", args{key2, key3}, set,
			[]*Record{expectRecords[2], expectRecords[3], expectRecords[4], expectRecords[5], expectRecords[6]},
			false,
		},
		{"first fake set", args{"fake_key1", key2}, set, nil, true},
		{"second fake set", args{key1, "fake_key2"}, set, nil, true},
		{"two fake set", args{"fake_key1", "fake_key2"}, set, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.set.SUnion(tt.args.key1, tt.args.key2)
			if (err != nil) != tt.wantErr {
				require.Errorf(t, err, "Get() error = %v, wantErr %v", tt.wantErr)
			}
			require.ElementsMatchf(t, tt.want, got, "Get() got = %v, want %v", got, tt.want)
		})
	}
}
