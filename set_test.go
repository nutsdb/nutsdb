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

package nutsdb

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSet_SAdd(t *testing.T) {
	set := newSet()
	key := "key"
	expectRecords := generateRecords(3)

	values := make([][]byte, 3)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, set.sAdd(key, values, expectRecords))

	ok, err := set.sAreMembers(key, values...)
	require.True(t, ok)
	require.NoError(t, err)
}

func TestSet_SRem(t *testing.T) {
	set := newSet()
	key := "key"
	expectRecords := generateRecords(4)
	values := make([][]byte, 4)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, set.sAdd(key, values, expectRecords))

	require.NoError(t, set.sRem(key, values[0]))
	require.NoError(t, set.sRem(key, values[1:]...))

	require.NoError(t, set.sAdd(key, values, expectRecords))
	require.NoError(t, set.sRem(key, values...))

	require.Error(t, set.sRem("fake key", values...))
	require.Error(t, set.sRem(key, nil))
}

func TestSet_SDiff(t *testing.T) {
	s := newSet()

	key1 := "set1"
	key2 := "set2"
	key3 := "set3"
	key4 := "set4"

	expectRecords := generateRecords(10)

	values := make([][]byte, 10)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, s.sAdd(key1, values[:5], expectRecords[:5]))
	require.NoError(t, s.sAdd(key2, values[2:6], expectRecords[2:6]))
	require.NoError(t, s.sAdd(key3, values[2:7], expectRecords[2:7]))
	require.NoError(t, s.sAdd(key4, values[7:], expectRecords[7:]))

	type args struct {
		key1 string
		key2 string
	}

	tests := []struct {
		name    string
		args    args
		set     *set
		want    []*record
		wantErr bool
	}{
		{"normal set diff1", args{key1, key2}, s, expectRecords[:2], false},
		{"normal set diff2", args{key1, key4}, s, expectRecords[:5], false},
		{"normal set diff3", args{key3, key2}, s, expectRecords[6:7], false},
		{"first fake set", args{"fake_key1", key2}, s, nil, true},
		{"second fake set", args{key1, "fake_key2"}, s, nil, true},
		{"two fake set", args{"fake_key1", "fake_key2"}, s, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.set.sDiff(tt.args.key1, tt.args.key2)
			if (err != nil) != tt.wantErr {
				require.Errorf(t, err, "Get() error = %v, wantErr %v", tt.wantErr)
			}
			require.ElementsMatchf(t, tt.want, got, "Get() got = %v, want %v", got, tt.want)
		})
	}
}

func TestSet_SCard(t *testing.T) {
	s := newSet()

	key1 := "set1"
	key2 := "set2"
	key3 := "set3"
	key4 := "set4"

	expectRecords := generateRecords(10)

	values := make([][]byte, 10)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, s.sAdd(key1, values[:5], expectRecords[:5]))
	require.NoError(t, s.sAdd(key2, values[2:6], expectRecords[2:6]))
	require.NoError(t, s.sAdd(key3, values[2:7], expectRecords[2:7]))
	require.NoError(t, s.sAdd(key4, values[7:], expectRecords[7:]))

	tests := []struct {
		name string
		key  string
		set  *set
		want int
	}{
		{"normal set", key1, s, 5},
		{"normal set", key2, s, 4},
		{"normal set", key3, s, 5},
		{"fake key", "key_fake", s, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.set.sCard(tt.key)
			require.Equalf(t, tt.want, got, "TestSet_SCard err")
		})
	}
}

func TestSet_SInter(t *testing.T) {
	s := newSet()

	key1 := "set1"
	key2 := "set2"
	key3 := "set3"
	key4 := "set4"

	expectRecords := generateRecords(10)

	values := make([][]byte, 10)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, s.sAdd(key1, values[:5], expectRecords[:5]))
	require.NoError(t, s.sAdd(key2, values[2:6], expectRecords[2:6]))
	require.NoError(t, s.sAdd(key3, values[2:7], expectRecords[2:7]))
	require.NoError(t, s.sAdd(key4, values[7:], expectRecords[7:]))

	type args struct {
		key1 string
		key2 string
	}

	tests := []struct {
		name    string
		args    args
		set     *set
		want    []*record
		wantErr bool
	}{
		{"normal set inter1", args{key1, key2}, s, []*record{expectRecords[2], expectRecords[3], expectRecords[4]}, false},
		{"normal set inter1", args{key2, key3}, s, []*record{expectRecords[2], expectRecords[3], expectRecords[4], expectRecords[5]}, false},
		{"normal set inter2", args{key1, key4}, s, nil, false},
		{"first fake set", args{"fake_key1", key2}, s, nil, true},
		{"second fake set", args{key1, "fake_key2"}, s, nil, true},
		{"two fake set", args{"fake_key1", "fake_key2"}, s, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.set.sInter(tt.args.key1, tt.args.key2)
			if (err != nil) != tt.wantErr {
				require.Errorf(t, err, "Get() error = %v, wantErr %v", tt.wantErr)
			}
			require.ElementsMatchf(t, tt.want, got, "Get() got = %v, want %v", got, tt.want)
		})
	}
}

func TestSet_SMembers(t *testing.T) {
	s := newSet()

	key := "set"

	expectRecords := generateRecords(3)

	values := make([][]byte, 3)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, s.sAdd(key, values, expectRecords))

	tests := []struct {
		name    string
		key     string
		set     *set
		want    []*record
		wantErr bool
	}{
		{"normal SMembers", key, s, expectRecords[0:1], false},
		{"normal SMembers", key, s, expectRecords[1:], false},
		{"normal SMembers", key, s, expectRecords, false},
		{"fake key", "fake_key", s, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.set.sMembers(tt.key)
			if (err != nil) != tt.wantErr {
				require.Errorf(t, err, "Get() error = %v, wantErr %v", tt.wantErr)
			}
			require.Subsetf(t, got, tt.want, "sInter() got = %v, want = %v", got, tt.want)
		})
	}
}

func TestSet_SMove(t *testing.T) {
	s := newSet()

	key1 := "set1"
	key2 := "set2"

	expectRecords := generateRecords(3)
	values := make([][]byte, 3)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, s.sAdd(key1, values[:2], expectRecords[:2]))
	require.NoError(t, s.sAdd(key2, values[2:], expectRecords[2:]))

	type args struct {
		key1 string
		key2 string
		item []byte
	}

	tests := []struct {
		name      string
		args      args
		set       *set
		want1     []*record
		want2     []*record
		expectErr error
	}{
		{"normal sMove", args{key1, key2, values[1]}, s, expectRecords[0:1], expectRecords[1:], nil},
		{"not exist member sMove", args{key1, key2, values[2]}, s, nil, nil, ErrSetMemberNotExist},
		{"fake key sMove1", args{"fake key", key2, values[2]}, s, nil, nil, ErrSetNotExist},
		{"fake key sMove", args{key1, "fake key", values[2]}, s, nil, nil, ErrSetNotExist},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.set.sMove(tt.args.key1, tt.args.key2, tt.args.item)
			if tt.expectErr != nil {
				require.Error(t, err)
				require.Equal(t, tt.expectErr, err)
			} else {
				got1, _ := tt.set.sMembers(tt.args.key1)
				got2, _ := tt.set.sMembers(tt.args.key2)
				require.ElementsMatchf(t, got1, tt.want1, "SMove() got = %v, want = %v", got1, tt.want1)
				require.ElementsMatchf(t, got2, tt.want2, "SMove() got = %v, want = %v", got2, tt.want2)
			}
		})
	}
}

func TestSet_SPop(t *testing.T) {
	s := newSet()

	key := "set"

	expectRecords := generateRecords(2)
	values := make([][]byte, 2)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}
	m := map[*record]struct{}{}
	for _, expectRecord := range expectRecords {
		m[expectRecord] = struct{}{}
	}

	require.NoError(t, s.sAdd(key, values, expectRecords))

	tests := []struct {
		name string
		key  string
		set  *set
		ok   bool
	}{
		{"normal set sPop", key, s, true},
		{"normal set sPop", key, s, true},
		{"normal set sPop", key, s, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := tt.set.sPop(tt.key)
			_, ok := m[record]
			require.Equal(t, tt.ok, ok)
		})
	}
}

func TestSet_SIsMember(t *testing.T) {
	s := newSet()

	key := "key"

	expectRecords := generateRecords(1)
	values := [][]byte{expectRecords[0].Value}

	require.NoError(t, s.sAdd(key, values, expectRecords))
	tests := []struct {
		key       string
		val       []byte
		ok        bool
		expectErr error
	}{
		{key, values[0], true, nil},
		{key, getRandomBytes(24), false, nil},
		{"fake key", getRandomBytes(24), false, ErrSetNotExist},
	}
	for _, tt := range tests {
		ok, err := s.sIsMember(tt.key, tt.val)
		if tt.expectErr != nil {
			require.Equal(t, tt.expectErr, err)
		} else {
			require.Equal(t, tt.ok, ok)
		}
	}
}

func TestSet_SAreMembers(t *testing.T) {
	s := newSet()

	key := "set"

	expectRecords := generateRecords(4)
	values := make([][]byte, 4)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, s.sAdd(key, values, expectRecords))
	tests := []struct {
		key       string
		val       [][]byte
		ok        bool
		expectErr error
	}{
		{key, values[0:2], true, nil},
		{key, values[2:], true, nil},
		{key, values, true, nil},
		{key, [][]byte{getRandomBytes(24)}, false, nil},
		{"fake key", values, true, ErrSetNotExist},
	}
	for _, tt := range tests {
		ok, err := s.sAreMembers(tt.key, tt.val...)
		if tt.expectErr != nil {
			require.Equal(t, tt.expectErr, err)
		} else {
			require.Equal(t, tt.ok, ok)
		}
	}
}

func TestSet_SUnion(t *testing.T) {
	s := newSet()

	key1 := "set1"
	key2 := "set2"
	key3 := "set3"
	key4 := "set4"

	expectRecords := generateRecords(10)

	values := make([][]byte, 10)
	for i := range expectRecords {
		values[i] = expectRecords[i].Value
	}

	require.NoError(t, s.sAdd(key1, values[:5], expectRecords[:5]))
	require.NoError(t, s.sAdd(key2, values[2:6], expectRecords[2:6]))
	require.NoError(t, s.sAdd(key3, values[2:7], expectRecords[2:7]))
	require.NoError(t, s.sAdd(key4, values[7:], expectRecords[7:]))

	type args struct {
		key1 string
		key2 string
	}

	tests := []struct {
		name    string
		args    args
		set     *set
		want    []*record
		wantErr bool
	}{
		{"normal set Union1", args{key1, key4}, s,
			[]*record{expectRecords[0], expectRecords[1], expectRecords[2], expectRecords[3], expectRecords[4], expectRecords[7], expectRecords[8], expectRecords[9]},
			false},
		{
			"normal set Union2", args{key2, key3}, s,
			[]*record{expectRecords[2], expectRecords[3], expectRecords[4], expectRecords[5], expectRecords[6]},
			false,
		},
		{"first fake set", args{"fake_key1", key2}, s, nil, true},
		{"second fake set", args{key1, "fake_key2"}, s, nil, true},
		{"two fake set", args{"fake_key1", "fake_key2"}, s, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.set.sUnion(tt.args.key1, tt.args.key2)
			if (err != nil) != tt.wantErr {
				require.Errorf(t, err, "Get() error = %v, wantErr %v", tt.wantErr)
			}
			require.ElementsMatchf(t, tt.want, got, "Get() got = %v, want %v", got, tt.want)
		})
	}
}
