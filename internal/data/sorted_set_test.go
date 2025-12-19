// Copyright 2025 The nutsdb Author. All rights reserved.
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

package data_test

import (
	"testing"

	"github.com/nutsdb/nutsdb/internal/data"
	"github.com/stretchr/testify/suite"
)

type sortedSetTestSuite struct {
	suite.Suite
}

func (st *sortedSetTestSuite) TestCRUD_ZAdd_ZCount() {
	s := data.NewSortedSet()
	zsetKey := "zset"
	values := []string{"3", "9", "11", "1", "29"}
	scores := []float64{3.0, 1, 11, 1, 29}
	l := len(values)
	for i := 0; i < l; i++ {
		v := values[i]
		score := scores[i]
		err := s.ZAdd(
			zsetKey,
			data.SCORE(score),
			[]byte(v),
			data.NewRecord().
				WithKey([]byte(v)).WithValue([]byte(v)),
		)
		st.NoError(err)
	}
	exist, err := s.ZExist(zsetKey, []byte("3"))
	st.NoError(err)
	st.True(exist)
	cnt, err := s.ZCount(
		zsetKey,
		data.SCORE(3.0),
		data.SCORE(20.0),
		nil,
	)
	st.NoError(err)
	st.Equal(2, cnt)
}

func (st *sortedSetTestSuite) Test_ZMembers() {
	s := data.NewSortedSet()
	zsetKey := "zset"
	values := []string{"3", "9", "11", "1", "29"}
	scores := []float64{3.0, 1, 11, 1, 29}
	mp := make(map[string]data.SCORE)
	l := len(values)
	for i := 0; i < l; i++ {
		v := values[i]
		score := scores[i]
		mp[v] = data.SCORE(score)
		err := s.ZAdd(
			zsetKey,
			data.SCORE(score),
			[]byte(v),
			data.NewRecord().
				WithKey([]byte(v)).WithValue([]byte(v)),
		)
		st.NoError(err)
	}

	members, err := s.ZMembers(zsetKey)
	mpCopy := make(map[string]data.SCORE)
	for rec, score := range members {
		mpCopy[string(rec.Key)] = score
	}
	st.NoError(err)
	st.EqualValues(mp, mpCopy)
}

func (st *sortedSetTestSuite) Test_ZCard() {
	s := data.NewSortedSet()
	zsetKey := "zset"
	values := []string{"3", "9", "11", "1", "29"}
	scores := []float64{3.0, 1, 11, 1, 29}
	mp := make(map[string]data.SCORE)
	l := len(values)
	for i := 0; i < l; i++ {
		v := values[i]
		score := scores[i]
		mp[v] = data.SCORE(score)
		err := s.ZAdd(
			zsetKey,
			data.SCORE(score),
			[]byte(v),
			data.NewRecord().
				WithKey([]byte(v)).WithValue([]byte(v)),
		)
		st.NoError(err)
	}

	// not found
	_, err := s.ZCard("zset_not_found")
	st.Equal(data.ErrSortedSetNotFound, err)
	// found
	l, err = s.ZCard(zsetKey)
	st.NoError(err)
	st.Equal(5, l)
}

func (st *sortedSetTestSuite) Test_ZExist() {
	s := data.NewSortedSet()
	zsetKey := "zset"
	err := s.ZAdd(
		zsetKey, data.SCORE(1), []byte("x"),
		data.NewRecord().WithKey([]byte("y")),
	)
	st.NoError(err)

	exists, err := s.ZExist(zsetKey, []byte("x"))
	st.NoError(err)
	st.True(exists)

	exists, err = s.ZExist(zsetKey, []byte("x1"))
	st.NoError(err)
	st.False(exists)

	exists, err = s.ZExist("111", []byte("x"))
	st.Equal(data.ErrSortedSetNotFound, err)
	st.False(exists)
}

func TestSortedSetTestSuiteMain(t *testing.T) {
	suite.Run(t, new(sortedSetTestSuite))
}
