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

package nutsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMarshalInts(t *testing.T) {
	assertions := assert.New(t)
	data, err := MarshalInts([]int{})
	assertions.NoError(err, "TestMarshalInts")

	ints, err := UnmarshalInts(data)
	assertions.NoError(err, "TestMarshalInts")
	assertions.Equal(0, len(ints), "TestMarshalInts")

	data, err = MarshalInts([]int{1, 3})
	assertions.NoError(err, "TestMarshalInts")

	ints, err = UnmarshalInts(data)
	assertions.NoError(err, "TestMarshalInts")
	assertions.Equal(2, len(ints), "TestMarshalInts")
	assertions.Equal(1, ints[0], "TestMarshalInts")
	assertions.Equal(3, ints[1], "TestMarshalInts")
}

func TestMatchForRange(t *testing.T) {
	assertions := assert.New(t)

	end, err := MatchForRange("*", "hello", func(key string) bool {
		return true
	})
	assertions.NoError(err, "TestMatchForRange")
	assertions.False(end, "TestMatchForRange")

	_, err = MatchForRange("[", "hello", func(key string) bool {
		return true
	})
	assertions.Error(err, "TestMatchForRange")

	end, err = MatchForRange("*", "hello", func(key string) bool {
		return false
	})
	assertions.NoError(err, "TestMatchForRange")
	assertions.True(end, "TestMatchForRange")
}
