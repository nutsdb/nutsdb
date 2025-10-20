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

	"github.com/stretchr/testify/require"
)

func TestCompareAndReturn(t *testing.T) {
	r := require.New(t)

	t.Run("target equal to others", func(t *testing.T) {
		target := []byte("test1")
		other := []byte("test1")

		r.Equal(target, compareAndReturn(target, other, 1))
		r.Equal(target, compareAndReturn(target, other, -1))
	})

	t.Run("target greater than others", func(t *testing.T) {
		target := []byte("b-string")
		other := []byte("c")
		r.Equal(other, compareAndReturn(target, other, 1))
		r.Equal(target, compareAndReturn(target, other, -1))
	})

	t.Run("target smaller than others", func(t *testing.T) {
		target := []byte("a-string")
		other := []byte("c")
		r.Equal(other, compareAndReturn(target, other, 1))
		r.Equal(target, compareAndReturn(target, other, -1))
	})
}
