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

package core_test

import (
	"testing"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/stretchr/testify/require"
)

func TestNewItem(t *testing.T) {
	t.Run("normal creation", func(t *testing.T) {
		r := require.New(t)
		testdata := []byte("xxx")
		item := core.NewItem(testdata, core.NewRecord().WithTTL(1000))
		r.Equal(testdata, item.Key)
		r.Equal(uint32(1000), item.Record.TTL)
	})

	t.Run("creation with nil record", func(t *testing.T) {
		r := require.New(t)
		testdata := []byte("xxx")
		item := core.NewItem[core.Record](testdata, nil)
		r.Equal(testdata, item.Key)
		r.Nil(item.Record)
	})
}
