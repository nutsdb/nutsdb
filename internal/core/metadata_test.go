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
	"github.com/stretchr/testify/assert"
)

func TestNewMetadata(t *testing.T) {
	metadata := core.NewMetaData().
		WithKeySize(uint32(5)).
		WithValueSize(uint32(3)).
		WithTimeStamp(uint64(1888888)).
		WithTTL(uint32(5)).
		WithFlag(core.DataSetFlag).
		WithBucketSize(uint32(10)).
		WithTxID(uint64(10)).
		WithStatus(core.UnCommitted).
		WithDs(core.DataStructureBTree).
		WithCrc(uint32(10)).
		WithBucketId(uint64(101))

	assert.True(t, metadata.IsBTree())
	assert.False(t, metadata.IsList())
	assert.False(t, metadata.IsSet())
	assert.False(t, metadata.IsSortSet())
	assert.Equal(t, int64(15), metadata.Size())
	assert.Equal(t, int64(18), metadata.PayloadSize())
}
