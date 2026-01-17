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

func TestRecord(t *testing.T) {
	r := require.New(t)
	rec := core.NewRecord()
	rec = rec.WithKey([]byte("111"))
	r.Equal([]byte("111"), rec.Key)

	rec = rec.WithValue([]byte("1112"))
	r.Equal([]byte("1112"), rec.Value)

	rec = rec.WithFileId(int64(1))
	r.Equal(int64(1), rec.FileID)

	rec = rec.WithDataPos(uint64(12))
	r.Equal(uint64(12), rec.DataPos)

	rec = rec.WithValueSize(uint32(123))
	r.Equal(uint32(123), rec.ValueSize)

	rec = rec.WithTimestamp(uint64(121314))
	r.Equal(uint64(121314), rec.Timestamp)

	rec = rec.WithTTL(uint32(9))
	r.Equal(uint32(9), rec.TTL)

	rec = rec.WithTxID(uint64(99))
	r.Equal(uint64(99), rec.TxID)
}
