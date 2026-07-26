// Copyright 2026 The nutsdb Author. All rights reserved.
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

package fileio

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFormatAndParseFileID(t *testing.T) {
	require.Equal(t, "0000000001", formatFileID(1))
	require.Equal(t, "0000000042", formatFileID(42))
	require.Equal(t, "4294967295", formatFileID(^uint32(0)))

	id, ok := parseSegmentID("0000000001.seg")
	require.True(t, ok)
	require.Equal(t, uint32(1), id)

	_, ok = parseSegmentID("1.seg")
	require.False(t, ok, "unpadded file id must be rejected")

	_, ok = parseSegmentID("00000000001.seg")
	require.False(t, ok, "width other than 10 must be rejected")

	_, ok = parseSegmentID("000000000a.seg")
	require.False(t, ok)
}
