// Copyright 2023 The PromiseDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testutils

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func GetTestBytes(i int) []byte {
	// Optimized version without fmt.Sprintf to reduce benchmark overhead
	// Format: "nutsdb-000000000" (7 prefix + 9 digits = 16 bytes)
	buf := make([]byte, 16)
	copy(buf, "nutsdb-")

	// Convert i to 9-digit string with leading zeros
	for j := 15; j >= 7; j-- {
		buf[j] = byte('0' + i%10)
		i /= 10
	}
	return buf
}

func GetRandomBytes(length int) []byte {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return b
}

func AssertErr(t *testing.T, err error, expectErr error) {
	if expectErr != nil {
		require.Equal(t, expectErr, err)
	} else {
		require.NoError(t, err)
	}
}
