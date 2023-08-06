// Copyright 2022 The nutsdb Author. All rights reserved.
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

func TestIterator(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 100; i++ {
			txPut(t, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil)
		}

		_ = db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: false})

			i := 0

			for {
				value, err := iterator.Value()
				require.NoError(t, err)
				require.Equal(t, GetTestBytes(i), value)
				if !iterator.Next() {
					break
				}
				i++
			}

			return nil
		})
	})
}

func TestIterator_Reverse(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 100; i++ {
			txPut(t, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil)
		}

		_ = db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: true})

			i := 99
			for {
				value, err := iterator.Value()
				require.NoError(t, err)
				require.Equal(t, GetTestBytes(i), value)
				if !iterator.Next() {
					break
				}
				i--
			}

			return nil
		})
	})
}

func TestIterator_Seek(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 100; i++ {
			txPut(t, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil)
		}

		_ = db.View(func(tx *Tx) error {
			iterator := NewIterator(tx, bucket, IteratorOptions{Reverse: true})

			iterator.Seek(GetTestBytes(40))

			value, err := iterator.Value()
			require.NoError(t, err)
			require.Equal(t, GetTestBytes(40), value)

			return nil
		})
	})
}
