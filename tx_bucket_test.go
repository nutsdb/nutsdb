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
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	setBucketName    = "set_bucket"
	zSetBucketName   = "zset_bucket"
	listBucketName   = "list_bucket"
	stringBucketName = "string_bucket"
)

func setupBucket(t *testing.T, db *DB) {
	key := getTestBytes(0)
	val := getTestBytes(1)
	txCreateBucket(t, db, DataStructureBTree, stringBucketName, nil)
	txCreateBucket(t, db, DataStructureSet, setBucketName, nil)
	txCreateBucket(t, db, DataStructureSortedSet, zSetBucketName, nil)
	txCreateBucket(t, db, DataStructureList, listBucketName, nil)

	txSAdd(t, db, setBucketName, key, val, nil, nil)
	txZAdd(t, db, zSetBucketName, key, val, 80, nil, nil)
	txPush(t, db, listBucketName, key, val, true, nil, nil)
	txPut(t, db, stringBucketName, key, val, Persistent, nil, nil)
}

func TestBucket_IterateBuckets(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		setupBucket(t, db)

		txIterateBuckets(t, db, DataStructureSet, "*", func(bucket string) bool {
			return true
		}, nil, setBucketName)

		txIterateBuckets(t, db, DataStructureSortedSet, "*", func(bucket string) bool {
			return true
		}, nil, zSetBucketName)

		txIterateBuckets(t, db, DataStructureList, "*", func(bucket string) bool {
			return true
		}, nil, listBucketName)

		txIterateBuckets(t, db, DataStructureBTree, "*", func(bucket string) bool {
			return true
		}, nil, stringBucketName)

		matched := false
		txIterateBuckets(t, db, DataStructureBTree, "str*", func(bucket string) bool {
			matched = true
			return true
		}, nil)
		assert.Equal(t, true, matched)

		matched = false
		txIterateBuckets(t, db, DataStructureList, "str*", func(bucket string) bool {
			return true
		}, nil)
		assert.Equal(t, false, matched)
	})
}

func TestBucket_DeleteBucket(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		setupBucket(t, db)

		txDeleteBucket(t, db, DataStructureSet, setBucketName, nil)
		txDeleteBucket(t, db, DataStructureSortedSet, zSetBucketName, nil)
		txDeleteBucket(t, db, DataStructureList, listBucketName, nil)
		txDeleteBucket(t, db, DataStructureBTree, stringBucketName, nil)

	})
}
