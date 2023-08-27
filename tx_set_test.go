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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTx_SAdd(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txSAdd(t, db, bucket, []byte(""), []byte("val1"), ErrKeyEmpty)

		key := GetTestBytes(0)
		num := 10
		for i := 0; i < num; i++ {
			txSAdd(t, db, bucket, key, GetTestBytes(i), nil)
		}

		for i := 0; i < num; i++ {
			txSIsMember(t, db, bucket, key, GetTestBytes(i), true)
		}

		txSIsMember(t, db, bucket, key, GetTestBytes(num), false)
	})
}

func TestTx_SRem(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		key := []byte("key1")
		val1 := []byte("one")
		val2 := []byte("two")
		val3 := []byte("three")

		txSAdd(t, db, bucket, key, val1, nil)
		txSAdd(t, db, bucket, key, val2, nil)
		txSAdd(t, db, bucket, key, val3, nil)

		txSRem(t, db, bucket, key, val3, nil)

		txSIsMember(t, db, bucket, key, val1, true)
		txSIsMember(t, db, bucket, key, val2, true)
		txSIsMember(t, db, bucket, key, val3, false)
	})
}

func TestTx_SRem2(t *testing.T) {

	bucket := "bucket"
	key := GetTestBytes(0)
	val1 := GetTestBytes(0)
	val2 := GetTestBytes(1)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txSAdd(t, db, bucket, key, val1, nil)
		txSAdd(t, db, bucket, key, val2, nil)

		txSRem(t, db, bucket, key, val1, nil)
		txSRem(t, db, bucket, key, val1, ErrSetMemberNotExist)

		txSRem(t, db, bucket, key, val2, nil)

		txSAreMembers(t, db, bucket, key, false, val1, val2)
	})
}

func TestTx_SMembers(t *testing.T) {
	bucket := "bucket"
	fakeBucket := "fake_bucket"
	key := GetTestBytes(0)
	val1 := GetTestBytes(0)
	val2 := GetTestBytes(1)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txSAdd(t, db, bucket, key, val1, nil)
		txSAdd(t, db, bucket, key, val2, nil)

		txSMembers(t, db, bucket, key, 2, nil)

		txSIsMember(t, db, bucket, key, val1, true)
		txSIsMember(t, db, bucket, key, val1, true)

		txSMembers(t, db, fakeBucket, key, 0, ErrBucketNotFound)
	})
}

func TestTx_SCard(t *testing.T) {
	bucket := "bucket"
	fakeBucket := "fake_bucket"
	key := GetTestBytes(0)
	val1 := GetTestBytes(1)
	val2 := GetTestBytes(2)
	val3 := GetTestBytes(3)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txSAdd(t, db, bucket, key, val1, nil)
		txSAdd(t, db, bucket, key, val2, nil)
		txSAdd(t, db, bucket, key, val3, nil)

		txSCard(t, db, bucket, key, 3, nil)

		txSCard(t, db, fakeBucket, key, 0, ErrBucketNotFound)
	})
}

func TestTx_SDiffByOneBucket(t *testing.T) {
	bucket := "bucket"
	fakeBucket := "fake_bucket"
	key1 := GetTestBytes(0)
	key2 := GetTestBytes(1)
	key3 := GetTestBytes(2)
	val1 := GetTestBytes(1)
	val2 := GetTestBytes(2)
	val3 := GetTestBytes(3)
	val4 := GetTestBytes(4)
	val5 := GetTestBytes(5)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txSAdd(t, db, bucket, key1, val1, nil)
		txSAdd(t, db, bucket, key1, val2, nil)
		txSAdd(t, db, bucket, key1, val3, nil)

		txSAdd(t, db, bucket, key2, val3, nil)
		txSAdd(t, db, bucket, key2, val4, nil)
		txSAdd(t, db, bucket, key2, val5, nil)

		diff := [][]byte{val1, val2}
		txSDiffByOneBucket(t, db, bucket, key1, key2, diff, nil)
		txSDiffByOneBucket(t, db, fakeBucket, key2, key1, nil, ErrBucketNotFound)

		txSAdd(t, db, bucket, key3, val1, nil)
		txSAdd(t, db, bucket, key3, val2, nil)

		for _, val := range diff {
			txSIsMember(t, db, bucket, key3, val, true)
		}
	})
}

func TestTx_SDiffByTwoBuckets(t *testing.T) {
	bucket1 := "bucket1"
	bucket2 := "bucket2"
	bucket3 := "bucket3"
	fakeBucket := "fake_bucket_%d"
	key1 := GetTestBytes(0)
	key2 := GetTestBytes(1)
	key3 := GetTestBytes(2)
	val1 := GetTestBytes(1)
	val2 := GetTestBytes(2)
	val3 := GetTestBytes(3)
	val4 := GetTestBytes(4)
	val5 := GetTestBytes(5)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txSAdd(t, db, bucket1, key1, val1, nil)
		txSAdd(t, db, bucket1, key1, val2, nil)
		txSAdd(t, db, bucket1, key1, val3, nil)

		txSAdd(t, db, bucket2, key2, val3, nil)
		txSAdd(t, db, bucket2, key2, val4, nil)
		txSAdd(t, db, bucket2, key2, val5, nil)

		diff := [][]byte{val1, val2}
		txSDiffByTwoBucket(t, db, bucket1, key1, bucket2, key2, diff, nil)

		txSDiffByTwoBucket(t, db, fmt.Sprintf(fakeBucket, 1), key1, bucket2, key2, nil, ErrBucketNotFound)
		txSDiffByTwoBucket(t, db, bucket1, key1, fmt.Sprintf(fakeBucket, 2), key2, nil, ErrBucketNotFound)

		txSAdd(t, db, bucket3, key3, val1, nil)
		txSAdd(t, db, bucket3, key3, val2, nil)

		for _, val := range diff {
			txSIsMember(t, db, bucket3, key3, val, true)
		}
	})
}

func TestTx_SPop(t *testing.T) {
	bucket := "bucket"
	fakeBucket := "fake_bucket"
	key := GetTestBytes(0)
	val1 := GetTestBytes(1)
	val2 := GetTestBytes(2)
	val3 := GetTestBytes(3)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txSAdd(t, db, bucket, key, val1, nil)
		txSAdd(t, db, bucket, key, val2, nil)
		txSAdd(t, db, bucket, key, val3, nil)

		txSCard(t, db, bucket, key, 3, nil)
		txSPop(t, db, bucket, key, nil)
		txSCard(t, db, bucket, key, 2, nil)

		txSPop(t, db, fakeBucket, key, ErrBucketNotFound)
	})

}

func TestTx_SMoveByOneBucket(t *testing.T) {
	bucket := "bucket"
	fakeBucket := "fake_bucket"
	key1 := GetTestBytes(0)
	key2 := GetTestBytes(1)
	val1 := GetTestBytes(1)
	val2 := GetTestBytes(2)
	val3 := GetTestBytes(3)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txSAdd(t, db, bucket, key1, val1, nil)
		txSAdd(t, db, bucket, key1, val2, nil)

		txSAdd(t, db, bucket, key2, val3, nil)

		txSMoveByOneBucket(t, db, bucket, key1, key2, val2, true, nil)
		txSIsMember(t, db, bucket, key1, val2, false)
		txSIsMember(t, db, bucket, key2, val2, true)

		txSMoveByOneBucket(t, db, fakeBucket, key1, key2, val2, false, ErrBucket)
	})
}

func TestTx_SMoveByTwoBuckets(t *testing.T) {
	bucket1 := "bucket1"
	bucket2 := "bucket2"
	fakeBucket := "fake_bucket_%d"
	key1 := GetTestBytes(0)
	key2 := GetTestBytes(1)
	fakeKey1 := GetTestBytes(2)
	fakeKey2 := GetTestBytes(3)
	val1 := GetTestBytes(1)
	val2 := GetTestBytes(2)
	val3 := GetTestBytes(3)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txSAdd(t, db, bucket1, key1, val1, nil)
		txSAdd(t, db, bucket1, key1, val2, nil)

		txSAdd(t, db, bucket2, key2, val3, nil)

		txSMoveByTwoBuckets(t, db, bucket1, key1, bucket2, key2, val2, true, nil)
		txSIsMember(t, db, bucket1, key1, val2, false)
		txSIsMember(t, db, bucket2, key2, val2, true)

		txSMoveByTwoBuckets(t, db, bucket1, fakeKey1, bucket2, key2, val2, false, ErrNotFoundKey)
		txSMoveByTwoBuckets(t, db, bucket1, key1, bucket2, fakeKey2, val2, false, ErrNotFoundKey)
		txSMoveByTwoBuckets(t, db, fmt.Sprintf(fakeBucket, 1), key1, bucket2, key2, val2, false, ErrBucketNotFound)
		txSMoveByTwoBuckets(t, db, bucket1, key1, fmt.Sprintf(fakeBucket, 2), key2, val2, false, ErrBucketNotFound)
		txSMoveByTwoBuckets(t, db, fmt.Sprintf(fakeBucket, 1), key1, fmt.Sprintf(fakeBucket, 2), key2, val2, false, ErrBucketNotFound)
	})
}

func TestTx_SUnionByOneBucket(t *testing.T) {
	bucket := "bucket"
	fakeBucket := "fake_bucket"
	key1 := GetTestBytes(0)
	key2 := GetTestBytes(1)
	key3 := GetTestBytes(2)

	val1 := GetTestBytes(1)
	val2 := GetTestBytes(2)
	val3 := GetTestBytes(3)
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txSAdd(t, db, bucket, key1, val1, nil)
		txSAdd(t, db, bucket, key1, val2, nil)
		txSAdd(t, db, bucket, key2, val3, nil)
		txSAdd(t, db, bucket, key3, val1, nil)
		txSAdd(t, db, bucket, key3, val2, nil)
		txSAdd(t, db, bucket, key3, val3, nil)

		all := [][]byte{val1, val2, val3}
		txSUnionByOneBucket(t, db, bucket, key1, key2, all, nil)
		for _, item := range all {
			txSIsMember(t, db, bucket, key3, item, true)
		}

		txSUnionByOneBucket(t, db, fakeBucket, key1, key2, nil, ErrBucket)
	})
}

func TestTx_SUnionByTwoBuckets(t *testing.T) {
	bucket1 := "bucket1"
	bucket2 := "bucket2"
	fakeBucket := "fake_bucket_%d"
	key1 := GetTestBytes(0)
	key2 := GetTestBytes(1)
	fakeKey1 := GetTestBytes(2)
	fakeKey2 := GetTestBytes(3)
	val1 := GetTestBytes(1)
	val2 := GetTestBytes(2)
	val3 := GetTestBytes(3)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txSAdd(t, db, bucket1, key1, val1, nil)
		txSAdd(t, db, bucket1, key1, val2, nil)
		txSAdd(t, db, bucket2, key2, val3, nil)

		all := [][]byte{val1, val2, val3}
		txSUnionByTwoBuckets(t, db, bucket1, key1, bucket2, key2, all, nil)

		txSUnionByTwoBuckets(t, db, fmt.Sprintf(fakeBucket, 1), key1, bucket2, key2, nil, ErrBucketNotFound)
		txSUnionByTwoBuckets(t, db, bucket1, key1, fmt.Sprintf(fakeBucket, 2), key2, nil, ErrBucketNotFound)
		txSUnionByTwoBuckets(t, db, bucket1, fakeKey1, bucket2, key2, nil, ErrNotFoundKey)
		txSUnionByTwoBuckets(t, db, bucket1, key1, bucket2, fakeKey2, nil, ErrNotFoundKey)
	})
}

func TestTx_SHasKey(t *testing.T) {
	bucket := "bucket"
	fakeBucket := "fake_bucket"
	key := GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txSAdd(t, db, bucket, key, GetTestBytes(1), nil)

		txSHasKey(t, db, bucket, key, true)
		txSHasKey(t, db, fakeBucket, key, false)
	})
}

func TestTx_SIsMember(t *testing.T) {
	bucket := "bucket"
	fakeBucket := "fake_bucket"
	key := GetTestBytes(0)
	fakeKey := GetTestBytes(1)
	val := GetTestBytes(0)
	fakeVal := GetTestBytes(1)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txSAdd(t, db, bucket, key, val, nil)

		txSIsMember(t, db, bucket, key, val, true)
		txSIsMember(t, db, bucket, key, fakeVal, false)
		txSIsMember(t, db, bucket, fakeKey, val, false)
		txSIsMember(t, db, fakeBucket, fakeKey, val, false)
	})
}

func TestTx_SAreMembers(t *testing.T) {
	bucket := "bucket"
	fakeBucket := "fake_bucket"
	key := GetTestBytes(0)
	fakeKey := GetTestBytes(1)
	val1 := GetTestBytes(0)
	val2 := GetTestBytes(1)
	fakeVal := GetTestBytes(2)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txSAdd(t, db, bucket, key, val1, nil)
		txSAdd(t, db, bucket, key, val2, nil)

		txSAreMembers(t, db, bucket, key, true)
		txSAreMembers(t, db, bucket, key, true, val1)
		txSAreMembers(t, db, bucket, key, true, val2)
		txSAreMembers(t, db, bucket, key, true, val1, val2)
		txSAreMembers(t, db, bucket, key, false, fakeVal)
		txSAreMembers(t, db, bucket, fakeKey, false, val1)
		txSAreMembers(t, db, fakeBucket, key, false, val1)
	})
}

func TestTx_SKeys(t *testing.T) {
	bucket := "bucket"
	key := "key_%d"
	val := GetTestBytes(0)

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		num := 3
		for i := 0; i < num; i++ {
			txSAdd(t, db, bucket, []byte(fmt.Sprintf(key, i)), val, nil)
		}

		var keys []string
		txSKeys(t, db, bucket, "*", func(key string) bool {
			keys = append(keys, key)
			return true
		}, num, nil)

		keys = []string{}
		txSKeys(t, db, bucket, "*", func(key string) bool {
			keys = append(keys, key)
			return len(keys) != num-1
		}, num-1, nil)

		keys = []string{}
		txSKeys(t, db, bucket, "fake_key*", func(key string) bool {
			keys = append(keys, key)
			return true
		}, 0, nil)
	})
}

func TestErrBucketAndKey(t *testing.T) {

	got := ErrBucketAndKey("foo", []byte("bar"))

	assert.True(t,
		errors.Is(got, ErrBucketNotFound))
}

func TestErrNotFoundKeyInBucket(t *testing.T) {

	got := ErrNotFoundKeyInBucket("foo", []byte("bar"))

	assert.True(t,
		errors.Is(got, ErrNotFoundKey))
}
