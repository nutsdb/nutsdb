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
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"testing"
)

func InitForBucket() {
	fileDir := "/tmp/nutsdbtestbuckettx"
	files, _ := ioutil.ReadDir(fileDir)
	for _, f := range files {
		name := f.Name()
		if name != "" {
			err := os.RemoveAll(fileDir + "/" + name)
			if err != nil {
				panic(err)
			}
		}
	}

	opt = DefaultOptions
	opt.Dir = fileDir
	opt.SegmentSize = 8 * 1024
	return
}

func TestTx_DeleteBucket(t *testing.T) {
	InitForBucket()
	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	var expectedBuckets []string

	deleteBucketNum := 3
	expectedDeleteBucketNum := "bucket" + fmt.Sprintf("%07d", deleteBucketNum)
	// write tx begin
	for i := 1; i <= 10; i++ {
		tx, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}

		numStr := fmt.Sprintf("%07d", i)
		bucket := "bucket" + numStr
		key := []byte("key" + numStr)
		val := []byte("val" + numStr)
		if err = tx.Put(bucket, key, val, Persistent); err != nil {
			// tx rollback
			err = tx.Rollback()
			t.Fatal(err)
		} else {
			// tx commit
			tx.Commit()
		}

		if bucket != expectedDeleteBucketNum {
			expectedBuckets = append(expectedBuckets, bucket)
		}
	}

	// write tx begin
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.DeleteBucket(DataStructureBPTree, expectedDeleteBucketNum)
	if err != nil {
		// tx rollback
		tx.Rollback()
		t.Fatal(err)

	} else {
		// tx commit
		tx.Commit()
	}

	// read tx begin
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	var buckets []string
	err = tx.IterateBuckets(DataStructureBPTree, func(bucket string) {
		buckets = append(buckets, bucket)
	})
	if err != nil {
		// tx rollback
		err = tx.Rollback()
		t.Fatal(err)
	} else {
		// tx commit
		tx.Commit()
	}

	compareBuckets(t, expectedBuckets, buckets)
}

func TestTx_IterateBuckets(t *testing.T) {
	InitForBucket()
	db, err = Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	// write tx begin
	var expectedBuckets []string
	for i := 1; i <= 10; i++ {
		tx, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}

		numStr := fmt.Sprintf("%07d", i)
		bucket := "bucket" + numStr
		key := []byte("key" + numStr)
		val := []byte("val" + numStr)
		if err = tx.Put(bucket, key, val, Persistent); err != nil {
			// tx rollback
			err = tx.Rollback()
			t.Fatal(err)
		} else {
			// tx commit
			tx.Commit()
		}

		expectedBuckets = append(expectedBuckets, "bucket"+numStr)
	}

	// read tx begin
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	var buckets []string
	err = tx.IterateBuckets(DataStructureBPTree, func(bucket string) {
		buckets = append(buckets, bucket)
	})
	if err != nil {
		// tx rollback
		err = tx.Rollback()
		t.Fatal(err)
	} else {
		// tx commit
		tx.Commit()
	}

	compareBuckets(t, expectedBuckets, buckets)
}

func compareBuckets(t *testing.T, expectedBuckets, actualBuckets []string) {
	sort.Strings(actualBuckets)
	for i, expectedBucket := range expectedBuckets {
		if actualBuckets[i] != expectedBucket {
			t.Error("err IterateBuckets")
		}
	}
}
