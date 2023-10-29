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

// IterateBuckets iterate over all the bucket depends on ds (represents the data structure)
func (tx *Tx) IterateBuckets(ds uint16, pattern string, f func(key string) bool) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	handle := func(bucket string) error {
		if end, err := MatchForRange(pattern, bucket, f); end || err != nil {
			return err
		}
		return nil
	}
	var err error
	if ds == DataStructureSet {
		err = tx.db.Index.set.handleIdxBucket(handle)
	}
	if ds == DataStructureSortedSet {
		err = tx.db.Index.sortedSet.handleIdxBucket(handle)
	}
	if ds == DataStructureList {
		err = tx.db.Index.list.handleIdxBucket(handle)
	}
	if ds == DataStructureBTree {
		err = tx.db.Index.bTree.handleIdxBucket(handle)
	}
	return err
}

func (tx *Tx) NewBucket(ds uint16, name string) (success bool, err error) {
	if tx.ExistBucket(ds, name) {
		return false, ErrBucketAlreadyExist
	}
	bucket := &Bucket{
		Meta: &BucketMeta{
			Op: BucketInsertOperation,
		},
		Id:   tx.db.bm.Gen.GenId(),
		Ds:   Ds(ds),
		Name: name,
	}
	if _, exist := tx.pendingBucketList[Ds(ds)]; !exist {
		tx.pendingBucketList[Ds(ds)] = map[BucketName]*Bucket{}
	}
	tx.pendingBucketList[Ds(ds)][BucketName(name)] = bucket
	return true, nil
}

// DeleteBucket delete bucket depends on ds (represents the data structure)
func (tx *Tx) DeleteBucket(ds uint16, bucket string) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	ok := tx.ExistBucket(ds, bucket)
	if !ok {
		return ErrBucketNotFound
	}

	b := &Bucket{
		Meta: &BucketMeta{
			Op: BucketDeleteOperation,
		},
		Id:   tx.db.bm.Gen.GenId(),
		Ds:   Ds(ds),
		Name: bucket,
	}

	return tx.putBucket(b)
}

func (tx *Tx) ExistBucket(ds uint16, bucket string) bool {
	return tx.db.bm.ExistBucket(Ds(ds), BucketName(bucket))
}
