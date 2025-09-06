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
func (tx *Tx) IterateBuckets(ds uint16, pattern string, f func(bucket string) bool) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	if ds == DataStructureSet {
		for bucketId := range tx.db.Index.set.idx {
			bucket, err := tx.db.bm.GetBucketById(uint64(bucketId))
			if err != nil {
				return err
			}
			if end, err := MatchForRange(pattern, bucket.Name, f); end || err != nil {
				return err
			}
		}
	}
	if ds == DataStructureSortedSet {
		for bucketId := range tx.db.Index.sortedSet.idx {
			bucket, err := tx.db.bm.GetBucketById(uint64(bucketId))
			if err != nil {
				return err
			}
			if end, err := MatchForRange(pattern, bucket.Name, f); end || err != nil {
				return err
			}
		}
	}
	if ds == DataStructureList {
		for bucketId := range tx.db.Index.list.idx {
			bucket, err := tx.db.bm.GetBucketById(uint64(bucketId))
			if err != nil {
				return err
			}
			if end, err := MatchForRange(pattern, bucket.Name, f); end || err != nil {
				return err
			}
		}
	}
	if ds == DataStructureBTree {
		for bucketId := range tx.db.Index.bTree.idx {
			bucket, err := tx.db.bm.GetBucketById(uint64(bucketId))
			if err != nil {
				return err
			}
			if end, err := MatchForRange(pattern, bucket.Name, f); end || err != nil {
				return err
			}
		}
	}
	return nil
}

func (tx *Tx) NewKVBucket(name string) error {
	return tx.NewBucket(DataStructureBTree, name)
}

func (tx *Tx) NewListBucket(name string) error {
	return tx.NewBucket(DataStructureList, name)
}

func (tx *Tx) NewSetBucket(name string) error {
	return tx.NewBucket(DataStructureSet, name)
}

func (tx *Tx) NewSortSetBucket(name string) error {
	return tx.NewBucket(DataStructureSortedSet, name)
}

func (tx *Tx) NewBucket(ds uint16, name string) (err error) {
	if tx.ExistBucket(ds, name) {
		return ErrBucketAlreadyExist
	}
	bucket := &Bucket{
		Meta: &BucketMeta{
			Op: BucketInsertOperation,
		},
		Id:   tx.db.bm.Gen.GenId(),
		Ds:   ds,
		Name: name,
	}
	if _, exist := tx.pendingBucketList[ds]; !exist {
		tx.pendingBucketList[ds] = map[BucketName]*Bucket{}
	}
	tx.pendingBucketList[ds][name] = bucket
	return nil
}

// DeleteBucket delete bucket depends on ds (represents the data structure)
func (tx *Tx) DeleteBucket(ds uint16, bucket string) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	b, err := tx.db.bm.GetBucket(ds, bucket)
	if err != nil {
		return ErrBucketNotFound
	}

	deleteBucket := &Bucket{
		Meta: &BucketMeta{
			Op: BucketDeleteOperation,
		},
		Id:   b.Id,
		Ds:   ds,
		Name: bucket,
	}

	return tx.putBucket(deleteBucket)
}

func (tx *Tx) ExistBucket(ds uint16, bucket string) bool {
	return tx.db.bm.ExistBucket(ds, bucket)
}
