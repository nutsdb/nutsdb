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

import "time"

// IterateBuckets iterate over all the bucket depends on ds (represents the data structure)
func (tx *Tx) IterateBuckets(ds uint16, pattern string, f func(key string) bool) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}
	if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		return ErrNotSupportHintBPTSparseIdxMode
	}
	if ds == DataStructureSet {
		for bucket := range tx.db.SetIdx {
			if end, err := MatchForRange(pattern, bucket, f); end || err != nil {
				return err
			}
		}
	}
	if ds == DataStructureSortedSet {
		for bucket := range tx.db.SortedSetIdx {
			if end, err := MatchForRange(pattern, bucket, f); end || err != nil {
				return err
			}
		}
	}
	if ds == DataStructureList {
		f := func(bucket string) error {
			if end, err := MatchForRange(pattern, bucket, f); end || err != nil {
				return err
			}
			return nil
		}
		err := tx.db.Index.handleListBucket(f)
		if err != nil {
			return err
		}
	}
	if ds == DataStructureTree {
		for bucket := range tx.db.BTreeIdx {
			if end, err := MatchForRange(pattern, bucket, f); end || err != nil {
				return err
			}
		}
	}
	return nil
}

// DeleteBucket delete bucket depends on ds (represents the data structure)
func (tx *Tx) DeleteBucket(ds uint16, bucket string) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}
	if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		return ErrNotSupportHintBPTSparseIdxMode
	}

	ok, err := tx.ExistBucket(ds, bucket)
	if err != nil {
		return err
	}
	if !ok {
		return ErrBucketNotFound
	}

	if ds == DataStructureSet {
		return tx.put(bucket, []byte("0"), nil, Persistent, DataSetBucketDeleteFlag, uint64(time.Now().Unix()), DataStructureNone)
	}
	if ds == DataStructureSortedSet {
		return tx.put(bucket, []byte("1"), nil, Persistent, DataSortedSetBucketDeleteFlag, uint64(time.Now().Unix()), DataStructureNone)
	}
	if ds == DataStructureTree {
		return tx.put(bucket, []byte("2"), nil, Persistent, DataBPTreeBucketDeleteFlag, uint64(time.Now().Unix()), DataStructureNone)
	}
	if ds == DataStructureList {
		return tx.put(bucket, []byte("3"), nil, Persistent, DataListBucketDeleteFlag, uint64(time.Now().Unix()), DataStructureNone)
	}
	return nil
}

func (tx *Tx) ExistBucket(ds uint16, bucket string) (bool, error) {
	var ok bool

	switch ds {
	case DataStructureSet:
		_, ok = tx.db.SetIdx[bucket]
	case DataStructureSortedSet:
		_, ok = tx.db.SortedSetIdx[bucket]
	case DataStructureTree:
		_, ok = tx.db.BTreeIdx[bucket]
	case DataStructureList:
		ok = tx.db.Index.existList(bucket)
	default:
		return false, ErrDataStructureNotSupported
	}

	return ok, nil
}
