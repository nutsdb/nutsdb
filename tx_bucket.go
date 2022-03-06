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
func (tx *Tx) IterateBuckets(ds uint16, f func(bucket string)) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}
	if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		return ErrNotSupportHintBPTSparseIdxMode
	}
	if ds == DataStructureSet {
		for bucket := range tx.db.SetIdx {
			f(bucket)
		}
	}
	if ds == DataStructureSortedSet {
		for bucket := range tx.db.SortedSetIdx {
			f(bucket)
		}
	}
	if ds == DataStructureList {
		for bucket := range tx.db.ListIdx {
			f(bucket)
		}
	}
	if ds == DataStructureBPTree {
		for bucket := range tx.db.BPTreeIdx {
			f(bucket)
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
	if ds == DataStructureSet {
		return tx.put(bucket, []byte("0"), nil, Persistent, DataSetBucketDeleteFlag, uint64(time.Now().Unix()), DataStructureNone)
	}
	if ds == DataStructureSortedSet {
		return tx.put(bucket, []byte("1"), nil, Persistent, DataSortedSetBucketDeleteFlag, uint64(time.Now().Unix()), DataStructureNone)
	}
	if ds == DataStructureBPTree {
		return tx.put(bucket, []byte("2"), nil, Persistent, DataBPTreeBucketDeleteFlag, uint64(time.Now().Unix()), DataStructureNone)
	}
	if ds == DataStructureList {
		return tx.put(bucket, []byte("3"), nil, Persistent, DataListBucketDeleteFlag, uint64(time.Now().Unix()), DataStructureNone)
	}
	return nil
}
