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
	"github.com/xujiajun/nutsdb/consts"
	"github.com/xujiajun/nutsdb/errs"
	"time"
)

// IterateBuckets iterate over all the bucket depends on ds (represents the data structure)
func (tx *Tx) IterateBuckets(ds consts.DataStruct, pattern string, f func(key string) bool) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}
	if tx.db.opt.EntryIdxMode.Is(consts.HintBPTSparseIdxMode) {
		return errs.ErrNotSupportHintBPTSparseIdxMode
	}
	if ds.Is(consts.DataStructureSet) {
		for bucket := range tx.db.SetIdx {
			if end, err := MatchForRange(pattern, bucket, f); end || err != nil {
				return err
			}
		}
	}
	if ds.Is(consts.DataStructureSortedSet) {
		for bucket := range tx.db.SortedSetIdx {
			if end, err := MatchForRange(pattern, bucket, f); end || err != nil {
				return err
			}
		}
	}
	if ds.Is(consts.DataStructureList) {
		for bucket := range tx.db.ListIdx {
			if end, err := MatchForRange(pattern, bucket, f); end || err != nil {
				return err
			}
		}
	}
	if ds.Is(consts.DataStructureBPTree) {
		for bucket := range tx.db.BPTreeIdx {
			if end, err := MatchForRange(pattern, bucket, f); end || err != nil {
				return err
			}
		}
	}
	return nil
}

// DeleteBucket delete bucket depends on ds (represents the data structure)
func (tx *Tx) DeleteBucket(ds consts.DataStruct, bucket string) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}
	if tx.db.opt.EntryIdxMode.Is(consts.HintBPTSparseIdxMode) {
		return errs.ErrNotSupportHintBPTSparseIdxMode
	}
	if ds.Is(consts.DataStructureSet) {
		return tx.put(bucket, []byte("0"), nil, consts.Persistent, consts.DataSetBucketDeleteFlag, uint64(time.Now().Unix()), consts.DataStructureNone)
	}
	if ds.Is(consts.DataStructureSortedSet) {
		return tx.put(bucket, []byte("1"), nil, consts.Persistent, consts.DataSortedSetBucketDeleteFlag, uint64(time.Now().Unix()), consts.DataStructureNone)
	}
	if ds.Is(consts.DataStructureBPTree) {
		return tx.put(bucket, []byte("2"), nil, consts.Persistent, consts.DataBPTreeBucketDeleteFlag, uint64(time.Now().Unix()), consts.DataStructureNone)
	}
	if ds == consts.DataStructureList {
		return tx.put(bucket, []byte("3"), nil, consts.Persistent, consts.DataListBucketDeleteFlag, uint64(time.Now().Unix()), consts.DataStructureNone)
	}
	return nil
}
