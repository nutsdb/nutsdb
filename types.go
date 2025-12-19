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
	"github.com/nutsdb/nutsdb/internal/core"
)

// Type Aliases to maintain compatibility after moving to internal/core

type DataStructure = core.DataStructure
type DataFlag = core.DataFlag
type DataStatus = core.DataStatus

const Persistent = core.Persistent

const (
	// DataStructureSet represents the data structure set flag
	DataStructureSet = core.DataStructureSet

	// DataStructureSortedSet represents the data structure sorted set flag
	DataStructureSortedSet = core.DataStructureSortedSet

	// DataStructureBTree represents the data structure b tree flag
	DataStructureBTree = core.DataStructureBTree

	// DataStructureList represents the data structure list flag
	DataStructureList = core.DataStructureList
)

const (
	// DataDeleteFlag represents the data delete flag
	DataDeleteFlag = core.DataDeleteFlag

	// DataSetFlag represents the data set flag
	DataSetFlag = core.DataSetFlag

	// DataLPushFlag represents the data LPush flag
	DataLPushFlag = core.DataLPushFlag

	// DataRPushFlag represents the data RPush flag
	DataRPushFlag = core.DataRPushFlag

	// DataLRemFlag represents the data LRem flag
	DataLRemFlag = core.DataLRemFlag

	// DataLPopFlag represents the data LPop flag
	DataLPopFlag = core.DataLPopFlag

	// DataRPopFlag represents the data RPop flag
	DataRPopFlag = core.DataRPopFlag

	// DataLTrimFlag represents the data LTrim flag
	DataLTrimFlag = core.DataLTrimFlag

	// DataZAddFlag represents the data ZAdd flag
	DataZAddFlag = core.DataZAddFlag

	// DataZRemFlag represents the data ZRem flag
	DataZRemFlag = core.DataZRemFlag

	// DataZRemRangeByRankFlag represents the data ZRemRangeByRank flag
	DataZRemRangeByRankFlag = core.DataZRemRangeByRankFlag

	// DataZPopMaxFlag represents the data ZPopMax flag
	DataZPopMaxFlag = core.DataZPopMaxFlag

	// DataZPopMinFlag represents the data aZPopMin flag
	DataZPopMinFlag = core.DataZPopMinFlag

	// DataSetBucketDeleteFlag represents the delete Set bucket flag
	DataSetBucketDeleteFlag = core.DataSetBucketDeleteFlag

	// DataSortedSetBucketDeleteFlag represents the delete Sorted Set bucket flag
	DataSortedSetBucketDeleteFlag = core.DataSortedSetBucketDeleteFlag

	// DataBTreeBucketDeleteFlag represents the delete BTree bucket flag
	DataBTreeBucketDeleteFlag = core.DataBTreeBucketDeleteFlag

	// DataListBucketDeleteFlag represents the delete List bucket flag
	DataListBucketDeleteFlag = core.DataListBucketDeleteFlag

	// DataLRemByIndex represents the data LRemByIndex flag
	DataLRemByIndex = core.DataLRemByIndex

	// DataExpireListFlag represents that set ttl for the list
	DataExpireListFlag = core.DataExpireListFlag

	// DataBucketDeleteFlag represents the delete bucket flag
	DataBucketDeleteFlag = core.DataBucketDeleteFlag
)

const (
	// UnCommitted represents the tx unCommitted status
	UnCommitted = core.UnCommitted

	// Committed represents the tx committed status
	Committed = core.Committed
)
