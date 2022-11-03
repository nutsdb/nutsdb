package consts

type DataFlag uint16

const (
	// DataDeleteFlag represents the data delete flag
	DataDeleteFlag DataFlag = iota

	// DataSetFlag represents the data set flag
	DataSetFlag

	// DataLPushFlag represents the data LPush flag
	DataLPushFlag

	// DataRPushFlag represents the data RPush flag
	DataRPushFlag

	// DataLRemFlag represents the data LRem flag
	DataLRemFlag

	// DataLPopFlag represents the data LPop flag
	DataLPopFlag

	// DataRPopFlag represents the data RPop flag
	DataRPopFlag

	// DataLSetFlag represents the data LSet flag
	DataLSetFlag

	// DataLTrimFlag represents the data LTrim flag
	DataLTrimFlag

	// DataZAddFlag represents the data ZAdd flag
	DataZAddFlag

	// DataZRemFlag represents the data ZRem flag
	DataZRemFlag

	// DataZRemRangeByRankFlag represents the data ZRemRangeByRank flag
	DataZRemRangeByRankFlag

	// DataZPopMaxFlag represents the data ZPopMax flag
	DataZPopMaxFlag

	// DataZPopMinFlag represents the data aZPopMin flag
	DataZPopMinFlag

	// DataSetBucketDeleteFlag represents the delete Set bucket flag
	DataSetBucketDeleteFlag

	// DataSortedSetBucketDeleteFlag represents the delete Sorted Set bucket flag
	DataSortedSetBucketDeleteFlag

	// DataBPTreeBucketDeleteFlag represents the delete BPTree bucket flag
	DataBPTreeBucketDeleteFlag

	// DataListBucketDeleteFlag represents the delete List bucket flag
	DataListBucketDeleteFlag

	// DataLRemByIndex represents the data LRemByIndex flag
	DataLRemByIndex
)

func (df DataFlag) Is(flag DataFlag) bool {
	return df == flag
}

func (df DataFlag) Not(flag DataFlag) bool {
	return df != flag
}

func (df DataFlag) OneOf(flags []DataFlag) bool {
	for _, flag := range flags {
		if df == flag {
			return true
		}
	}
	return false
}

func (df DataFlag) NoneOf(flags []DataFlag) bool {
	for _, flag := range flags {
		if df == flag {
			return false
		}
	}
	return true
}
