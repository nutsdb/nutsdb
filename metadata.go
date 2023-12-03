package nutsdb

// DataStructure represents the data structure we have already supported
type DataStructure = uint16

// DataFlag means the data operations have done by users.
type DataFlag = uint16

// DataStatus means the status of data
type DataStatus = uint16

const (
	// DataStructureSet represents the data structure set flag
	DataStructureSet DataStructure = 0

	// DataStructureSortedSet represents the data structure sorted set flag
	DataStructureSortedSet DataStructure = 1

	// DataStructureBTree represents the data structure b tree flag
	DataStructureBTree DataStructure = 2

	// DataStructureList represents the data structure list flag
	DataStructureList DataStructure = 3
)

const (
	// DataDeleteFlag represents the data delete flag
	DataDeleteFlag DataFlag = 0

	// DataSetFlag represents the data set flag
	DataSetFlag DataFlag = 1

	// DataLPushFlag represents the data LPush flag
	DataLPushFlag DataFlag = 2

	// DataRPushFlag represents the data RPush flag
	DataRPushFlag DataFlag = 3

	// DataLRemFlag represents the data LRem flag
	DataLRemFlag DataFlag = 4

	// DataLPopFlag represents the data LPop flag
	DataLPopFlag DataFlag = 5

	// DataRPopFlag represents the data RPop flag
	DataRPopFlag DataFlag = 6

	// DataLTrimFlag represents the data LTrim flag
	DataLTrimFlag DataFlag = 8

	// DataZAddFlag represents the data ZAdd flag
	DataZAddFlag DataFlag = 9

	// DataZRemFlag represents the data ZRem flag
	DataZRemFlag DataFlag = 10

	// DataZRemRangeByRankFlag represents the data ZRemRangeByRank flag
	DataZRemRangeByRankFlag DataFlag = 11

	// DataZPopMaxFlag represents the data ZPopMax flag
	DataZPopMaxFlag DataFlag = 12

	// DataZPopMinFlag represents the data aZPopMin flag
	DataZPopMinFlag DataFlag = 13

	// DataSetBucketDeleteFlag represents the delete Set bucket flag
	DataSetBucketDeleteFlag DataFlag = 14

	// DataSortedSetBucketDeleteFlag represents the delete Sorted Set bucket flag
	DataSortedSetBucketDeleteFlag DataFlag = 15

	// DataBTreeBucketDeleteFlag represents the delete BTree bucket flag
	DataBTreeBucketDeleteFlag DataFlag = 16

	// DataListBucketDeleteFlag represents the delete List bucket flag
	DataListBucketDeleteFlag DataFlag = 17

	// DataLRemByIndex represents the data LRemByIndex flag
	DataLRemByIndex DataFlag = 18

	// DataExpireListFlag represents that set ttl for the list
	DataExpireListFlag DataFlag = 19
)

const (
	// UnCommitted represents the tx unCommitted status
	UnCommitted uint16 = 0

	// Committed represents the tx committed status
	Committed uint16 = 1
)

// Persistent represents the data persistent flag
const Persistent uint32 = 0

type MetaData struct {
	KeySize    uint32
	ValueSize  uint32
	Timestamp  uint64
	TTL        uint32
	Flag       DataFlag // delete / set
	BucketSize uint32
	TxID       uint64
	Status     DataStatus    // committed / uncommitted
	Ds         DataStructure // data structure
	Crc        uint32
	BucketId   BucketId
}

func (meta *MetaData) Size() int64 {
	// CRC
	size := 4

	size += UvarintSize(uint64(meta.KeySize))
	size += UvarintSize(uint64(meta.ValueSize))
	size += UvarintSize(meta.Timestamp)
	size += UvarintSize(uint64(meta.TTL))
	size += UvarintSize(uint64(meta.Flag))
	size += UvarintSize(meta.TxID)
	size += UvarintSize(uint64(meta.Status))
	size += UvarintSize(uint64(meta.Ds))
	size += UvarintSize(meta.BucketId)

	return int64(size)
}

func (meta *MetaData) PayloadSize() int64 {
	return int64(meta.BucketSize) + int64(meta.KeySize) + int64(meta.ValueSize)
}

func NewMetaData() *MetaData {
	return new(MetaData)
}

func (meta *MetaData) WithKeySize(keySize uint32) *MetaData {
	meta.KeySize = keySize
	return meta
}

func (meta *MetaData) WithValueSize(valueSize uint32) *MetaData {
	meta.ValueSize = valueSize
	return meta
}

func (meta *MetaData) WithTimeStamp(timestamp uint64) *MetaData {
	meta.Timestamp = timestamp
	return meta
}

func (meta *MetaData) WithTTL(ttl uint32) *MetaData {
	meta.TTL = ttl
	return meta
}

func (meta *MetaData) WithFlag(flag uint16) *MetaData {
	meta.Flag = flag
	return meta
}

func (meta *MetaData) WithBucketSize(bucketSize uint32) *MetaData {
	meta.BucketSize = bucketSize
	return meta
}

func (meta *MetaData) WithTxID(txID uint64) *MetaData {
	meta.TxID = txID
	return meta
}

func (meta *MetaData) WithStatus(status uint16) *MetaData {
	meta.Status = status
	return meta
}

func (meta *MetaData) WithDs(ds uint16) *MetaData {
	meta.Ds = ds
	return meta
}

func (meta *MetaData) WithCrc(crc uint32) *MetaData {
	meta.Crc = crc
	return meta
}

func (meta *MetaData) WithBucketId(bucketID uint64) *MetaData {
	meta.BucketId = bucketID
	return meta
}

func (meta *MetaData) IsBPlusTree() bool {
	return meta.Ds == DataStructureBTree
}

func (meta *MetaData) IsSet() bool {
	return meta.Ds == DataStructureSet
}

func (meta *MetaData) IsSortSet() bool {
	return meta.Ds == DataStructureSortedSet
}

func (meta *MetaData) IsList() bool {
	return meta.Ds == DataStructureList
}
