package nutsdb

// dataStructure represents the data structure we have already supported
type dataStructure = uint16

// dataFlag means the data operations have done by users.
type dataFlag = uint16

// dataStatus means the status of data
type dataStatus = uint16

const (
	// DataStructureSet represents the data structure set flag
	DataStructureSet dataStructure = 0

	// DataStructureSortedSet represents the data structure sorted set flag
	DataStructureSortedSet dataStructure = 1

	// DataStructureBTree represents the data structure b tree flag
	DataStructureBTree dataStructure = 2

	// DataStructureList represents the data structure list flag
	DataStructureList dataStructure = 3
)

const (
	// DataDeleteFlag represents the data delete flag
	DataDeleteFlag dataFlag = 0

	// DataSetFlag represents the data set flag
	DataSetFlag dataFlag = 1

	// DataLPushFlag represents the data LPush flag
	DataLPushFlag dataFlag = 2

	// DataRPushFlag represents the data RPush flag
	DataRPushFlag dataFlag = 3

	// DataLRemFlag represents the data LRem flag
	DataLRemFlag dataFlag = 4

	// DataLPopFlag represents the data LPop flag
	DataLPopFlag dataFlag = 5

	// DataRPopFlag represents the data RPop flag
	DataRPopFlag dataFlag = 6

	// DataLTrimFlag represents the data LTrim flag
	DataLTrimFlag dataFlag = 8

	// DataZAddFlag represents the data ZAdd flag
	DataZAddFlag dataFlag = 9

	// DataZRemFlag represents the data ZRem flag
	DataZRemFlag dataFlag = 10

	// DataZRemRangeByRankFlag represents the data ZRemRangeByRank flag
	DataZRemRangeByRankFlag dataFlag = 11

	// DataZPopMaxFlag represents the data ZPopMax flag
	DataZPopMaxFlag dataFlag = 12

	// DataZPopMinFlag represents the data aZPopMin flag
	DataZPopMinFlag dataFlag = 13

	// DataSetBucketDeleteFlag represents the delete Set bucket flag
	DataSetBucketDeleteFlag dataFlag = 14

	// DataSortedSetBucketDeleteFlag represents the delete Sorted Set bucket flag
	DataSortedSetBucketDeleteFlag dataFlag = 15

	// DataBTreeBucketDeleteFlag represents the delete bTree bucket flag
	DataBTreeBucketDeleteFlag dataFlag = 16

	// DataListBucketDeleteFlag represents the delete list bucket flag
	DataListBucketDeleteFlag dataFlag = 17

	// DataLRemByIndex represents the data LRemByIndex flag
	DataLRemByIndex dataFlag = 18

	// DataExpireListFlag represents that set ttl for the list
	DataExpireListFlag dataFlag = 19
)

const (
	// UnCommitted represents the tx unCommitted status
	UnCommitted uint16 = 0

	// Committed represents the tx committed status
	Committed uint16 = 1
)

// Persistent represents the data persistent flag
const Persistent uint32 = 0

type metaData struct {
	KeySize    uint32
	ValueSize  uint32
	Timestamp  uint64
	TTL        uint32
	Flag       dataFlag // delete / set
	BucketSize uint32
	TxID       uint64
	Status     dataStatus    // committed / uncommitted
	Ds         dataStructure // data structure
	Crc        uint32
	BucketId   bucketId
}

func (meta *metaData) size() int64 {
	// CRC
	size := 4

	size += uVarIntSize(uint64(meta.KeySize))
	size += uVarIntSize(uint64(meta.ValueSize))
	size += uVarIntSize(meta.Timestamp)
	size += uVarIntSize(uint64(meta.TTL))
	size += uVarIntSize(uint64(meta.Flag))
	size += uVarIntSize(meta.TxID)
	size += uVarIntSize(uint64(meta.Status))
	size += uVarIntSize(uint64(meta.Ds))
	size += uVarIntSize(meta.BucketId)

	return int64(size)
}

func (meta *metaData) payloadSize() int64 {
	return int64(meta.BucketSize) + int64(meta.KeySize) + int64(meta.ValueSize)
}

func newMetaData() *metaData {
	return new(metaData)
}

func (meta *metaData) withKeySize(keySize uint32) *metaData {
	meta.KeySize = keySize
	return meta
}

func (meta *metaData) withValueSize(valueSize uint32) *metaData {
	meta.ValueSize = valueSize
	return meta
}

func (meta *metaData) withTimeStamp(timestamp uint64) *metaData {
	meta.Timestamp = timestamp
	return meta
}

func (meta *metaData) withTTL(ttl uint32) *metaData {
	meta.TTL = ttl
	return meta
}

func (meta *metaData) withFlag(flag uint16) *metaData {
	meta.Flag = flag
	return meta
}

func (meta *metaData) withBucketSize(bucketSize uint32) *metaData {
	meta.BucketSize = bucketSize
	return meta
}

func (meta *metaData) withTxID(txID uint64) *metaData {
	meta.TxID = txID
	return meta
}

func (meta *metaData) withStatus(status uint16) *metaData {
	meta.Status = status
	return meta
}

func (meta *metaData) withDs(ds uint16) *metaData {
	meta.Ds = ds
	return meta
}

func (meta *metaData) withCrc(crc uint32) *metaData {
	meta.Crc = crc
	return meta
}

func (meta *metaData) withBucketId(bucketID uint64) *metaData {
	meta.BucketId = bucketID
	return meta
}

func (meta *metaData) isBPlusTree() bool {
	return meta.Ds == DataStructureBTree
}

func (meta *metaData) isSet() bool {
	return meta.Ds == DataStructureSet
}

func (meta *metaData) isSortSet() bool {
	return meta.Ds == DataStructureSortedSet
}

func (meta *metaData) isList() bool {
	return meta.Ds == DataStructureList
}
