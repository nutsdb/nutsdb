package consts

const (

	// defaultSegmentSize is default data file size.
	DefaultSegmentSize int64 = 256 * MB

	// BucketMetaHeaderSize returns the header size of the BucketMeta.
	BucketMetaHeaderSize = 12

	// DataEntryHeaderSize returns the entry header size
	DataEntryHeaderSize = 42

	// BPTreeRootIdxHeaderSize returns the header size of the root index.
	BPTreeRootIdxHeaderSize = 28
)

const (
	// DataSuffix returns the data suffix
	DataSuffix = ".dat"

	// BucketMetaSuffix returns b+ tree index suffix.
	BucketMetaSuffix = ".meta"
)

const (
	// Persistent represents the data persistent flag
	Persistent = 0

	// ScanNoLimit represents the data scan no limit flag
	ScanNoLimit = -1
)

const (
	B = 1

	KB = 1024 * B

	MB = 1024 * KB

	GB = 1024 * MB
)

const (
	// Order Default number of b+ tree orders.
	Order = 8

	// DefaultInvalidAddress returns default invalid node address.
	DefaultInvalidAddress = -1

	// RangeScan returns range scanMode flag.
	RangeScan = "RangeScan"

	// PrefixScan returns prefix scanMode flag.
	PrefixScan = "PrefixScan"

	// PrefixSearchScan returns prefix and search scanMode flag.
	PrefixSearchScan = "PrefixSearchScan"

	// CountFlagEnabled returns enabled CountFlag.
	CountFlagEnabled = true

	// CountFlagDisabled returns disabled CountFlag.
	CountFlagDisabled = false

	// BPTIndexSuffix returns b+ tree index suffix.
	BPTIndexSuffix = ".bptidx"

	// BPTRootIndexSuffix returns b+ tree root index suffix.
	BPTRootIndexSuffix = ".bptridx"

	// BPTTxIDIndexSuffix returns b+ tree tx ID index suffix.
	BPTTxIDIndexSuffix = ".bpttxid"

	// BPTRootTxIDIndexSuffix returns b+ tree root tx ID index suffix.
	BPTRootTxIDIndexSuffix = ".bptrtxid"
)

// SeparatorForListKey represents separator for listKey
const SeparatorForListKey = "|"

// SeparatorForZSetKey represents separator for zSet key.
const SeparatorForZSetKey = "|"
