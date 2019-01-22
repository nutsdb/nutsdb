package nutsdb

type (
	EntryIdxMode  int
	MergeStrategy string
)

const (
	HintAndRAMIdxMode EntryIdxMode = iota
	HintAndMemoryMapIdxMode
)

// Options records params for creating DB object.
type Options struct {
	Dir          string
	EntryIdxMode EntryIdxMode
	SegmentSize  int64
	IsMerging    bool
	NodeNum      int64
}

var defaultSegmentSize int64 = 64 * 1024 * 1024

var DefaultOptions = Options{
	EntryIdxMode: HintAndRAMIdxMode,
	SegmentSize:  defaultSegmentSize,
	IsMerging:    false,
	NodeNum:      1,
}
