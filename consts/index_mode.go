package consts

// EntryIdxMode represents entry index mode.
type EntryIdxMode int

const (
	// HintKeyValAndRAMIdxMode represents ram index (key and value) mode.
	HintKeyValAndRAMIdxMode EntryIdxMode = iota

	// HintKeyAndRAMIdxMode represents ram index (only key) mode.
	HintKeyAndRAMIdxMode

	// HintBPTSparseIdxMode represents b+ tree sparse index mode.
	HintBPTSparseIdxMode
)

func (ds EntryIdxMode) Is(data EntryIdxMode) bool {
	return ds == data
}

func (ds EntryIdxMode) NoneOf(datas []EntryIdxMode) bool {
	for _, d := range datas {
		if ds == d {
			return false
		}
	}
	return true
}

func (ds EntryIdxMode) OneOf(datas []EntryIdxMode) bool {
	for _, s := range datas {
		if ds == s {
			return true
		}
	}
	return false
}

func (ds EntryIdxMode) Not(s EntryIdxMode) bool {
	return ds != s
}
