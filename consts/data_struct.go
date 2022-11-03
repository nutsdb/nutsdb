package consts

type DataStruct uint16

const (
	// DataStructureSet represents the data structure set flag
	DataStructureSet DataStruct = iota

	// DataStructureSortedSet represents the data structure sorted set flag
	DataStructureSortedSet

	// DataStructureBPTree represents the data structure b+ tree flag
	DataStructureBPTree

	// DataStructureList represents the data structure list flag
	DataStructureList

	// DataStructureNone represents not the data structure
	DataStructureNone
)

func (ds DataStruct) Is(data DataStruct) bool {
	return ds == data
}

func (ds DataStruct) NoneOf(datas []DataStruct) bool {
	for _, d := range datas {
		if ds == d {
			return false
		}
	}
	return true
}

func (ds DataStruct) OneOf(datas []DataStruct) bool {
	for _, s := range datas {
		if ds == s {
			return true
		}
	}
	return false
}

func (ds DataStruct) Not(s DataStruct) bool {
	return ds != s
}
