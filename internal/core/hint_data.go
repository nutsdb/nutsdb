package core

type HintData struct {
	// which data file to read the data from
	FileID int64
	// the position of the data in the data file
	DataPos uint64
}
