package fileio

import "os"

const (
	B = 1

	KB = 1024 * B

	MB = 1024 * KB

	GB = 1024 * MB
)

var (
	openFile = os.OpenFile
)
