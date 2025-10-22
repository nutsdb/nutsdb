package fileio

import "os"

// Truncate changes the size of the file.
// If readOnly is true, it skips the file stat check and truncate operation,
// which significantly improves read performance by avoiding unnecessary syscalls.
func Truncate(path string, capacity int64, f *os.File, readOnly bool) error {
	// Skip truncation for read-only operations to avoid expensive os.Stat syscall
	if readOnly {
		return nil
	}

	fileInfo, _ := os.Stat(path)
	if fileInfo.Size() < capacity {
		if err := f.Truncate(capacity); err != nil {
			return err
		}
	}
	return nil
}
