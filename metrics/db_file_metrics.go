package metrics

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

type FileMetrics struct {
	ValidEntries   int32
	InvalidEntries int32
	ValidBytes     int64
	InvalidBytes   int64
}

var (
	once  sync.Once
	fmMap map[int32]*FileMetrics
	lock  sync.Mutex
)

func Init() {
	once.Do(func() {
		fmMap = make(map[int32]*FileMetrics)
	})
}

// this method is for test case reset the state.
func reset() {
	fmMap = make(map[int32]*FileMetrics)
}

func DeleteFileMetrics(fd int32) {
	delete(fmMap, fd)
}

// UpdateFileMetrics
// you can start with a new &FileMetrics{0,0,0,0},
// then update it along the way you update the DB entries,
// then update it back to fmMap with UpdateFileMetrics.
func UpdateFileMetrics(fd int32, update *FileMetrics) error {
	m, ok := fmMap[fd]
	if !ok {
		return errors.Errorf("FileMetrics for fd: %d dese not exist, please Initiate it", fd)
	}
	atomicUpdateInt32(&m.ValidEntries, update.ValidEntries)
	atomicUpdateInt32(&m.InvalidEntries, update.InvalidEntries)
	atomicUpdateInt64(&m.ValidBytes, update.ValidBytes)
	atomicUpdateInt64(&m.InvalidBytes, update.InvalidBytes)
	return nil
}

func atomicUpdateInt32(addr *int32, delta int32) {
	for old := atomic.LoadInt32(addr); !atomic.CompareAndSwapInt32(addr, old, old+delta); old = atomic.LoadInt32(addr) {
	}
}

func atomicUpdateInt64(addr *int64, delta int64) {
	for old := atomic.LoadInt64(addr); !atomic.CompareAndSwapInt64(addr, old, old+delta); old = atomic.LoadInt64(addr) {
	}
}

func InitFileMetricsForFd(fd int32) {
	lock.Lock()
	if _, ok := fmMap[fd]; !ok {
		fmMap[fd] = &FileMetrics{0, 0, 0, 0}
	}
	lock.Unlock()
}

func GetFileMetrics(fd int32) (*FileMetrics, bool) {
	m, ok := fmMap[fd]
	if ok {
		m = &FileMetrics{m.ValidEntries, m.InvalidEntries, m.ValidBytes, m.InvalidBytes}
	}
	return m, ok
}

func CountFileMetrics() int {
	return len(fmMap)
}

func GetFDsExceedThreshold(threshold float64) []int32 {
	var fds []int32
	for fd, m := range fmMap {
		ratio1 := float64(m.InvalidEntries) / float64(m.InvalidEntries+m.ValidEntries)
		ratio2 := float64(m.InvalidBytes) / float64(m.InvalidBytes+m.ValidBytes)
		if ratio1 >= threshold || ratio2 >= threshold {
			fds = append(fds, fd)
		}
	}
	sort.Slice(fds, func(i, j int) bool { return fds[i] < fds[j] })
	return fds
}
