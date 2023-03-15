package metrics

import (
	"sort"
	"sync"
	"sync/atomic"
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
//
// this function can be used when an FD does not exist,
// the effect is a new fd and FileMetrics set into the map
func UpdateFileMetrics(fd int32, update *FileMetrics) {
	updateFileMetrics(fd, update)
}

func BatchUpdateFileMetrics(updates map[int32]*FileMetrics) {
	for fd, update := range updates {
		updateFileMetrics(fd, update)
	}
}

func updateFileMetrics(fd int32, update *FileMetrics) {
	m, ok := fmMap[fd]
	if !ok {
		lock.Lock()
		if _, okk := fmMap[fd]; !okk {
			fmMap[fd] = &FileMetrics{0, 0, 0, 0}
		}
		lock.Unlock()
		m = fmMap[fd]
	}
	for !atomic.CompareAndSwapInt32(&(m.ValidEntries), m.ValidEntries, m.ValidEntries+update.ValidEntries) {
	}
	for !atomic.CompareAndSwapInt32(&(m.InvalidEntries), m.InvalidEntries, m.InvalidEntries+update.InvalidEntries) {
	}
	for !atomic.CompareAndSwapInt64(&(m.ValidBytes), m.ValidBytes, m.ValidBytes+update.ValidBytes) {
	}
	for !atomic.CompareAndSwapInt64(&(m.InvalidBytes), m.InvalidBytes, m.InvalidBytes+update.InvalidBytes) {
	}
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
