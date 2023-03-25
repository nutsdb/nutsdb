package metrics

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

type FileMetric struct {
	ValidEntries   int32
	InvalidEntries int32
	ValidBytes     int64
	InvalidBytes   int64
}

var (
	once        sync.Once
	fileMetrics map[int32]*atomic.Value // hold all the metrics
	lock        sync.Mutex
)

func Init() {
	once.Do(func() {
		fileMetrics = make(map[int32]*atomic.Value)
	})
}

// this method is for test purpose
func reset() {
	fileMetrics = make(map[int32]*atomic.Value)
}

func DeleteFileMetrics(fd int32) {
	lock.Lock()
	delete(fileMetrics, fd)
	lock.Unlock()
}

// UpdateFileMetrics
// for a fd, you should start with a new FileMetric{0,0,0,0},
// then update it along the way you do your business with the DB,
// then write it back to fileMetrics using this method.
func UpdateFileMetrics(fd int32, update *FileMetric) error {
	if _, ok := fileMetrics[fd]; !ok {
		return errors.Errorf("FileMetric for fd: %d dese not exist, please Initiate it", fd)
	}
	for {
		mOld := fileMetrics[fd].Load().(FileMetric)
		mNew := FileMetric{
			mOld.ValidEntries + update.ValidEntries,
			mOld.InvalidEntries + update.InvalidEntries,
			mOld.ValidBytes + update.ValidBytes,
			mOld.InvalidBytes + update.InvalidBytes,
		}
		if fileMetrics[fd].CompareAndSwap(mOld, mNew) {
			return nil
		}
	}
}

func InitFileMetricsForFd(fd int32) {
	lock.Lock()
	if _, ok := fileMetrics[fd]; !ok {
		fileMetrics[fd] = &atomic.Value{}
		fileMetrics[fd].Store(FileMetric{0, 0, 0, 0})
	}
	lock.Unlock()
}

func GetFileMetrics(fd int32) (*FileMetric, bool) {
	if value, ok := fileMetrics[fd]; ok {
		fm := value.Load().(FileMetric)
		return &fm, ok
	} else {
		return nil, ok
	}
}

func CountFileMetrics() int {
	return len(fileMetrics)
}

func GetFDsExceedThreshold(threshold float64) []int32 {
	var fds []int32
	for fd, fm := range fileMetrics {
		m := fm.Load().(FileMetric)
		ratio1 := float64(m.InvalidEntries) / float64(m.InvalidEntries+m.ValidEntries)
		ratio2 := float64(m.InvalidBytes) / float64(m.InvalidBytes+m.ValidBytes)
		if ratio1 >= threshold || ratio2 >= threshold {
			fds = append(fds, fd)
		}
	}
	sort.Slice(fds, func(i, j int) bool { return fds[i] < fds[j] })
	return fds
}
