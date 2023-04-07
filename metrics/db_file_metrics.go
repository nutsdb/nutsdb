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

var (
	ErrFileMetricNotExists = errors.New("fileMetric not exits")
)

func Init() {
	once.Do(func() {
		fileMetrics = make(map[int32]*atomic.Value)
	})
}

func DeleteFileMetric(fd int32) {
	delete(fileMetrics, fd)
}

// UpdateFileMetric
// for an existing fd, you should start with a new FileMetric{0,0,0,0},
// then use it to accumulate the metric updates when you do your business,
// after committing, update it into the fileMetrics using this method.
func UpdateFileMetric(fd int32, delta *FileMetric) error {
	if _, ok := fileMetrics[fd]; !ok {
		return ErrFileMetricNotExists
	}
	for {
		mOld := fileMetrics[fd].Load().(FileMetric)
		mNew := FileMetric{
			mOld.ValidEntries + delta.ValidEntries,
			mOld.InvalidEntries + delta.InvalidEntries,
			mOld.ValidBytes + delta.ValidBytes,
			mOld.InvalidBytes + delta.InvalidBytes,
		}
		if fileMetrics[fd].CompareAndSwap(mOld, mNew) {
			return nil
		}
	}
}

func InitFileMetricForFile(fd int32) {
	lock.Lock()
	defer lock.Unlock()
	if _, ok := fileMetrics[fd]; !ok {
		fileMetrics[fd] = &atomic.Value{}
		fileMetrics[fd].Store(FileMetric{0, 0, 0, 0})
	}
}

func GetFileMetric(fd int32) (*FileMetric, bool) {
	if value, ok := fileMetrics[fd]; ok {
		fm := value.Load().(FileMetric)
		return &fm, ok
	} else {
		return nil, ok
	}
}

func GetFileMetricSize() int {
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
	sort.Slice(fds, func(i, j int) bool {
		return fds[i] < fds[j]
	})
	return fds
}
