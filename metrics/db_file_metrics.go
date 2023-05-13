package metrics

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

// FileMetric data structure for one file, will be maintained in a map
type FileMetric struct {
	ValidEntries   int32
	InvalidEntries int32
	ValidBytes     int64
	InvalidBytes   int64
}

var (
	once        sync.Once
	fileMetrics map[int32]*atomic.Value // a map holding all the metrics for all files
	lock        sync.Mutex
)

var ErrFileMetricNotExists = errors.New("fileMetric not exits")

// Init called when start or restart, only the first call will take effect.
func Init() {
	once.Do(func() {
		fileMetrics = make(map[int32]*atomic.Value)
	})
}

// DeleteFileMetric delete a metric for a file, should be called when a file is deleted
func DeleteFileMetric(fid int32) {
	delete(fileMetrics, fid)
}

// UpdateFileMetric
// for an existing fid, you should start with a new FileMetric{0,0,0,0},
// then use it to accumulate the metric updates when you do your business,
// after committing, update it into the fileMetrics using this method.
func UpdateFileMetric(fid int32, delta *FileMetric) error {
	if _, ok := fileMetrics[fid]; !ok {
		return ErrFileMetricNotExists
	}
	for {
		mOld := fileMetrics[fid].Load().(FileMetric)
		mNew := FileMetric{
			mOld.ValidEntries + delta.ValidEntries,
			mOld.InvalidEntries + delta.InvalidEntries,
			mOld.ValidBytes + delta.ValidBytes,
			mOld.InvalidBytes + delta.InvalidBytes,
		}
		if uint32(mNew.ValidEntries) > 1<<31-1 || uint32(mNew.InvalidEntries) > 1<<31-1 {
			return errors.New("ValidEntries or InvalidEntries overflowed")
		}
		if fileMetrics[fid].CompareAndSwap(mOld, mNew) {
			return nil
		}
	}
}

// InitFileMetricForFile initiate a metric for a newly create file
func InitFileMetricForFile(fid int32) {
	lock.Lock()
	defer lock.Unlock()
	if _, ok := fileMetrics[fid]; !ok {
		fileMetrics[fid] = &atomic.Value{}
		fileMetrics[fid].Store(FileMetric{0, 0, 0, 0})
	}
}

func GetFileMetric(fid int32) (*FileMetric, bool) {
	if value, ok := fileMetrics[fid]; ok {
		fm := value.Load().(FileMetric)
		return &fm, ok
	} else {
		return nil, ok
	}
}

func GetFileMetricSize() int {
	return len(fileMetrics)
}

// GetFIDsExceedThreshold get all the fids for those whose invalid_ratio exceeded the provided threshold
func GetFIDsExceedThreshold(threshold float64) []int32 {
	var fids []int32
	for fid, fm := range fileMetrics {
		m := fm.Load().(FileMetric)
		ratio1 := float64(m.InvalidEntries) / float64(m.InvalidEntries+m.ValidEntries)
		ratio2 := float64(m.InvalidBytes) / float64(m.InvalidBytes+m.ValidBytes)
		if ratio1 >= threshold || ratio2 >= threshold {
			fids = append(fids, fid)
		}
	}
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	return fids
}
