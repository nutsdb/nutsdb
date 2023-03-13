package metrics

import (
	"sort"
	"sync"
)

type (
	FileMetrics struct {
		ValidEntries   int32
		InvalidEntries int32
		ValidBytes     int64
		InvalidBytes   int64
	}
	dbFileMetrics map[int32]FileMetrics
)

var (
	once sync.Once
	dbfm dbFileMetrics
)

func Init() {
	once.Do(
		func() {
			dbfm = make(dbFileMetrics)
		},
	)
}

func reset() {
	dbfm = make(dbFileMetrics)
}

func DeleteMetrics(fd int) {
	delete(dbfm, int32(fd))
}

func PutMetrics(fd int, m *FileMetrics) {
	dbfm[int32(fd)] = *m
}

func GetMetrics(fd int) (*FileMetrics, bool) {
	m, ok := dbfm[int32(fd)]
	return &m, ok
}

func CountMetrics() int {
	return len(dbfm)
}

func GetFDsExceedThreshold(threshold float64) []int32 {
	var fds []int32
	for fd, m := range dbfm {
		ratio1 := float64(m.InvalidEntries) / float64(m.InvalidEntries+m.ValidEntries)
		ratio2 := float64(m.InvalidBytes) / float64(m.InvalidBytes+m.ValidBytes)
		if ratio1 >= threshold || ratio2 >= threshold {
			fds = append(fds, fd)
		}
	}
	sort.Slice(fds, func(i, j int) bool { return fds[i] < fds[j] })
	return fds
}
