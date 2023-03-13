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

// this method is for test case reset the state.
func reset() {
	dbfm = make(dbFileMetrics)
}

func DeleteFileMetrics(fd int) {
	delete(dbfm, int32(fd))
}

//PutFileMetrics you can start with an existing FileMetrics via GetFileMetrics,
//then update it along the way you update the DB entries,
//then Put it back to dbfm with PutFileMetrics
func PutFileMetrics(fd int, m *FileMetrics) {
	dbfm[int32(fd)] = *m
}

//UpdateFileMetrics you can start with a new &FileMetrics{0,0,0,0},
//then update it along the way you update the DB entries,
//then update it back to dbfm with UpdateFileMetrics
func UpdateFileMetrics(fd int, change *FileMetrics) {
	m := dbfm[int32(fd)]
	m.ValidEntries += change.ValidEntries
	m.InvalidEntries += change.InvalidEntries
	m.ValidBytes += change.ValidBytes
	m.InvalidBytes += change.InvalidBytes
	PutFileMetrics(fd, &m)
}

func GetFileMetrics(fd int) (*FileMetrics, bool) {
	m, ok := dbfm[int32(fd)]
	return &m, ok
}

func CountFileMetrics() int {
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
