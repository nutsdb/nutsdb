package metrics

import (
	"fmt"
	"sort"
	"sync"
)

type (
	Metrics interface {
		UpdateMetrics(change ...int) error
	}
	ValidMetrics interface {
		Metrics
		UpdateValid(change ...int) error
		UpdateInvalid(change ...int) error
	}
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

func ErrTooManyOrTooLessArgs(expect, actual int) error {
	return fmt.Errorf("too many or too less args, expect: %d, actual: %d", expect, actual)
}

func Init()                        { once.Do(func() { dbfm = make(dbFileMetrics) }) }
func reset()                       { dbfm = make(dbFileMetrics) }
func GetZeroMetrics() *FileMetrics { return &FileMetrics{0, 0, 0, 0} }

func DeleteMetrics(fd int)                   { delete(dbfm, int32(fd)) }
func PutMetrics(fd int, m *FileMetrics)      { dbfm[int32(fd)] = *m }
func GetMetrics(fd int) (*FileMetrics, bool) { m, ok := dbfm[int32(fd)]; return &m, ok }
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

func (m *FileMetrics) UpdateValid(change ...int) error {
	if len(change) != 2 {
		return ErrTooManyOrTooLessArgs(2, len(change))
	}
	m.ValidEntries += int32(change[0])
	m.ValidBytes += int64(change[1])
	return nil
}

func (m *FileMetrics) UpdateInvalid(change ...int) error {
	if len(change) != 2 {
		return ErrTooManyOrTooLessArgs(2, len(change))
	}
	m.InvalidEntries += int32(change[0])
	m.InvalidBytes += int64(change[1])
	return nil
}

func (m *FileMetrics) Update(change ...int) error {
	if len(change) != 4 {
		return ErrTooManyOrTooLessArgs(4, len(change))
	}
	m.ValidEntries += int32(change[0])
	m.InvalidEntries += int32(change[1])
	m.ValidBytes += int64(change[2])
	m.InvalidBytes += int64(change[3])
	return nil
}
