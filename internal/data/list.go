package data

import "errors"

var (
	// ErrListNotFound is returned when the list not found.
	ErrListNotFound = errors.New("the list not found")

	// ErrCount is returned when count is error.
	ErrCount = errors.New("err count")

	// ErrEmptyList is returned when the list is empty.
	ErrEmptyList = errors.New("the list is empty")

	// ErrStartOrEnd is returned when start > end
	ErrStartOrEnd = errors.New("start or end error")
)

// HeadTailSeq list head and tail seq num
type HeadTailSeq struct {
	Head uint64
	Tail uint64
}

func (seq *HeadTailSeq) GenerateSeq(isLeft bool) uint64 {
	var res uint64
	if isLeft {
		res = seq.Head
		seq.Head--
	} else {
		res = seq.Tail
		seq.Tail++
	}

	return res
}

type List struct{}
