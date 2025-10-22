package data

type Item[T any] struct {
	Key    []byte
	Record *T
}

func NewItem[T any](key []byte, record *T) *Item[T] {
	return &Item[T]{
		Key:    key,
		Record: record,
	}
}
