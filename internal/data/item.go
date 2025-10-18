package data

type Item[T any] struct {
	Key    []byte
	Record *T
}
