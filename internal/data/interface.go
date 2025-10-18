package data

type KeyValueType interface {
	GetKey() []byte
	GetValue() []byte
}
