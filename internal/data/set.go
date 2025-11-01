package data

import "errors"

var (
	// ErrSetNotExist is returned when the key does not exist.
	ErrSetNotExist = errors.New("set not exist")

	// ErrSetMemberNotExist is returned when the member of set does not exist
	ErrSetMemberNotExist = errors.New("set member not exist")

	// ErrMemberEmpty is returned when the item received is nil
	ErrMemberEmpty = errors.New("item empty")
)
