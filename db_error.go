package nutsdb

import "errors"

var (
	// ErrDBClosed is returned when db is closed.
	ErrDBClosed = errors.New("db is closed")

	// ErrBucket is returned when bucket is not in the HintIdx.
	ErrBucket = errors.New("err bucket")

	// ErrFn is returned when fn is nil.
	ErrFn = errors.New("err fn")

	// ErrBucketNotFound is returned when looking for bucket that does not exist
	ErrBucketNotFound = errors.New("bucket not found")

	// ErrDataStructureNotSupported is returned when pass a not supported data structure
	ErrDataStructureNotSupported = errors.New("this data structure is not supported for now")

	// ErrDirLocked is returned when can't get the file lock of dir
	ErrDirLocked = errors.New("the dir of db is locked")

	// ErrDirUnlocked is returned when the file lock already unlocked
	ErrDirUnlocked = errors.New("the dir of db is unlocked")

	// ErrIsMerging is returned when merge in progress
	ErrIsMerging = errors.New("merge in progress")

	// ErrNotSupportMergeWhenUsingList is returned calling 'Merge' when using list
	ErrNotSupportMergeWhenUsingList = errors.New("not support merge when using list for now")

	// ErrRecordIsNil is returned when Record is nil
	ErrRecordIsNil = errors.New("the record is nil")
)
