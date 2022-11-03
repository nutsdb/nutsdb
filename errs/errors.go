package errs

import (
	"errors"
	errors2 "github.com/pkg/errors"
	"github.com/xujiajun/nutsdb/consts"
)

var (
	// ErrCrcZero is returned when crc is 0
	ErrCrcZero = errors.New("error crc is 0")

	// ErrCrc is returned when crc is error
	ErrCrc = errors.New("crc error")

	// ErrCapacity is returned when capacity is error.
	ErrCapacity = errors.New("capacity error")
)

var (
	// ErrDBClosed is returned when db is closed.
	ErrDBClosed = errors.New("db is closed")

	// ErrBucket is returned when bucket is not in the HintIdx.
	ErrBucket = errors.New("err bucket")

	// ErrEntryIdxModeOpt is returned when set db EntryIdxMode option is wrong.
	ErrEntryIdxModeOpt = errors.New("err EntryIdxMode option set")

	// ErrFn is returned when fn is nil.
	ErrFn = errors.New("err fn")

	// ErrBucketNotFound is returned when looking for bucket that does not exist
	ErrBucketNotFound = errors.New("bucket not found")

	// ErrNotSupportHintBPTSparseIdxMode is returned not support mode `HintBPTSparseIdxMode`
	ErrNotSupportHintBPTSparseIdxMode = errors.New("not support mode `HintBPTSparseIdxMode`")
)

var (
	// ErrStartKey is returned when Range is called by a error start key.
	ErrStartKey = errors.New("err start key")

	// ErrScansNoResult is returned when Range or prefixScan or prefixSearchScan are called no result to found.
	ErrScansNoResult = errors.New("range scans or prefix or prefix and search scans no result")

	// ErrPrefixSearchScansNoResult is returned when prefixSearchScan is called no result to found.
	ErrPrefixSearchScansNoResult = errors.New("prefix and search scans no result")

	// ErrKeyNotFound is returned when the key is not in the b+ tree.
	ErrKeyNotFound = errors.New("key not found")

	// ErrBadRegexp is returned when bad regular expression given.
	ErrBadRegexp = errors.New("bad regular expression")
)

var (
	// ErrKeyAndValSize is returned when given key and value size is too big.
	ErrKeyAndValSize = errors.New("key and value size too big")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx is closed")

	// ErrTxNotWritable is returned when performing a write operation on
	// a read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrKeyEmpty is returned if an empty key is passed on an update function.
	ErrKeyEmpty = errors.New("key cannot be empty")

	// ErrBucketEmpty is returned if bucket is empty.
	ErrBucketEmpty = errors.New("bucket is empty")

	// ErrRangeScan is returned when range scanning not found the result
	ErrRangeScan = errors.New("range scans not found")

	// ErrPrefixScan is returned when prefix scanning not found the result
	ErrPrefixScan = errors.New("prefix scans not found")

	// ErrPrefixSearchScan is returned when prefix and search scanning not found the result
	ErrPrefixSearchScan = errors.New("prefix and search scans not found")

	// ErrNotFoundKey is returned when key not found int the bucket on an view function.
	ErrNotFoundKey = errors.New("key not found in the bucket")
)

var (
	// ErrSeparatorForListKey returns when list key contains the SeparatorForListKey.
	ErrSeparatorForListKey = errors2.Errorf("contain separator (%s) for List key", consts.SeparatorForListKey)
)

// IsDBClosed is true if the error indicates the db was closed.
func IsDBClosed(err error) bool {
	return errors.Is(err, ErrDBClosed)
}

// IsKeyNotFound is true if the error indicates the key is not found.
func IsKeyNotFound(err error) bool {
	return errors.Is(err, ErrKeyNotFound)
}

// IsBucketNotFound is true if the error indicates the bucket is not exists.
func IsBucketNotFound(err error) bool {
	return errors.Is(err, ErrBucketNotFound)
}

// IsBucketEmpty is true if the bucket is empty.
func IsBucketEmpty(err error) bool {
	return errors.Is(err, ErrBucketEmpty)
}

// IsKeyEmpty is true if the key is empty.
func IsKeyEmpty(err error) bool {
	return errors.Is(err, ErrKeyEmpty)
}

// IsPrefixScan is true if prefix scanning not found the result.
func IsPrefixScan(err error) bool {
	return errors.Is(err, ErrPrefixScan)
}

// IsPrefixSearchScan is true if prefix and search scanning not found the result.
func IsPrefixSearchScan(err error) bool {
	return errors.Is(err, ErrPrefixScan)
}
