package nutsdb

import "errors"

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
	return errors.Is(err, ErrPrefixSearchScan)
}
