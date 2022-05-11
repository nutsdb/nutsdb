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
