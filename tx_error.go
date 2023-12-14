package nutsdb

import "errors"

var (
	// ErrDataSizeExceed is returned when given key and value size is too big.
	ErrDataSizeExceed = errors.New("data size too big")

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

	// ErrCannotCommitAClosedTx is returned when the tx committing a closed tx
	ErrCannotCommitAClosedTx = errors.New("can not commit a closed tx")

	// ErrCannotRollbackACommittingTx is returned when the tx rollback a committing tx
	ErrCannotRollbackACommittingTx = errors.New("can not rollback a committing tx")

	ErrCannotRollbackAClosedTx = errors.New("can not rollback a closed tx")

	// ErrNotFoundBucket is returned when key not found int the bucket on an view function.
	ErrNotFoundBucket = errors.New("bucket not found")

	// ErrTxnTooBig is returned if too many writes are fit into a single transaction.
	ErrTxnTooBig = errors.New("Txn is too big to fit into one request")

	// ErrTxnExceedWriteLimit is returned when this tx's write is exceed max write record
	ErrTxnExceedWriteLimit = errors.New("txn is exceed max write record count")

	ErrBucketAlreadyExist = errors.New("bucket is already exist")

	ErrorBucketNotExist = errors.New("bucket is not exist yet, please use NewBucket function to create this bucket first")

	ErrValueNotInteger = errors.New("value is not an integer")

	ErrOffsetInvalid = errors.New("offset is invalid")

	ErrKVArgsLenNotEven = errors.New("parameters is used to represent key value pairs and cannot be odd numbers")

	ErrStartGreaterThanEnd = errors.New("start is greater than end")
)
