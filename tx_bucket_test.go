package nutsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	setBucketName    = "set_bucket"
	zSetBucketName   = "zset_bucket"
	listBucketName   = "list_bucket"
	stringBucketName = "string_bucket"
)

func setupBucket(t *testing.T, db *DB) {
	key := GetTestBytes(0)
	val := GetTestBytes(1)

	txSAdd(t, db, setBucketName, key, val, nil)
	txZAdd(t, db, zSetBucketName, key, val, 80, nil)
	txPush(t, db, listBucketName, key, val, nil, true)
	txPut(t, db, stringBucketName, key, val, Persistent, nil)
}

func TestA_IterateBuckets(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		setupBucket(t, db)

		txIterateBuckets(t, db, DataStructureNone, "*", nil, nil)

		txIterateBuckets(t, db, DataStructureSet, "*", func(bucket string) bool {
			return true
		}, nil, setBucketName)

		txIterateBuckets(t, db, DataStructureSortedSet, "*", func(bucket string) bool {
			return true
		}, nil, zSetBucketName)

		txIterateBuckets(t, db, DataStructureList, "*", func(bucket string) bool {
			return true
		}, nil, listBucketName)

		txIterateBuckets(t, db, DataStructureTree, "*", func(bucket string) bool {
			return true
		}, nil, stringBucketName)

		matched := false
		txIterateBuckets(t, db, DataStructureTree, "str*", func(bucket string) bool {
			matched = true
			return true
		}, nil)
		assert.Equal(t, true, matched)

		matched = false
		txIterateBuckets(t, db, DataStructureList, "str*", func(bucket string) bool {
			return true
		}, nil)
		assert.Equal(t, false, matched)
	})
}

func TestB_DeleteBucket(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		setupBucket(t, db)

		txDeleteBucket(t, db, DataStructureSet, setBucketName, nil)
		txDeleteBucket(t, db, DataStructureSortedSet, zSetBucketName, nil)
		txDeleteBucket(t, db, DataStructureList, listBucketName, nil)
		txDeleteBucket(t, db, DataStructureTree, stringBucketName, nil)

		txDeleteBucket(t, db, DataStructureNone, "none_bucket", ErrDataStructureNotSupported)
	})
}

func TestC_HintBPTSparseIdxMode(t *testing.T) {
	opt = DefaultOptions
	opt.Dir = "/tmp/nutsdbtestbuckettxx"
	opt.SegmentSize = 8 * 1024
	opt.EntryIdxMode = HintBPTSparseIdxMode

	runNutsDBTest(t, &opt, func(t *testing.T, db *DB) {
		txIterateBuckets(t, db, DataStructureList, "*", func(bucket string) bool {
			return true
		}, ErrNotSupportHintBPTSparseIdxMode)

		txDeleteBucket(t, db, DataStructureList, "", ErrNotSupportHintBPTSparseIdxMode)
	})
}
