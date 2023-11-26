package nutsdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBucketManager_NewBucketAndDeleteBucket(t *testing.T) {
	bucket1 := "bucket_1"
	bucket2 := "bucket_2"
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txNewBucket(t, db, bucket1, DataStructureBTree, nil, nil)
		exist := db.bm.ExistBucket(Ds(DataStructureBTree), BucketName(bucket1))
		assert.Equal(t, true, exist)
		txNewBucket(t, db, bucket2, DataStructureBTree, nil, nil)
		exist = db.bm.ExistBucket(Ds(DataStructureBTree), BucketName(bucket2))
		assert.Equal(t, true, exist)
	})

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txNewBucket(t, db, bucket1, DataStructureBTree, nil, nil)
		exist := db.bm.ExistBucket(Ds(DataStructureBTree), BucketName(bucket1))
		assert.Equal(t, true, exist)
		txDeleteBucketFunc(t, db, bucket1, DataStructureBTree, nil, nil)
		exist = db.bm.ExistBucket(Ds(DataStructureBTree), BucketName(bucket1))
		assert.Equal(t, false, exist)
	})
}

func TestBucketManager_ExistBucket(t *testing.T) {
	bucket1 := "bucket_1"
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		exist := db.bm.ExistBucket(Ds(DataStructureBTree), BucketName(bucket1))
		assert.Equal(t, false, exist)

		txNewBucket(t, db, bucket1, DataStructureBTree, nil, nil)
		exist = db.bm.ExistBucket(Ds(DataStructureBTree), BucketName(bucket1))
		assert.Equal(t, true, exist)
	})
}

func TestBucketManager_Recovery(t *testing.T) {
	dir := "/tmp/nutsdb_test_data"
	const bucket1 = "bucket_1"
	const bucket2 = "bucket_2"
	db, err := Open(DefaultOptions, WithDir(dir))
	defer removeDir(dir)
	assert.NotNil(t, db)
	assert.Nil(t, err)
	txNewBucket(t, db, bucket1, DataStructureBTree, nil, nil)
	txNewBucket(t, db, bucket2, DataStructureBTree, nil, nil)
	txDeleteBucketFunc(t, db, bucket1, DataStructureBTree, nil, nil)
	db.Close()

	db, err = Open(DefaultOptions, WithDir(dir))
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.View(func(tx *Tx) error {
		exist := tx.ExistBucket(DataStructureBTree, bucket2)
		assert.Equal(t, true, exist)
		exist = tx.ExistBucket(DataStructureBTree, bucket1)
		assert.Equal(t, false, exist)
		return nil
	})
	assert.Nil(t, err)
}

func TestBucketManager_DataStructureIsolation(t *testing.T) {
	const bucket1 = "bucket_1"
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket1, nil)

		assert.Equal(t, false, db.bm.ExistBucket(Ds(DataStructureList), bucket1))
		assert.Equal(t, false, db.bm.ExistBucket(Ds(DataStructureSortedSet), bucket1))
		assert.Equal(t, false, db.bm.ExistBucket(Ds(DataStructureSet), bucket1))
	})
}

func TestBucketManager_DeleteBucketIsolation(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		const bucket1 = "bucket_1"
		txCreateBucket(t, db, DataStructureBTree, bucket1, nil)
		txPut(t, db, bucket1, []byte("key_1"), []byte("value_1"), Persistent, nil, nil)
		txDeleteBucket(t, db, DataStructureBTree, bucket1, nil)
		txGet(t, db, bucket1, []byte("key_1"), nil, ErrBucketNotExist)
	})
}

func txNewBucket(t *testing.T, db *DB, bucket string, ds uint16, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		success, err2 := tx.NewBucket(ds, bucket)
		assert.Equal(t, true, success)
		assertErr(t, expectErr, err2)
		return nil
	})
	assertErr(t, err, finalExpectErr)

}

func txDeleteBucketFunc(t *testing.T, db *DB, bucket string, ds uint16, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err2 := tx.DeleteBucket(uint16(ds), bucket)
		assertErr(t, expectErr, err2)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}
