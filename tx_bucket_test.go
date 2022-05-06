package nutsdb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TxBucketTestSuite struct {
	suite.Suite
	Db     *DB
	DbHBPT *DB
}

func (suite *TxBucketTestSuite) SetupSuite() {
	fileDir := "/tmp/nutsdbtestbuckettx"
	files, _ := ioutil.ReadDir(fileDir)
	for _, f := range files {
		name := f.Name()
		if name != "" {
			err := os.RemoveAll(fileDir + "/" + name)
			if err != nil {
				panic(err)
			}
		}
	}
	opt = DefaultOptions
	opt.Dir = fileDir
	opt.SegmentSize = 8 * 1024
	suite.Db, err = Open(opt)
	assert.Nil(suite.T(), err)

	tx, err := suite.Db.Begin(true)
	assert.Nil(suite.T(), err)
	//set bucket
	err = tx.SAdd("set_bucket", []byte("set_key"), []byte("set_value"))
	assert.Nil(suite.T(), err)

	//sort set bucket
	err = tx.ZAdd("zset_bucket", []byte("zset_key"), 80, []byte("zset_value"))
	assert.Nil(suite.T(), err)

	//list bucket
	err = tx.LPush("list_bucket", []byte("list_key"), []byte("list_value"))
	assert.Nil(suite.T(), err)

	//string bucket
	err = tx.Put("string_bucket", []byte("string_key"), []byte("string_value"), 0)
	assert.Nil(suite.T(), err)

	err = tx.Commit()
	assert.Nil(suite.T(), err)
}

func (suite *TxBucketTestSuite) TearDownSuite() {
	err := suite.Db.Close()
	assert.Nil(suite.T(), err)

	err = os.RemoveAll("/tmp/nutsdbtestbuckettx")
	if err != nil {
		require.Failf(suite.T(), "remve file fail", err.Error())
	}

	err = suite.DbHBPT.Close()
	assert.Nil(suite.T(), err)

	err = os.RemoveAll("/tmp/nutsdbtestbuckettxx")
	if err != nil {
		require.Failf(suite.T(), "remve file fail", err.Error())
	}
}

func (suite *TxBucketTestSuite) TestA_IterateBuckets() {
	tx, err := suite.Db.Begin(false)
	assert.Nil(suite.T(), err)

	err = tx.IterateBuckets(DataStructureSet, func(bucket string) {
		assert.Equal(suite.T(), "set_bucket", bucket)
	})
	assert.Nil(suite.T(), err)

	err = tx.IterateBuckets(DataStructureSortedSet, func(bucket string) {
		assert.Equal(suite.T(), "zset_bucket", bucket)
	})
	assert.Nil(suite.T(), err)

	err = tx.IterateBuckets(DataStructureList, func(bucket string) {
		assert.Equal(suite.T(), "list_bucket", bucket)
	})
	assert.Nil(suite.T(), err)

	err = tx.IterateBuckets(DataStructureBPTree, func(bucket string) {
		assert.Equal(suite.T(), "string_bucket", bucket)
	})
	assert.Nil(suite.T(), err)

	err = tx.IterateBuckets(DataStructureNone, func(bucket string) {})
	assert.Nil(suite.T(), err)

	err = tx.Commit()
	assert.Nil(suite.T(), err)
}

func (suite *TxBucketTestSuite) TestB_DeleteBucket() {
	tx, err := suite.Db.Begin(true)
	assert.Nil(suite.T(), err)

	err = tx.DeleteBucket(DataStructureSet, "set_bucket")
	assert.Nil(suite.T(), err)

	err = tx.DeleteBucket(DataStructureSortedSet, "zset_bucket")
	assert.Nil(suite.T(), err)

	err = tx.DeleteBucket(DataStructureList, "list_bucket")
	assert.Nil(suite.T(), err)

	err = tx.DeleteBucket(DataStructureBPTree, "string_bucket")
	assert.Nil(suite.T(), err)

	err = tx.DeleteBucket(DataStructureNone, "none_bucket")
	assert.Nil(suite.T(), err)

	err = tx.Commit()
	assert.Nil(suite.T(), err)

}

func (suite *TxBucketTestSuite) TestC_HintBPTSparseIdxMode() {
	fileDir := "/tmp/nutsdbtestbuckettxx"
	files, _ := ioutil.ReadDir(fileDir)
	for _, f := range files {
		name := f.Name()
		if name != "" {
			err := os.RemoveAll(fileDir + "/" + name)
			if err != nil {
				panic(err)
			}
		}
	}
	opt = DefaultOptions
	opt.Dir = fileDir
	opt.SegmentSize = 8 * 1024
	opt.EntryIdxMode = HintBPTSparseIdxMode
	suite.DbHBPT, err = Open(opt)
	assert.Nil(suite.T(), err)

	tx, err := suite.DbHBPT.Begin(false)
	assert.Nil(suite.T(), err)

	err = tx.IterateBuckets(DataStructureList, func(bucket string) {})
	assert.Error(suite.T(), err)

	err = tx.DeleteBucket(DataStructureList, "")
	assert.Error(suite.T(), err)

	err = tx.Commit()
	assert.Nil(suite.T(), err)
}

func TestTxBucketSuit(t *testing.T) {
	suite.Run(t, new(TxBucketTestSuite))
}
