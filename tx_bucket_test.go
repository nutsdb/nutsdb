package nutsdb

import (
	"github.com/xujiajun/nutsdb/consts"
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

	err = tx.IterateBuckets(consts.DataStructureSet, "*", func(bucket string) bool {
		assert.Equal(suite.T(), "set_bucket", bucket)
		return true
	})
	assert.Nil(suite.T(), err)

	err = tx.IterateBuckets(consts.DataStructureSortedSet, "*", func(bucket string) bool {
		assert.Equal(suite.T(), "zset_bucket", bucket)
		return true
	})
	assert.Nil(suite.T(), err)

	err = tx.IterateBuckets(consts.DataStructureList, "*", func(bucket string) bool {
		assert.Equal(suite.T(), "list_bucket", bucket)
		return true
	})
	assert.Nil(suite.T(), err)

	err = tx.IterateBuckets(consts.DataStructureBPTree, "*", func(bucket string) bool {
		assert.Equal(suite.T(), "string_bucket", bucket)
		return true
	})
	assert.Nil(suite.T(), err)

	matched := false
	_ = tx.IterateBuckets(consts.DataStructureBPTree, "str*", func(bucket string) bool {
		matched = true
		return true
	})
	assert.Equal(suite.T(), true, matched)

	matched = false
	_ = tx.IterateBuckets(consts.DataStructureList, "str*", func(bucket string) bool {
		matched = true
		return true
	})
	assert.Equal(suite.T(), false, matched)

	err = tx.IterateBuckets(consts.DataStructureNone, "*", func(bucket string) bool {
		return true
	})
	assert.Nil(suite.T(), err)

	err = tx.Commit()
	assert.Nil(suite.T(), err)
}

func (suite *TxBucketTestSuite) TestB_DeleteBucket() {
	tx, err := suite.Db.Begin(true)
	assert.Nil(suite.T(), err)

	err = tx.DeleteBucket(consts.DataStructureSet, "set_bucket")
	assert.Nil(suite.T(), err)

	err = tx.DeleteBucket(consts.DataStructureSortedSet, "zset_bucket")
	assert.Nil(suite.T(), err)

	err = tx.DeleteBucket(consts.DataStructureList, "list_bucket")
	assert.Nil(suite.T(), err)

	err = tx.DeleteBucket(consts.DataStructureBPTree, "string_bucket")
	assert.Nil(suite.T(), err)

	err = tx.DeleteBucket(consts.DataStructureNone, "none_bucket")
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
	opt.EntryIdxMode = consts.HintBPTSparseIdxMode
	suite.DbHBPT, err = Open(opt)
	assert.Nil(suite.T(), err)

	tx, err := suite.DbHBPT.Begin(false)
	assert.Nil(suite.T(), err)

	err = tx.IterateBuckets(consts.DataStructureList, "*", func(bucket string) bool {
		return true
	})
	assert.Error(suite.T(), err)

	err = tx.DeleteBucket(consts.DataStructureList, "")
	assert.Error(suite.T(), err)

	err = tx.Commit()
	assert.Nil(suite.T(), err)
}

func TestTxBucketSuit(t *testing.T) {
	suite.Run(t, new(TxBucketTestSuite))
}
