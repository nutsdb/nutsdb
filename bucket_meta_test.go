// Copyright 2019 The nutsdb Author. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nutsdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type BucketTestSuite struct {
	suite.Suite
	bucketMeat   *BucketMeta
	exceptEncode []byte
	tempFile     string
}

func (suite *BucketTestSuite) SetupSuite() {
	suite.bucketMeat = &BucketMeta{
		start:     []byte("key100"),
		end:       []byte("key999"),
		startSize: 6,
		endSize:   6,
	}
	suite.exceptEncode = []byte{51, 34, 113, 225, 6, 0, 0, 0, 6, 0, 0, 0, 107, 101, 121, 49, 48, 48, 107, 101, 121, 57, 57, 57}
	suite.tempFile = "/tmp/metadata.meta"
	fd, err := os.OpenFile(filepath.Clean(suite.tempFile), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		require.Failf(suite.T(), "init file fail", err.Error())
	}
	fd.WriteAt(suite.exceptEncode, 0)
	defer fd.Close()
}

func (suite *BucketTestSuite) TestEncode() {
	encodeValue := suite.bucketMeat.Encode()
	assert.Equal(suite.T(), suite.exceptEncode, encodeValue)
}

func (suite *BucketTestSuite) TestReadBucketMeta() {
	bucket, err := ReadBucketMeta(suite.tempFile)
	assert.Nil(suite.T(), err)
	assert.ObjectsAreEqual(bucket, suite.bucketMeat)
}

func TestBucketSuit(t *testing.T) {
	suite.Run(t, new(BucketTestSuite))
}
