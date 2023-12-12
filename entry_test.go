// Copyright 2019 The nutsdb Authors. All rights reserved.
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
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type EntryTestSuite struct {
	suite.Suite
	entry          Entry
	expectedEncode []byte
}

func (suite *EntryTestSuite) SetupSuite() {
	suite.entry = Entry{
		Key:   []byte("key_0001"),
		Value: []byte("val_0001"),
		Meta: newMetaData().withKeySize(uint32(len("key_0001"))).
			withValueSize(uint32(len("val_0001"))).withTimeStamp(1547707905).withTTL(Persistent).withFlag(DataSetFlag).withBucketId(1),
	}
	suite.expectedEncode = []byte{168, 1, 59, 122, 129, 204, 128, 226, 5, 8, 8, 1, 0, 0, 0, 0, 1, 107, 101, 121, 95, 48, 48, 48, 49, 118, 97, 108, 95, 48, 48, 48, 49}
}

func (suite *EntryTestSuite) TestEncode() {
	ok := reflect.DeepEqual(suite.entry.Encode(), suite.expectedEncode)
	assert.True(suite.T(), ok, "entry's encode test fail")
}

func (suite *EntryTestSuite) TestIsZero() {

	if ok := suite.entry.IsZero(); ok {
		assert.Fail(suite.T(), "entry's IsZero test fail")
	}

}

func (suite *EntryTestSuite) TestGetCrc() {

	headerSize := suite.entry.Meta.size()
	crc1 := suite.entry.GetCrc(suite.expectedEncode[:headerSize])
	crc2 := binary.LittleEndian.Uint32(suite.expectedEncode[:4])

	if crc1 != crc2 {
		assert.Fail(suite.T(), "entry's GetCrc test fail")
	}
}

func TestEntrySuit(t *testing.T) {
	suite.Run(t, new(EntryTestSuite))
}

func TestEntries_processEntriesScanOnDisk(t *testing.T) {
	tests := []struct {
		name       string
		e          Entries
		wantResult []*Entry
	}{
		{
			"sort",
			Entries{
				{
					Key:  []byte("abc"),
					Meta: newMetaData().withTTL(0).withFlag(DataSetFlag),
				},
				{
					Key:  []byte("z"),
					Meta: newMetaData().withTTL(0).withFlag(DataSetFlag),
				},
				{
					Key:  []byte("abcd"),
					Meta: newMetaData().withTTL(0).withFlag(DataSetFlag),
				},
			},
			[]*Entry{
				{
					Key:  []byte("abc"),
					Meta: newMetaData().withTTL(0).withFlag(DataSetFlag),
				},
				{
					Key:  []byte("abcd"),
					Meta: newMetaData().withTTL(0).withFlag(DataSetFlag),
				},
				{
					Key:  []byte("z"),
					Meta: newMetaData().withTTL(0).withFlag(DataSetFlag),
				},
			},
		},
		{
			"expired",
			Entries{
				{
					Key:  []byte("abc"),
					Meta: newMetaData().withTTL(1),
				},
				{
					Key:  []byte("abc"),
					Meta: newMetaData().withTTL(0).withFlag(DataDeleteFlag),
				},
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.wantResult, tt.e.processEntriesScanOnDisk(), "processEntriesScanOnDisk()")
		})
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.wantResult, tt.e.toCEntries(nil).processEntriesScanOnDisk(), "CEntries.processEntriesScanOnDisk()")
		})
	}
}
