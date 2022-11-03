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
	"fmt"
	"github.com/xujiajun/nutsdb/consts"
	"github.com/xujiajun/nutsdb/model"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	tree     *BPTree
	expected Records

	keyFormat = "key_%03d"
	valFormat = "val_%03d"
)

func withBPTree(t *testing.T, fn func(t *testing.T, tree *BPTree)) {
	tree = NewTree()
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf(keyFormat, i))
		val := []byte(fmt.Sprintf(valFormat, i))

		err := tree.Insert(key,
			&model.Entry{Key: key, Value: val},
			&model.Hint{Key: key, Meta: &model.MetaData{Flag: consts.DataSetFlag}},
			consts.CountFlagEnabled)
		require.NoError(t, err)
	}

	fn(t, tree)
}

func setup(t *testing.T, limit int) {
	tree = NewTree()
	for i := 0; i < 100; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_" + fmt.Sprintf("%03d", i))
		err := tree.Insert(key, &model.Entry{Key: key, Value: val}, &model.Hint{Key: key, Meta: &model.MetaData{
			Flag: consts.DataSetFlag,
		}}, consts.CountFlagEnabled)
		require.NoError(t, err)
	}

	expected = Records{}
	for i := 0; i < limit; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_" + fmt.Sprintf("%03d", i))

		expected = append(expected, &model.Record{E: &model.Entry{Key: key, Value: val}, H: &model.Hint{Key: key, Meta: &model.MetaData{
			Flag: consts.DataSetFlag,
		}}})
	}
}

func TestBPTree_Find(t *testing.T) {
	setup(t, 10)

	var (
		key = []byte("key_001")
		val = []byte("val_001")
	)

	r, err := tree.Find(key)
	require.NoError(t, err)

	assert.Equal(t, val, r.E.Value)
}

func TestBPTree_PrefixScan(t *testing.T) {

	t.Run("prefix scan in empty b+ tree", func(t *testing.T) {
		tree = NewTree()
		_, _, err := tree.PrefixScan([]byte("key_001"), 0, 10)
		assert.Error(t, err)
	})

	t.Run("prefix scan from beginning", func(t *testing.T) {
		withBPTree(t, func(t *testing.T, tree *BPTree) {
			const limit = 10

			records, _, err := tree.PrefixScan([]byte("key_"), 0, limit)
			assert.NoError(t, err)

			for i, r := range records {
				wantKey := []byte(fmt.Sprintf(keyFormat, i))
				wantValue := []byte(fmt.Sprintf(valFormat, i))

				assert.Equal(t, wantKey, r.E.Key)
				assert.Equal(t, wantValue, r.E.Value)
			}
		})
	})

	t.Run("prefix scan in the middle", func(t *testing.T) {
		withBPTree(t, func(t *testing.T, tree *BPTree) {
			const (
				offset = 10
				limit  = 10
			)
			records, _, err := tree.PrefixScan([]byte("key_"), offset, limit)
			assert.NoError(t, err)

			assert.Equal(t, limit, len(records))
			for i, r := range records {
				wantKey := []byte(fmt.Sprintf(keyFormat, i+offset))
				wantValue := []byte(fmt.Sprintf(valFormat, i+offset))

				assert.Equal(t, wantKey, r.E.Key)
				assert.Equal(t, wantValue, r.E.Value)
			}
		})
	})

	t.Run("prefix scan in the end", func(t *testing.T) {
		withBPTree(t, func(t *testing.T, tree *BPTree) {
			const (
				offset = 90
				limit  = 100
			)
			records, _, err := tree.PrefixScan([]byte("key_"), offset, limit)
			assert.NoError(t, err)

			const wantLen = 10 // only left 10 records
			assert.Equal(t, wantLen, len(records))
			for i, r := range records {
				wantKey := []byte(fmt.Sprintf(keyFormat, i+offset))
				wantValue := []byte(fmt.Sprintf(valFormat, i+offset))

				assert.Equal(t, wantKey, r.E.Key)
				assert.Equal(t, wantValue, r.E.Value)
			}
		})
	})

	t.Run("prefix scan for not exists pre-key", func(t *testing.T) {
		withBPTree(t, func(t *testing.T, tree *BPTree) {
			_, _, err = tree.PrefixScan([]byte("key_xx"), 0, 10)
			assert.Error(t, err)
		})
	})

	t.Run("prefix scan after insert a new record", func(t *testing.T) {
		withBPTree(t, func(t *testing.T, tree *BPTree) {
			for i := 0; i <= 100; i++ {
				key := []byte("name_" + fmt.Sprintf("%03d", i))
				val := []byte("val_" + fmt.Sprintf("%03d", i))
				err := tree.Insert(key, &model.Entry{Key: key, Value: val}, &model.Hint{Key: key, Meta: &model.MetaData{
					Flag: consts.DataSetFlag,
				}}, consts.CountFlagEnabled)
				require.NoError(t, err)
			}

			const limit = 1
			records, _, err := tree.PrefixScan([]byte("name_"), 5, limit)
			assert.NoError(t, err)
			assert.Equal(t, limit, len(records))
			assert.Equal(t, []byte("name_005"), records[0].E.Key)
		})
	})
}

func TestBPTree_PrefixSearchScan(t *testing.T) {

	regs := "001"
	regm := "005"
	regl := "099"

	tree = NewTree()
	_, _, err := tree.PrefixSearchScan([]byte("key_"), regs, 1, 10)
	assert.Error(t, err)

	limit := 10
	setup(t, limit)

	rgxs := regexp.MustCompile(regs)
	rgxm := regexp.MustCompile(regm)
	rgxl := regexp.MustCompile(regl)

	// prefix search scan
	rss, _, err := tree.PrefixSearchScan([]byte("key_"), regs, 1, limit)
	assert.NoError(t, err)

	// prefix search scan
	rsm, _, err := tree.PrefixSearchScan([]byte("key_"), regm, 5, limit)
	assert.NoError(t, err)

	// prefix search scan
	rsl, _, err := tree.PrefixSearchScan([]byte("key_"), regl, 99, limit)
	assert.NoError(t, err)

	for i, e := range rss {

		if !rgxs.Match(expected[i].E.Key) {
			continue
		}

		assert.Equal(t, string(expected[i].E.Key), string(e.E.Key))

		if string(expected[i].E.Key) == string(e.E.Key) {
			break
		}

		t.Errorf("err prefix search Scan. Regexp not found")
	}

	for i, e := range rsm {

		if !rgxm.Match(expected[i].E.Key) {
			continue
		}

		assert.Equal(t, string(expected[i].E.Key), string(e.E.Key))

		if string(expected[i].E.Key) == string(e.E.Key) {
			break
		}

		t.Errorf("err prefix search Scan. Regexp not found")
	}

	for i, e := range rsl {

		if !rgxl.Match(expected[i].E.Key) {
			continue
		}

		assert.Equal(t, string(expected[i].E.Key), string(e.E.Key))

		if string(expected[i].E.Key) == string(e.E.Key) {
			break
		}

		t.Errorf("err prefix search Scan. Regexp not found")
	}

	for i := 0; i <= 100; i++ {
		key := []byte("name_" + fmt.Sprintf("%03d", i))
		val := []byte("val_" + fmt.Sprintf("%03d", i))
		err := tree.Insert(key, &model.Entry{Key: key, Value: val}, &model.Hint{Key: key, Meta: &model.MetaData{
			Flag: consts.DataSetFlag,
		}}, consts.CountFlagEnabled)
		require.NoError(t, err)
	}

	_, _, err = tree.PrefixSearchScan([]byte("name_"), "005", 5, limit)
	assert.NoError(t, err)

	_, _, err = tree.PrefixSearchScan([]byte("key_"), "099", 99, limit)
	assert.NoError(t, err)
}

func TestBPTree_All(t *testing.T) {
	tree = NewTree()
	_, err := tree.All()
	assert.Error(t, err)

	setup(t, 100)
	rs, err := tree.All()
	assert.NoError(t, err)

	assert.Equal(t, rs, expected)
}

func TestBPTree_Range(t *testing.T) {
	tree = NewTree()
	_, err := tree.Range([]byte("key_001"), []byte("key_010"))
	assert.Error(t, err)

	limit := 10
	setup(t, limit)

	rs, err := tree.Range([]byte("key_000"), []byte("key_009"))
	assert.NoError(t, err)

	for i, e := range rs {
		assert.Equal(t, expected[i].E.Key, e.E.Key)
	}

	_, err = tree.Range([]byte("key_101"), []byte("key_110"))
	assert.Error(t, err)

	_, err = tree.Range([]byte("key_101"), []byte("key_100"))
	assert.Error(t, err)
}

func TestBPTree_FindLeaf(t *testing.T) {
	limit := 10
	setup(t, limit)

	node := tree.FindLeaf([]byte("key_001"))
	assert.Equal(t, []byte("key_000"), node.Keys[0])
}

func TestRecordExpired(t *testing.T) {
	expiredTime := uint64(time.Now().Add(-1 * time.Hour).Unix())

	t.Run("test expired record", func(t *testing.T) {

		record := &model.Record{
			H: &model.Hint{
				Meta: &model.MetaData{
					Timestamp: expiredTime,
					TTL:       10,
				},
			},
			E: nil,
		}

		assert.True(t, record.IsExpired())
	})

	t.Run("test persistent record", func(t *testing.T) {
		record := &model.Record{
			H: &model.Hint{
				Meta: &model.MetaData{
					Timestamp: expiredTime,
					TTL:       consts.Persistent,
				},
			},
			E: nil,
		}

		assert.False(t, record.IsExpired())
	})
}

func TestBPTree_Update(t *testing.T) {
	limit := 10
	setup(t, limit)

	for i := 0; i <= 100; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_modify" + fmt.Sprintf("%03d", i))
		err := tree.Insert(key, &model.Entry{Key: key, Value: val}, &model.Hint{Key: key, Meta: &model.MetaData{
			Flag: consts.DataSetFlag,
		}}, consts.CountFlagEnabled)

		require.NoError(t, err)
	}

	expected := Records{}
	for i := 0; i <= limit; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_modify" + fmt.Sprintf("%03d", i))

		expected = append(expected, &model.Record{E: &model.Entry{Key: key, Value: val}, H: &model.Hint{Key: key, Meta: &model.MetaData{
			Flag: consts.DataSetFlag}},
		})
	}

	rs, err := tree.Range([]byte("key_000"), []byte("key_009"))
	assert.NoError(t, err)

	for i, e := range rs {
		assert.Equal(t, expected[i].E.Key, e.E.Key)
	}

	// delete
	for i := 1; i <= limit; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		err := tree.Insert(key, &model.Entry{Key: key, Value: nil}, &model.Hint{Key: key, Meta: &model.MetaData{
			Flag: consts.DataDeleteFlag,
		}}, consts.CountFlagEnabled)

		assert.NoError(t, err)
	}

	rs, err = tree.Range([]byte("key_001"), []byte("key_010"))
	assert.NoError(t, err)

	for _, v := range rs {
		assert.Equal(t, consts.DataDeleteFlag, v.H.Meta.Flag)
	}

	key := []byte("key_001")
	err = tree.Insert(key, &model.Entry{Key: key, Value: nil}, &model.Hint{Key: key, Meta: &model.MetaData{
		Flag: consts.DataSetFlag,
	}}, consts.CountFlagEnabled)
	assert.NoError(t, err)

	r, err := tree.Find(key)
	assert.NoError(t, err)
	assert.Equal(t, consts.DataSetFlag, r.H.Meta.Flag)
}
