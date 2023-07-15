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
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
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

		meta := &MetaData{Flag: DataSetFlag}
		err := tree.Insert(key,
			&Entry{Key: key, Value: val},
			NewHint().WithKey(key).WithMeta(meta),
			CountFlagEnabled)
		require.NoError(t, err)
	}

	fn(t, tree)
}

func setup(t *testing.T, limit int) {
	tree = NewTree()
	meta := &MetaData{
		Flag: DataSetFlag,
	}
	for i := 0; i < 100; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_" + fmt.Sprintf("%03d", i))
		err := tree.Insert(key, &Entry{Key: key, Value: val}, NewHint().WithKey(key).WithMeta(meta), CountFlagEnabled)
		require.NoError(t, err)
	}

	expected = Records{}
	for i := 0; i < limit; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_" + fmt.Sprintf("%03d", i))

		expected = append(expected, &Record{E: &Entry{Key: key, Value: val}, H: NewHint().WithKey(key).WithMeta(meta)})
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
			meta := &MetaData{
				Flag: DataSetFlag,
			}
			for i := 0; i <= 100; i++ {
				key := []byte("name_" + fmt.Sprintf("%03d", i))
				val := []byte("val_" + fmt.Sprintf("%03d", i))
				err := tree.Insert(key, &Entry{Key: key, Value: val}, NewHint().WithKey(key).WithMeta(meta), CountFlagEnabled)
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

	meta := &MetaData{
		Flag: DataSetFlag,
	}
	for i := 0; i <= 100; i++ {
		key := []byte("name_" + fmt.Sprintf("%03d", i))
		val := []byte("val_" + fmt.Sprintf("%03d", i))
		err := tree.Insert(key, &Entry{Key: key, Value: val}, NewHint().WithKey(key).WithMeta(meta), CountFlagEnabled)
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

		meta := &MetaData{
			Timestamp: expiredTime,
			TTL:       10,
		}
		record := &Record{
			H: NewHint().WithMeta(meta),
			E: nil,
		}

		assert.True(t, record.IsExpired())
	})

	t.Run("test persistent record", func(t *testing.T) {
		meta := &MetaData{
			Timestamp: expiredTime,
			TTL:       Persistent,
		}
		record := &Record{
			H: NewHint().WithMeta(meta),
			E: nil,
		}

		assert.False(t, record.IsExpired())
	})
}

func TestBPTree_Update(t *testing.T) {
	limit := 10
	setup(t, limit)

	meta := &MetaData{
		Flag: DataSetFlag,
	}
	for i := 0; i <= 100; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_modify" + fmt.Sprintf("%03d", i))
		err := tree.Insert(key, &Entry{Key: key, Value: val}, NewHint().WithKey(key).WithMeta(meta), CountFlagEnabled)

		require.NoError(t, err)
	}

	expected := Records{}
	meta = &MetaData{
		Flag: DataSetFlag,
	}
	for i := 0; i <= limit; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_modify" + fmt.Sprintf("%03d", i))

		expected = append(expected, &Record{E: &Entry{Key: key, Value: val}, H: NewHint().WithKey(key).WithMeta(meta)})
	}

	rs, err := tree.Range([]byte("key_000"), []byte("key_009"))
	assert.NoError(t, err)

	for i, e := range rs {
		assert.Equal(t, expected[i].E.Key, e.E.Key)
	}

	// delete
	meta = &MetaData{
		Flag: DataDeleteFlag,
	}
	for i := 1; i <= limit; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		err := tree.Insert(key, &Entry{Key: key, Value: nil}, NewHint().WithKey(key).WithMeta(meta), CountFlagEnabled)

		assert.NoError(t, err)
	}

	rs, err = tree.Range([]byte("key_001"), []byte("key_010"))
	assert.NoError(t, err)

	for _, v := range rs {
		assert.Equal(t, DataDeleteFlag, v.H.Meta.Flag)
	}

	key := []byte("key_001")
	meta = &MetaData{
		Flag: DataSetFlag,
	}
	err = tree.Insert(key, &Entry{Key: key, Value: nil}, NewHint().WithKey(key).WithMeta(meta), CountFlagEnabled)
	assert.NoError(t, err)

	r, err := tree.Find(key)
	assert.NoError(t, err)
	assert.Equal(t, DataSetFlag, r.H.Meta.Flag)
}

func TestBPTree_SetKeyPosMap(t *testing.T) {
	tree = NewTree()

	keyPosMap := map[string]int64{
		"key_001": 1,
		"key_002": 2,
		"key_003": 3,
	}
	tree.SetKeyPosMap(keyPosMap)
	assert.Equal(t, keyPosMap, tree.keyPosMap)
}

func TestBPTree_ToBinary(t *testing.T) {
	// change keyFormat into a pure integer
	keyFormat = "%03d"

	t.Run("test leaf node without keyPosMap", func(t *testing.T) {
		withBPTree(t, func(t *testing.T, tree *BPTree) {
			key := []byte("001")

			n := tree.FindLeaf(key)
			r, err := tree.ToBinary(n)
			assert.NoError(t, err)

			bin := BinaryNode{
				Keys:        [7]int64{0, 1, 2, 3},
				Pointers:    [9]int64{},
				IsLeaf:      1,
				KeysNum:     4,
				Address:     0,
				NextAddress: -1,
			}
			buf := new(bytes.Buffer)
			err = binary.Write(buf, binary.LittleEndian, bin)
			assert.NoError(t, err)

			assert.Equal(t, r, buf.Bytes())
		})
	})

	t.Run("test leaf node with keyPosMap", func(t *testing.T) {
		withBPTree(t, func(t *testing.T, tree *BPTree) {
			key := []byte("001")
			keyPosMap := map[string]int64{"000": 3, "001": 2, "002": 1, "003": 0}

			tree.enabledKeyPosMap = true
			tree.keyPosMap = keyPosMap
			n := tree.FindLeaf(key)
			r, err := tree.ToBinary(n)
			assert.NoError(t, err)

			bin := BinaryNode{
				Keys:        [7]int64{3, 2, 1, 0},
				Pointers:    [9]int64{},
				IsLeaf:      1,
				KeysNum:     4,
				Address:     0,
				NextAddress: -1,
			}
			buf := new(bytes.Buffer)
			err = binary.Write(buf, binary.LittleEndian, bin)
			assert.NoError(t, err)

			assert.Equal(t, r, buf.Bytes())
		})
	})

	t.Run("test non-leaf node without keyPosMap", func(t *testing.T) {
		withBPTree(t, func(t *testing.T, tree *BPTree) {
			n := tree.root
			r, err := tree.ToBinary(n)
			assert.NoError(t, err)

			bin := BinaryNode{
				Keys:        [7]int64{20, 40, 60, 80},
				Pointers:    [9]int64{304, 1520, 2584, 3496, 4408},
				IsLeaf:      0,
				KeysNum:     4,
				Address:     1672,
				NextAddress: -1,
			}
			buf := new(bytes.Buffer)
			err = binary.Write(buf, binary.LittleEndian, bin)
			assert.NoError(t, err)

			assert.Equal(t, r, buf.Bytes())
		})
	})

	t.Run("test non-leaf node with keyPosMap", func(t *testing.T) {
		withBPTree(t, func(t *testing.T, tree *BPTree) {
			keyPosMap := map[string]int64{"020": 80, "040": 60, "060": 40, "080": 20}

			tree.enabledKeyPosMap = true
			tree.keyPosMap = keyPosMap
			n := tree.root
			r, err := tree.ToBinary(n)
			assert.NoError(t, err)

			bin := BinaryNode{
				Keys:        [7]int64{80, 60, 40, 20},
				Pointers:    [9]int64{304, 1520, 2584, 3496, 4408},
				IsLeaf:      0,
				KeysNum:     4,
				Address:     1672,
				NextAddress: -1,
			}
			buf := new(bytes.Buffer)
			err = binary.Write(buf, binary.LittleEndian, bin)
			assert.NoError(t, err)

			assert.Equal(t, r, buf.Bytes())
		})
	})
}

func TestBPTree_WriteNode(t *testing.T) {
	testFilename := "bptree_write_test.bptidx"

	t.Run("test write node", func(t *testing.T) {
		withBPTree(t, func(t *testing.T, tree *BPTree) {
			key := []byte("key_001")
			tree.Filepath = testFilename

			fd, err := os.OpenFile(tree.Filepath, os.O_CREATE|os.O_RDWR, 0644)
			assert.NoError(t, err)

			node := tree.FindLeaf(key)
			num, err := tree.WriteNode(node, -1, false, fd)
			assert.NoError(t, err)

			bnByte, err := tree.ToBinary(node)
			assert.NoError(t, err)
			assert.Equal(t, num, len(bnByte))
		})
	})

	// remove the test file
	_ = os.Remove(testFilename)
}
