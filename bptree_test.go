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
	"reflect"
	"regexp"
	"testing"
)

var (
	tree     *BPTree
	expected Records
)

func setup(t *testing.T, limit int) {
	tree = NewTree()
	for i := 0; i < 100; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_" + fmt.Sprintf("%03d", i))
		err := tree.Insert(key, &Entry{Key: key, Value: val}, &Hint{key: key, meta: &MetaData{
			Flag: DataSetFlag,
		}}, CountFlagEnabled)
		if err != nil {
			t.Fatal(err)
		}
	}

	expected = Records{}
	for i := 0; i < limit; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_" + fmt.Sprintf("%03d", i))

		expected = append(expected, &Record{E: &Entry{Key: key, Value: val}, H: &Hint{key: key, meta: &MetaData{
			Flag: DataSetFlag,
		}}})
	}
}

func TestBPTree_Find(t *testing.T) {
	setup(t, 10)

	key := []byte("key_001")
	val := []byte("val_001")

	// find
	r, err := tree.Find(key)
	if err != nil {
		t.Fatal(err)
	}

	if string(r.E.Value) != string(val) {
		t.Errorf("err find. got %s want %s", string(r.E.Value), string(val))
	}
}

func TestBPTree_PrefixScan(t *testing.T) {
	tree = NewTree()
	_, err := tree.PrefixScan([]byte("key_001"), 10)
	if err == nil {
		t.Fatal("err prefix Scan")
	}
	limit := 10
	setup(t, limit)

	// prefix scan
	rs, err := tree.PrefixScan([]byte("key_"), limit)
	if err != nil {
		t.Fatal(err)
	}

	for i, e := range rs {
		if string(expected[i].E.Key) != string(e.E.Key) {
			t.Errorf("err prefix Scan. got %v want %v", string(expected[i].E.Key), string(e.E.Key))
		}
	}

	_, err = tree.PrefixScan([]byte("key_xx"), limit)
	if err == nil {
		t.Error("err prefix Scan")
	}

	for i := 1; i <= 100; i++ {
		key := []byte("name_" + fmt.Sprintf("%03d", i))
		val := []byte("val_" + fmt.Sprintf("%03d", i))
		err := tree.Insert(key, &Entry{Key: key, Value: val}, &Hint{key: key, meta: &MetaData{
			Flag: DataSetFlag,
		}}, CountFlagEnabled)
		if err != nil {
			t.Fatal(err)
		}
	}

	_, err = tree.PrefixScan([]byte("name_"), limit)
	if err != nil {
		t.Error("err prefix Scan")
	}
	_, err = tree.PrefixScan([]byte("key_099"), limit)
	if err != nil {
		t.Error("err prefix Scan")
	}
}

func TestBPTree_PrefixSearchScan(t *testing.T) {

	regs := "001"
	regm := "005"
	regl := "099"

	tree = NewTree()
	_, err := tree.PrefixSearchScan([]byte("key_"), regs, 10)
	if err == nil {
		t.Fatal("err prefix search Scan")
	}
	limit := 10
	setup(t, limit)

	rgxs := regexp.MustCompile(regs)
	rgxm := regexp.MustCompile(regm)
	rgxl := regexp.MustCompile(regl)

	// prefix search scan
	rss, err := tree.PrefixSearchScan([]byte("key_"), regs, limit)
	if err != nil {
		t.Fatal(err)
	}

	// prefix search scan
	rsm, err := tree.PrefixSearchScan([]byte("key_"), regm, limit)
	if err != nil {
		t.Fatal(err)
	}

	// prefix search scan
	rsl, err := tree.PrefixSearchScan([]byte("key_"), regl, limit)
	if err != nil {
		t.Fatal(err)
	}

	for i, e := range rss {

		if !rgxs.Match(expected[i].E.Key) {
			continue
		}

		if string(expected[i].E.Key) != string(e.E.Key) {
			t.Errorf("err prefix Scan. got %v want %v", string(expected[i].E.Key), string(e.E.Key))
		}

		if string(expected[i].E.Key) == string(e.E.Key) {
			break
		}

		t.Errorf("err prefix search Scan. Regexp not found")

	}

	for i, e := range rsm {

		if !rgxm.Match(expected[i].E.Key) {
			continue
		}

		if string(expected[i].E.Key) != string(e.E.Key) {
			t.Errorf("err prefix Scan. got %v want %v", string(expected[i].E.Key), string(e.E.Key))
		}

		if string(expected[i].E.Key) == string(e.E.Key) {
			break
		}

		t.Errorf("err prefix search Scan. Regexp not found")

	}

	for i, e := range rsl {

		if !rgxl.Match(expected[i].E.Key) {
			continue
		}

		if string(expected[i].E.Key) != string(e.E.Key) {
			t.Errorf("err prefix search Scan. got %v want %v", string(expected[i].E.Key), string(e.E.Key))
		}

		if string(expected[i].E.Key) == string(e.E.Key) {
			break
		}

		t.Errorf("err prefix search Scan. Regexp not found")

	}

	for i := 1; i <= 100; i++ {
		key := []byte("name_" + fmt.Sprintf("%03d", i))
		val := []byte("val_" + fmt.Sprintf("%03d", i))
		err := tree.Insert(key, &Entry{Key: key, Value: val}, &Hint{key: key, meta: &MetaData{
			Flag: DataSetFlag,
		}}, CountFlagEnabled)
		if err != nil {
			t.Fatal(err)
		}
	}

	_, err = tree.PrefixSearchScan([]byte("name_"), "005", limit)
	if err != nil {
		t.Error("err prefix search Scan")
	}
	_, err = tree.PrefixSearchScan([]byte("key_"), "099", limit)
	if err != nil {
		t.Error("err prefix search Scan")
	}
}

func TestBPTree_All(t *testing.T) {
	tree = NewTree()
	_, err := tree.All()
	if err == nil {
		t.Fatal("err scan all")
	}
	setup(t, 100)
	rs, err := tree.All()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, rs) {
		t.Errorf("err scan all. got %v want %v", rs, expected)
	}
}

func TestBPTree_Range(t *testing.T) {
	tree = NewTree()
	_, err := tree.Range([]byte("key_001"), []byte("key_010"))
	if err == nil {
		t.Fatal("err prefix Scan")
	}

	limit := 10
	setup(t, limit)
	// range scan
	rs, err := tree.Range([]byte("key_000"), []byte("key_009"))
	if err != nil {
		t.Fatal(err)
	}

	for i, e := range rs {
		if string(expected[i].E.Key) != string(e.E.Key) {
			t.Errorf("err prefix Scan. got %v want %v", string(expected[i].E.Key), string(e.E.Key))
		}
	}

	_, err = tree.Range([]byte("key_101"), []byte("key_110"))
	if err == nil {
		t.Error("err tree.Range scan")
	}

	_, err = tree.Range([]byte("key_101"), []byte("key_100"))
	if err == nil {
		t.Error("err tree.Range scan")
	}
}

func TestBPTree_FindLeaf(t *testing.T) {
	limit := 10
	setup(t, limit)

	node := tree.FindLeaf([]byte("key_001"))
	if string(node.Keys[0]) != "key_000" {
		t.Error("err TestBPTree_FindLeaf")
	}
}

func TestIsExpired(t *testing.T) {
	record := &Record{
		H: &Hint{
			meta: &MetaData{
				timestamp: 1547707905,
				TTL:       10,
			},
		},
		E: nil,
	}
	if !record.IsExpired() {
		t.Error("err TestIsExpired")
	}

	record.H.meta.TTL = Persistent
	if record.IsExpired() {
		t.Error("err TestIsExpired")
	}
}

func TestBPTree_Update(t *testing.T) {
	limit := 10
	setup(t, limit)

	for i := 0; i <= 100; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_modify" + fmt.Sprintf("%03d", i))
		err := tree.Insert(key, &Entry{Key: key, Value: val}, &Hint{key: key, meta: &MetaData{
			Flag: DataSetFlag,
		}}, CountFlagEnabled)
		if err != nil {
			t.Fatal(err)
		}
	}

	expected = Records{}
	for i := 0; i <= limit; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_modify" + fmt.Sprintf("%03d", i))

		expected = append(expected, &Record{E: &Entry{Key: key, Value: val}, H: &Hint{key: key, meta: &MetaData{
			Flag: DataSetFlag}},
		})
	}

	rs, err := tree.Range([]byte("key_000"), []byte("key_009"))
	if err != nil {
		t.Error("err TestBPTree_Update tree.Range scan")
	}

	for i, e := range rs {
		if string(expected[i].E.Key) != string(e.E.Key) {
			t.Errorf("err prefix Scan. got %v want %v", string(expected[i].E.Key), string(e.E.Key))
		}
	}

	//delete
	for i := 1; i <= limit; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		err := tree.Insert(key, &Entry{Key: key, Value: nil}, &Hint{key: key, meta: &MetaData{
			Flag: DataDeleteFlag,
		}}, CountFlagEnabled)
		if err != nil {
			t.Fatal(err)
		}
	}

	rs, err = tree.Range([]byte("key_001"), []byte("key_010"))

	if err != nil {
		t.Fatal(err)
	}

	for _, v := range rs {
		if v.H.meta.Flag != DataDeleteFlag {
			t.Error("err TestBPTree_Update")
		}
	}

	key := []byte("key_001")
	err = tree.Insert(key, &Entry{Key: key, Value: nil}, &Hint{key: key, meta: &MetaData{
		Flag: DataSetFlag,
	}}, CountFlagEnabled)

	if err != nil {
		t.Fatal(err)
	}

	r, err := tree.Find(key)
	if err != nil {
		t.Fatal(err)
	}

	if r.H.meta.Flag == DataDeleteFlag {
		t.Error("err TestBPTree_Update")
	}
}
