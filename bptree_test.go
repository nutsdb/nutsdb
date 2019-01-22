package nutsdb

import (
	"fmt"
	"reflect"
	"testing"
)

var (
	tree     *BPTree
	expected Records
)

func setup(t *testing.T, limit int) {
	tree = NewTree()
	for i := 1; i <= 100; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_" + fmt.Sprintf("%03d", i))
		err := tree.Insert(key, &Entry{Key: key, Value: val}, &Hint{key: key, meta: &MetaData{
			Flag: DataSetFlag,
		}}, CountFlagEnabled)
		if err != nil {
			t.Fatal(err)
		}
	}

	expected = make(Records, limit)
	for i := 1; i <= limit; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_" + fmt.Sprintf("%03d", i))
		expected[string(key)] = &Record{E: &Entry{Key: key, Value: val}, H: &Hint{key: key, meta: &MetaData{
			Flag: DataSetFlag,
		}}}
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
	rs, err := tree.PrefixScan([]byte("key_001"), 10)
	if err == nil {
		t.Fatal("err prefix Scan")
	}
	limit := 10
	setup(t, limit)

	// prefix scan
	rs, err = tree.PrefixScan([]byte("key_"), limit)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expected, rs) {
		t.Errorf("err prefix Scan. got %v want %v", rs, expected)
	}

	rs, err = tree.PrefixScan([]byte("key_xx"), limit)
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

	rs, err = tree.PrefixScan([]byte("name_"), limit)
	if err != nil {
		t.Error("err prefix Scan")
	}
	rs, err = tree.PrefixScan([]byte("key_100"), limit)
	if err != nil {
		t.Error("err prefix Scan")
	}
}

func TestBPTree_Range(t *testing.T) {
	tree = NewTree()
	rs, err := tree.Range([]byte("key_001"), []byte("key_010"))
	if err == nil {
		t.Fatal("err prefix Scan")
	}

	limit := 10
	setup(t, limit)
	// range scan
	rs, err = tree.Range([]byte("key_001"), []byte("key_010"))
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expected, rs) {
		t.Errorf("err range Scan. got %v want %v", rs, expected)
	}

	rs, err = tree.Range([]byte("key_101"), []byte("key_110"))
	if err == nil {
		t.Error("err tree.Range scan")
	}

	rs, err = tree.Range([]byte("key_101"), []byte("key_100"))
	if err == nil {
		t.Error("err tree.Range scan")
	}
}

func TestBPTree_FindLeaf(t *testing.T) {
	limit := 10
	setup(t, limit)

	node := tree.FindLeaf([]byte("key_001"))
	if string(node.Keys[0]) != "key_001" {
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
	if !record.isExpired() {
		t.Error("err TestIsExpired")
	}

	record.H.meta.TTL = Persistent
	if record.isExpired() {
		t.Error("err TestIsExpired")
	}
}

func TestBPTree_Update(t *testing.T) {
	limit := 10
	setup(t, limit)

	for i := 1; i <= 100; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_modify" + fmt.Sprintf("%03d", i))
		err := tree.Insert(key, &Entry{Key: key, Value: val}, &Hint{key: key, meta: &MetaData{
			Flag: DataSetFlag,
		}}, CountFlagEnabled)
		if err != nil {
			t.Fatal(err)
		}
	}

	expected = make(Records, limit)
	for i := 1; i <= limit; i++ {
		key := []byte("key_" + fmt.Sprintf("%03d", i))
		val := []byte("val_modify" + fmt.Sprintf("%03d", i))
		expected[string(key)] = &Record{E: &Entry{Key: key, Value: val}, H: &Hint{key: key, meta: &MetaData{
			Flag: DataSetFlag}},
		}
	}

	rs, err := tree.Range([]byte("key_001"), []byte("key_010"))
	if err != nil {
		t.Error("err TestBPTree_Update tree.Range scan")
	}

	if !reflect.DeepEqual(expected, rs) {
		t.Errorf("err TestBPTree_Update range Scan. got %v want %v", rs, expected)
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
