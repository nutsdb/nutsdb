// Copyright 2021 The nutsdb Author. All rights reserved.
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

package inmemory

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

var (
	testDB *DB
)

type testMap struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func NewTestMap() *testMap {
	return &testMap{data: make(map[string][]byte)}
}

func (m *testMap) put(key string, value []byte) {
	m.mu.Lock()
	m.data[key] = value
	m.mu.Unlock()
}

func (m *testMap) get(key string) []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if val, ok := m.data[key]; ok {
		return val
	}
	return nil
}

func Benchmark_TestMap_Get(b *testing.B) {
	m := NewTestMap()

	key := "key1"
	value := bytes.Repeat([]byte("a"), 1024)
	m.put(key, value)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.get(key)
	}
}
func Benchmark_TestMap_Get_RunParallel(b *testing.B) {
	m := NewTestMap()

	key := "key1"
	value := bytes.Repeat([]byte("a"), 1024)
	m.put(key, value)

	b.RunParallel(func(pb *testing.PB) {
		b.ReportAllocs()
		for pb.Next() {
			val := m.get(key)
			if bytes.Compare(val, value) != 0 {
				b.Error("err testDB Get")
			}
		}
	})
}

func Benchmark_TestMap_Put(b *testing.B) {
	m := NewTestMap()

	values := bytes.Repeat([]byte("a"), 1024)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := []byte(fmt.Sprintf("val%s-%d", values, i))
		m.put(key, val)
	}
}

func BenchmarkDB_TestMap_Put_RunParallel(b *testing.B) {
	m := NewTestMap()

	rand.Seed(time.Now().Unix())

	b.RunParallel(func(pb *testing.PB) {
		id := rand.Int()

		b.ReportAllocs()
		for pb.Next() {
			key := fmt.Sprintf("key-%d", id)
			value := []byte(fmt.Sprintf("value-%d", id))
			m.put(key, value)
		}
	})

}

func BenchmarkShardDB_Get(b *testing.B) {
	opts := DefaultOptions
	bucket := "bucket1"
	key := []byte("key1")
	value := bytes.Repeat([]byte("a"), 1024)

	testDB, _ = Open(opts)
	err := testDB.Put(bucket, key, value, 0)
	if err != nil {
		log.Fatal(err)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		entry, err := testDB.Get(bucket, key)
		if err != nil {
			log.Fatal(err)
		}
		if bytes.Compare(entry.Value, value) != 0 {
			b.Error("err testDB Get")
		}
	}
}

func BenchmarkShardDB_GET_RunParallel(b *testing.B) {
	bucket := "bucket1"
	key := []byte("key1")
	value := bytes.Repeat([]byte("a"), 1024)

	opts := DefaultOptions
	testDB, _ = Open(opts)
	err := testDB.Put(bucket, key, value, 0)
	if err != nil {
		log.Fatal(err)
	}

	b.RunParallel(func(pb *testing.PB) {
		b.ReportAllocs()
		for pb.Next() {
			entry, err := testDB.Get(bucket, key)
			if err != nil {
				log.Fatal(err)
			}
			if bytes.Compare(entry.Value, value) != 0 {
				b.Error("err testDB Get")
			}
		}
	})
}

func BenchmarkDB_Put(b *testing.B) {
	values := bytes.Repeat([]byte("a"), 1024)
	opts := DefaultOptions
	testDB, _ = Open(opts)

	bucket := "bucket1"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("val%s-%d", values, i))
		err := testDB.Put(bucket, key, val, 0)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkShardDB_Put_OneBucket_RunParallel(b *testing.B) {
	opts := DefaultOptions
	opts.ShardsCount = 1024
	testDB, _ = Open(opts)

	bucket := "bucket1"

	rand.Seed(time.Now().UnixNano())

	b.RunParallel(func(pb *testing.PB) {
		id := rand.Int()

		b.ReportAllocs()
		for pb.Next() {
			key := []byte(fmt.Sprintf("key-%d", id))
			value := []byte(fmt.Sprintf("value-%d", id))
			err := testDB.Put(bucket, key, value, 0)
			if err != nil {
				log.Fatal(err)
			}
		}
	})
}

func BenchmarkShardDB_Put_MultiBuckets_RunParallel(b *testing.B) {
	opts := DefaultOptions
	opts.ShardsCount = 1024
	testDB, _ = Open(opts)

	rand.Seed(time.Now().UnixNano())

	b.RunParallel(func(pb *testing.PB) {
		id := rand.Int()

		b.ReportAllocs()
		for pb.Next() {
			key := []byte(fmt.Sprintf("key-%d", id))
			value := []byte(fmt.Sprintf("value-%d", id))
			num := rand.Intn(100)
			bucket := "bucket" + strconv.Itoa(num)
			err := testDB.Put(bucket, key, value, 0)
			if err != nil {
				log.Fatal(err)
			}
		}
	})
}
