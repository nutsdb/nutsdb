// Copyright 2026 The nutsdb Author. All rights reserved.
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

package store_test

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/fileio"
	"github.com/nutsdb/nutsdb/internal/store"
)

// exampleOpts returns durable options suitable for short examples.
func exampleOpts(dir string) store.DiskStoreOptions {
	opts := store.DefaultDiskStoreOptions(dir)
	opts.SyncMode = fileio.SyncEveryWrite
	return opts
}

func mustOpen(dir string) store.StoreManager {
	st, err := store.OpenStoreManager(exampleOpts(dir))
	if err != nil {
		log.Fatal(err)
	}
	return st
}

// ExampleOpenStoreManager shows how to open a StoreManager on a data directory.
func ExampleOpenStoreManager() {
	dir, err := os.MkdirTemp("", "nutsdb-storemgr-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	st, err := store.OpenStoreManager(store.DefaultDiskStoreOptions(dir))
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()

	fmt.Println("opened")
	// Output:
	// opened
}

// ExampleOpenDiskStore shows the compatibility alias of OpenStoreManager.
func ExampleOpenDiskStore() {
	dir, err := os.MkdirTemp("", "nutsdb-diskstore-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	st, err := store.OpenDiskStore(store.DefaultDiskStoreOptions(dir))
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()

	fmt.Println("opened")
	// Output:
	// opened
}

// ExampleStoreManager_Put demonstrates inserting or updating a key.
func ExampleStoreManager_Put() {
	dir, err := os.MkdirTemp("", "nutsdb-put-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	st := mustOpen(dir)
	defer st.Close()
	ctx := context.Background()

	rec := core.NewRecord().WithValue([]byte("hello"))
	if err := st.Put(ctx, []byte("name"), rec); err != nil {
		log.Fatal(err)
	}
	fmt.Println("put ok")
	// Output:
	// put ok
}

// ExampleStoreManager_Get demonstrates reading a key.
func ExampleStoreManager_Get() {
	dir, err := os.MkdirTemp("", "nutsdb-get-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	st := mustOpen(dir)
	defer st.Close()
	ctx := context.Background()

	_ = st.Put(ctx, []byte("name"), core.NewRecord().WithValue([]byte("alice")))

	rec, err := st.Get(ctx, []byte("name"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s=%s\n", rec.Key, rec.Value)
	// Output:
	// name=alice
}

// ExampleStoreManager_Get_notFound shows ErrKeyNotFound when the key is missing.
func ExampleStoreManager_Get_notFound() {
	dir, err := os.MkdirTemp("", "nutsdb-get-miss-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	st := mustOpen(dir)
	defer st.Close()

	_, err = st.Get(context.Background(), []byte("missing"))
	fmt.Println(err == store.ErrKeyNotFound)
	// Output:
	// true
}

// ExampleStoreManager_Delete demonstrates deleting a key.
func ExampleStoreManager_Delete() {
	dir, err := os.MkdirTemp("", "nutsdb-delete-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	st := mustOpen(dir)
	defer st.Close()
	ctx := context.Background()

	_ = st.Put(ctx, []byte("tmp"), core.NewRecord().WithValue([]byte("1")))
	if err := st.Delete(ctx, []byte("tmp")); err != nil {
		log.Fatal(err)
	}
	_, err = st.Get(ctx, []byte("tmp"))
	fmt.Println(err == store.ErrKeyNotFound)
	// Output:
	// true
}

// ExampleStoreManager_Iterate walks all keys in ascending order.
func ExampleStoreManager_Iterate() {
	dir, err := os.MkdirTemp("", "nutsdb-iterate-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	st := mustOpen(dir)
	defer st.Close()
	ctx := context.Background()

	_ = st.Put(ctx, []byte("c"), core.NewRecord().WithValue([]byte("3")))
	_ = st.Put(ctx, []byte("a"), core.NewRecord().WithValue([]byte("1")))
	_ = st.Put(ctx, []byte("b"), core.NewRecord().WithValue([]byte("2")))

	err = st.Iterate(ctx, func(key []byte, value *core.Record) bool {
		fmt.Printf("%s=%s\n", key, value.Value)
		return true // return false to stop early
	})
	if err != nil {
		log.Fatal(err)
	}
	// Output:
	// a=1
	// b=2
	// c=3
}

// ExampleStoreManager_Close shows flushing and releasing resources.
func ExampleStoreManager_Close() {
	dir, err := os.MkdirTemp("", "nutsdb-close-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	st := mustOpen(dir)
	_ = st.Put(context.Background(), []byte("k"), core.NewRecord().WithValue([]byte("v")))
	if err := st.Close(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("closed")
	// Output:
	// closed
}

// ExampleStoreManager_BatchPut writes multiple keys in one batch (group commit).
func ExampleStoreManager_BatchPut() {
	dir, err := os.MkdirTemp("", "nutsdb-batchput-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	st := mustOpen(dir)
	defer st.Close()

	err = st.BatchPut(context.Background(), []struct {
		Key   []byte
		Value *core.Record
	}{
		{Key: []byte("u1"), Value: core.NewRecord().WithValue([]byte("a"))},
		{Key: []byte("u2"), Value: core.NewRecord().WithValue([]byte("b"))},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("batch put ok")
	// Output:
	// batch put ok
}

// ExampleStoreManager_BatchGet reads multiple keys; missing keys have Value == nil.
func ExampleStoreManager_BatchGet() {
	dir, err := os.MkdirTemp("", "nutsdb-batchget-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	st := mustOpen(dir)
	defer st.Close()
	ctx := context.Background()

	_ = st.Put(ctx, []byte("u1"), core.NewRecord().WithValue([]byte("a")))
	_ = st.Put(ctx, []byte("u2"), core.NewRecord().WithValue([]byte("b")))

	rows, err := st.BatchGet(ctx, [][]byte{
		[]byte("u1"),
		[]byte("missing"),
		[]byte("u2"),
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range rows {
		if row.Value == nil {
			fmt.Printf("%s=<nil>\n", row.Key)
			continue
		}
		fmt.Printf("%s=%s\n", row.Key, row.Value.Value)
	}
	// Output:
	// u1=a
	// missing=<nil>
	// u2=b
}

// ExampleStoreManager_BatchDelete deletes multiple keys in one batch.
func ExampleStoreManager_BatchDelete() {
	dir, err := os.MkdirTemp("", "nutsdb-batchdelete-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	st := mustOpen(dir)
	defer st.Close()
	ctx := context.Background()

	_ = st.Put(ctx, []byte("x"), core.NewRecord().WithValue([]byte("1")))
	_ = st.Put(ctx, []byte("y"), core.NewRecord().WithValue([]byte("2")))

	if err := st.BatchDelete(ctx, [][]byte{[]byte("x"), []byte("y")}); err != nil {
		log.Fatal(err)
	}
	_, err = st.Get(ctx, []byte("x"))
	fmt.Println(err == store.ErrKeyNotFound)
	// Output:
	// true
}

// ExampleStoreManager_reopen shows the simplest durable workflow:
// put a value, close the store, reopen the same directory, then get it back.
func ExampleStoreManager_reopen() {
	dir, err := os.MkdirTemp("", "nutsdb-reopen-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := store.DefaultDiskStoreOptions(dir)
	opts.SyncMode = fileio.SyncEveryWrite
	ctx := context.Background()

	st1, err := store.OpenStoreManager(opts)
	if err != nil {
		log.Fatal(err)
	}
	if err := st1.Put(ctx, []byte("greeting"), core.NewRecord().WithValue([]byte("hello"))); err != nil {
		log.Fatal(err)
	}
	if err := st1.Close(); err != nil {
		log.Fatal(err)
	}

	st2, err := store.OpenStoreManager(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer st2.Close()

	rec, err := st2.Get(ctx, []byte("greeting"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", rec.Value)
	// Output:
	// hello
}

// ExampleStoreManager demonstrates a minimal end-to-end KV workflow.
func ExampleStoreManager() {
	dir, err := os.MkdirTemp("", "nutsdb-workflow-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := store.DefaultDiskStoreOptions(dir)
	opts.SyncMode = fileio.SyncEveryWrite

	st, err := store.OpenStoreManager(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()

	ctx := context.Background()

	// Write
	if err := st.Put(ctx, []byte("user:1"), core.NewRecord().WithValue([]byte(`{"name":"bob"}`))); err != nil {
		log.Fatal(err)
	}

	// Read
	rec, err := st.Get(ctx, []byte("user:1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("get %s\n", rec.Value)

	// Update
	if err := st.Put(ctx, []byte("user:1"), core.NewRecord().WithValue([]byte(`{"name":"bob2"}`))); err != nil {
		log.Fatal(err)
	}

	// Batch
	_ = st.BatchPut(ctx, []struct {
		Key   []byte
		Value *core.Record
	}{
		{Key: []byte("user:2"), Value: core.NewRecord().WithValue([]byte("x"))},
	})

	// Iterate (sorted by key)
	_ = st.Iterate(ctx, func(key []byte, value *core.Record) bool {
		fmt.Printf("iter %s\n", key)
		return true
	})

	// Delete
	_ = st.Delete(ctx, []byte("user:2"))

	fmt.Println("done")
	// Output:
	// get {"name":"bob"}
	// iter user:1
	// iter user:2
	// done
}
