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

func exampleOpts(dir string) store.LSMOptions {
	opts := store.DefaultLSMOptions(dir)
	// Durable writes so Close → Reopen can read everything back.
	opts.WALSyncMode = fileio.SyncEveryWrite
	opts.ValueLog.SyncMode = fileio.SyncEveryWrite
	return opts
}

// ExampleStoreManager shows Open / Put / Get / Batch* / Iterate / Delete /
// Close, then reopen the same directory and read persisted data.
func ExampleStoreManager() {
	dir, err := os.MkdirTemp("", "nutsdb-store-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()
	opts := exampleOpts(dir)

	st, err := store.OpenStoreManager(opts)
	if err != nil {
		log.Fatal(err)
	}

	// Put + Get
	if err := st.Put(ctx, []byte("user:1"), core.NewRecord().WithValue([]byte(`{"name":"alice"}`))); err != nil {
		log.Fatal(err)
	}
	rec, err := st.Get(ctx, []byte("user:1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("get %s\n", rec.Value)

	// BatchPut + BatchGet
	if err := st.BatchPut(ctx, []struct {
		Key   []byte
		Value *core.Record
	}{
		{Key: []byte("user:2"), Value: core.NewRecord().WithValue([]byte(`{"name":"bob"}`))},
		{Key: []byte("user:3"), Value: core.NewRecord().WithValue([]byte(`{"name":"carol"}`))},
	}); err != nil {
		log.Fatal(err)
	}
	batch, err := st.BatchGet(ctx, [][]byte{[]byte("user:2"), []byte("missing"), []byte("user:3")})
	if err != nil {
		log.Fatal(err)
	}
	for _, item := range batch {
		if item.Value == nil {
			fmt.Printf("batch %s <nil>\n", item.Key)
			continue
		}
		fmt.Printf("batch %s %s\n", item.Key, item.Value.Value)
	}

	// Iterate (sorted by key)
	if err := st.Iterate(ctx, func(key []byte, value *core.Record) bool {
		fmt.Printf("iter %s\n", key)
		return true
	}); err != nil {
		log.Fatal(err)
	}

	// Delete + BatchDelete
	if err := st.Delete(ctx, []byte("user:3")); err != nil {
		log.Fatal(err)
	}
	if err := st.BatchDelete(ctx, [][]byte{[]byte("user:2")}); err != nil {
		log.Fatal(err)
	}

	if err := st.Close(); err != nil {
		log.Fatal(err)
	}

	// Reopen the same directory and read what was persisted.
	st2, err := store.OpenStoreManager(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer st2.Close()

	rec, err = st2.Get(ctx, []byte("user:1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("reopen %s\n", rec.Value)

	_, err = st2.Get(ctx, []byte("user:2"))
	if err == store.ErrKeyNotFound {
		fmt.Println("reopen user:2 gone")
	}

	// Output:
	// get {"name":"alice"}
	// batch user:2 {"name":"bob"}
	// batch missing <nil>
	// batch user:3 {"name":"carol"}
	// iter user:1
	// iter user:2
	// iter user:3
	// reopen {"name":"alice"}
	// reopen user:2 gone
}
