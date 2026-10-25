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

package nutsdb

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/nutsdb/nutsdb/internal/store"
)

// DB is an opened NutsDB database handle.
// DB 表示已打开的 NutsDB 数据库句柄。
type DB struct {
	opts    Options
	store   store.StoreManager
	buckets *bucketManager

	writeMu sync.Mutex // single writer (Update)
	mu      sync.RWMutex
	closed  bool
}

// Open opens or creates a database with the given options.
//
// Open 使用给定选项打开或创建数据库。
func Open(options Options, ops ...Option) (*DB, error) {
	opts := applyOptions(options, ops...)
	if opts.Dir == "" {
		return nil, ErrDirRequired
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir data dir: %w", err)
	}

	st, err := store.OpenStoreManager(store.DefaultLSMOptions(opts.Dir))
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}

	bm := newBucketManager(opts.Dir)
	if err := bm.load(); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("load buckets meta: %w", err)
	}

	return &DB{
		opts:    opts,
		store:   st,
		buckets: bm,
	}, nil
}

// Update executes fn inside a read-write transaction.
// ctx covers lock wait, fn, and commit; fn receives the same (or a derived) ctx.
//
// Update 在读写事务中执行 fn。
// ctx 贯穿等锁、fn 与 Commit；fn 内收到同一（或派生）ctx。
func (db *DB) Update(ctx context.Context, fn func(ctx context.Context, tx *Tx) error) error {
	if err := db.checkOpen(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if fn == nil {
		return nil
	}

	if err := db.lockWrite(ctx); err != nil {
		return err
	}
	defer db.writeMu.Unlock()

	if err := db.checkOpen(); err != nil {
		return err
	}

	tx := &Tx{
		db:            db,
		writable:      true,
		ctx:           ctx,
		pending:       make(map[string]*pendingKV),
		bucketCreates: make(map[bucketKey]uint64),
		bucketDeletes: make(map[bucketKey]struct{}),
	}
	defer tx.markClosed()

	if err := fn(ctx, tx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return tx.commit(ctx)
}

// View executes fn inside a read-only transaction.
// Write methods on Tx/Bucket return ErrTxNotWritable.
//
// View 在只读事务中执行 fn。
// Tx/Bucket 上的写操作将返回 ErrTxNotWritable。
func (db *DB) View(ctx context.Context, fn func(ctx context.Context, tx *Tx) error) error {
	if err := db.checkOpen(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if fn == nil {
		return nil
	}

	tx := &Tx{
		db:       db,
		writable: false,
		ctx:      ctx,
	}
	defer tx.markClosed()
	return fn(ctx, tx)
}

// Close closes the database. ctx may cancel before the close starts.
//
// Close 关闭数据库。ctx 可在关闭开始前取消。
func (db *DB) Close(ctx context.Context) error {
	if db == nil {
		return ErrDBClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDBClosed
	}
	db.closed = true
	st := db.store
	db.mu.Unlock()

	// Wait for any in-flight Update to finish.
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	if st != nil {
		if err := st.Close(); err != nil {
			return fmt.Errorf("close store: %w", err)
		}
	}
	return nil
}

func (db *DB) checkOpen() error {
	if db == nil {
		return ErrDBClosed
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrDBClosed
	}
	return nil
}

func (db *DB) lockWrite(ctx context.Context) error {
	locked := make(chan struct{})
	go func() {
		db.writeMu.Lock()
		close(locked)
	}()
	select {
	case <-ctx.Done():
		go func() {
			<-locked
			db.writeMu.Unlock()
		}()
		return ctx.Err()
	case <-locked:
		return nil
	}
}
