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
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"sort"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/store"
)

// pendingKV is an uncommitted put or delete staged in a writable Tx.
type pendingKV struct {
	deleted bool
	value   []byte
	ttl     uint32
}

// Tx is a database transaction. Obtain via DB.Update or DB.View.
// Do not use a Tx after the Update/View callback returns.
//
// Tx 表示数据库事务，通过 DB.Update 或 DB.View 获取。
// Update/View 的回调返回后不得再使用该 Tx。
type Tx struct {
	db       *DB
	writable bool
	closed   bool
	ctx      context.Context

	pending       map[string]*pendingKV
	bucketCreates map[bucketKey]uint64
	bucketDeletes map[bucketKey]struct{}
	deletedIDs    []uint64 // committed bucket ids deleted in this tx (for key GC)
}

// NewKVBucket creates a KV (BTree) bucket. Equivalent to
// NewBucket(DataStructureBTree, name).
//
// NewKVBucket 创建 KV（BTree）bucket，等价于 NewBucket(DataStructureBTree, name)。
func (tx *Tx) NewKVBucket(name string) error {
	return tx.NewBucket(DataStructureBTree, name)
}

// NewBucket creates a bucket of the given data structure type.
// Only DataStructureBTree is supported in this version; others return ErrNotSupported.
//
// NewBucket 按数据结构类型创建 bucket。
// 本版仅支持 DataStructureBTree；其它类型返回 ErrNotSupported。
func (tx *Tx) NewBucket(ds uint16, name string) error {
	if err := tx.check(); err != nil {
		return err
	}
	if !tx.writable {
		return ErrTxNotWritable
	}
	if ds != DataStructureBTree {
		return ErrNotSupported
	}
	if name == "" {
		return ErrBucketNameEmpty
	}
	k := bucketKey{ds: ds, name: name}
	if _, ok := tx.bucketDeletes[k]; ok {
		delete(tx.bucketDeletes, k)
		// fall through to recreate with a new id
	} else if tx.bucketExists(ds, name) {
		return ErrBucketAlreadyExist
	}
	id := tx.db.buckets.allocID()
	tx.bucketCreates[k] = id
	return nil
}

// DeleteBucket deletes a bucket of the given data structure type.
//
// DeleteBucket 删除指定数据结构类型的 bucket。
func (tx *Tx) DeleteBucket(ds uint16, bucket string) error {
	if err := tx.check(); err != nil {
		return err
	}
	if !tx.writable {
		return ErrTxNotWritable
	}
	if bucket == "" {
		return ErrBucketNameEmpty
	}
	k := bucketKey{ds: ds, name: bucket}
	if id, ok := tx.bucketCreates[k]; ok {
		delete(tx.bucketCreates, k)
		tx.purgePendingForBucket(id)
		return nil
	}
	id, ok := tx.db.buckets.getID(ds, bucket)
	if !ok {
		return ErrBucketNotFound
	}
	if _, deleted := tx.bucketDeletes[k]; deleted {
		return ErrBucketNotFound
	}
	tx.bucketDeletes[k] = struct{}{}
	tx.deletedIDs = append(tx.deletedIDs, id)
	tx.purgePendingForBucket(id)
	return nil
}

// ExistBucket reports whether the bucket exists.
//
// ExistBucket 判断 bucket 是否存在。
func (tx *Tx) ExistBucket(ds uint16, bucket string) bool {
	if tx == nil || tx.closed {
		return false
	}
	return tx.bucketExists(ds, bucket)
}

// IterateBuckets calls f for each bucket name matching pattern under ds.
// If f returns false, iteration stops. First cut supports path.Match patterns
// (e.g. "*").
//
// IterateBuckets 对 ds 下匹配 pattern 的每个 bucket 名调用 f。
// f 返回 false 时停止。首版支持 path.Match（如 "*"）。
func (tx *Tx) IterateBuckets(ds uint16, pattern string, f func(bucket string) bool) error {
	if err := tx.check(); err != nil {
		return err
	}
	if f == nil {
		return nil
	}
	if pattern == "" {
		pattern = "*"
	}

	names := make(map[string]struct{})
	for _, name := range tx.db.buckets.list(ds) {
		k := bucketKey{ds: ds, name: name}
		if _, del := tx.bucketDeletes[k]; del {
			continue
		}
		names[name] = struct{}{}
	}
	for k := range tx.bucketCreates {
		if k.ds == ds {
			names[k.name] = struct{}{}
		}
	}

	list := make([]string, 0, len(names))
	for name := range names {
		ok, err := path.Match(pattern, name)
		if err != nil {
			return fmt.Errorf("bucket pattern: %w", err)
		}
		if ok {
			list = append(list, name)
		}
	}
	sort.Strings(list)
	for _, name := range list {
		if !f(name) {
			break
		}
	}
	return nil
}

// GetBucket returns a handle bound to this Tx, ds, and name (always non-nil).
// If the bucket was not created, subsequent Put/Get/etc. return ErrBucketNotFound.
// Non-BTree ds returns ErrNotSupported from KV methods in this version.
//
// GetBucket 返回绑定到本 Tx、ds 与名称的句柄（始终非 nil）。
// 未创建时后续 Put/Get 等返回 ErrBucketNotFound；
// 本版非 BTree 的 ds 上调用 KV 方法返回 ErrNotSupported。
func (tx *Tx) GetBucket(ds uint16, name string) *Bucket {
	return &Bucket{tx: tx, ds: ds, name: name}
}

func (tx *Tx) check() error {
	if tx == nil || tx.closed || tx.db == nil {
		return ErrTxClosed
	}
	if err := tx.db.checkOpen(); err != nil {
		return ErrTxClosed
	}
	return nil
}

func (tx *Tx) markClosed() {
	tx.closed = true
}

func (tx *Tx) bucketExists(ds uint16, name string) bool {
	k := bucketKey{ds: ds, name: name}
	if _, ok := tx.bucketCreates[k]; ok {
		return true
	}
	if _, del := tx.bucketDeletes[k]; del {
		return false
	}
	return tx.db.buckets.exists(ds, name)
}

func (tx *Tx) resolveBucketID(ds uint16, name string) (uint64, error) {
	k := bucketKey{ds: ds, name: name}
	if id, ok := tx.bucketCreates[k]; ok {
		return id, nil
	}
	if _, del := tx.bucketDeletes[k]; del {
		return 0, ErrBucketNotFound
	}
	id, ok := tx.db.buckets.getID(ds, name)
	if !ok {
		return 0, ErrBucketNotFound
	}
	return id, nil
}

func (tx *Tx) purgePendingForBucket(id uint64) {
	if tx.pending == nil {
		return
	}
	prefix := encodeBucketPrefix(id)
	for ek := range tx.pending {
		if bytes.HasPrefix([]byte(ek), prefix) {
			delete(tx.pending, ek)
		}
	}
}

func (tx *Tx) commit(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// 1) Apply KV pending to the engine.
	if len(tx.pending) > 0 {
		puts := make([]struct {
			Key   []byte
			Value *core.Record
		}, 0)
		dels := make([][]byte, 0)
		for ek, e := range tx.pending {
			key := []byte(ek)
			if e.deleted {
				dels = append(dels, key)
				continue
			}
			rec := core.NewRecord().WithValue(append([]byte(nil), e.value...)).WithTTL(e.ttl)
			puts = append(puts, struct {
				Key   []byte
				Value *core.Record
			}{Key: key, Value: rec})
		}
		if len(puts) > 0 {
			if err := tx.db.store.BatchPut(ctx, puts); err != nil {
				return fmt.Errorf("commit puts: %w", err)
			}
		}
		if len(dels) > 0 {
			if err := tx.db.store.BatchDelete(ctx, dels); err != nil {
				return fmt.Errorf("commit deletes: %w", err)
			}
		}
	}

	// 2) GC keys for deleted buckets.
	for _, id := range tx.deletedIDs {
		if err := tx.deleteBucketKeys(ctx, id); err != nil {
			return err
		}
	}

	// 3) Persist bucket meta (creates/deletes).
	if len(tx.bucketCreates) > 0 || len(tx.bucketDeletes) > 0 {
		if err := tx.db.buckets.applyCommit(tx.bucketCreates, tx.bucketDeletes); err != nil {
			return fmt.Errorf("commit bucket meta: %w", err)
		}
	}
	return nil
}

func (tx *Tx) deleteBucketKeys(ctx context.Context, id uint64) error {
	prefix := encodeBucketPrefix(id)
	keys := make([][]byte, 0)
	err := tx.db.store.Iterate(ctx, func(key []byte, _ *core.Record) bool {
		if bytes.HasPrefix(key, prefix) {
			keys = append(keys, append([]byte(nil), key...))
		}
		return true
	})
	if err != nil {
		return fmt.Errorf("iterate bucket keys: %w", err)
	}
	if len(keys) == 0 {
		return nil
	}
	if err := tx.db.store.BatchDelete(ctx, keys); err != nil {
		return fmt.Errorf("delete bucket keys: %w", err)
	}
	return nil
}

// getPending looks up a pending entry by engine key string.
func (tx *Tx) getPending(engineKey []byte) (*pendingKV, bool) {
	if tx.pending == nil {
		return nil, false
	}
	e, ok := tx.pending[string(engineKey)]
	return e, ok
}

func (tx *Tx) setPending(engineKey []byte, e *pendingKV) {
	if tx.pending == nil {
		tx.pending = make(map[string]*pendingKV)
	}
	tx.pending[string(engineKey)] = e
}

func mapStoreErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, store.ErrKeyNotFound) {
		return ErrKeyNotFound
	}
	return err
}
