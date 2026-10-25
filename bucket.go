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

import "errors"

// Bucket is a transaction-scoped handle for a named bucket of a given data structure.
// Obtain via Tx.GetBucket. Do not use after the enclosing Update/View returns.
//
// Bucket 是事务内、绑定名称与数据结构类型的句柄。
// 通过 Tx.GetBucket 获取；外层 Update/View 返回后不得再使用。
type Bucket struct {
	tx   *Tx
	ds   uint16
	name string
}

// Name returns the bucket name.
// Name 返回 bucket 名称。
func (b *Bucket) Name() string {
	if b == nil {
		return ""
	}
	return b.name
}

// DS returns the data structure type of this bucket handle.
// DS 返回该句柄的数据结构类型。
func (b *Bucket) DS() uint16 {
	if b == nil {
		return 0
	}
	return b.ds
}

// Put stores a key/value. ttl is in seconds; 0 means never expire.
//
// Put 写入 key/value。ttl 单位为秒；0 表示永不过期。
func (b *Bucket) Put(key, value []byte, ttl uint32) error {
	if err := b.checkWrite(); err != nil {
		return err
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	id, err := b.tx.resolveBucketID(b.ds, b.name)
	if err != nil {
		return err
	}
	ek := encodeEngineKey(id, key)
	b.tx.setPending(ek, &pendingKV{
		deleted: false,
		value:   append([]byte(nil), value...),
		ttl:     ttl,
	})
	return nil
}

// Get returns the value for key, or ErrKeyNotFound.
//
// Get 返回 key 对应的 value；不存在则返回 ErrKeyNotFound。
func (b *Bucket) Get(key []byte) ([]byte, error) {
	if err := b.checkRead(); err != nil {
		return nil, err
	}
	if len(key) == 0 {
		return nil, ErrKeyEmpty
	}
	id, err := b.tx.resolveBucketID(b.ds, b.name)
	if err != nil {
		return nil, err
	}
	ek := encodeEngineKey(id, key)

	if e, ok := b.tx.getPending(ek); ok {
		if e.deleted {
			return nil, ErrKeyNotFound
		}
		return append([]byte(nil), e.value...), nil
	}

	ctx := b.tx.ctx
	if ctx == nil {
		return nil, ErrTxClosed
	}
	rec, err := b.tx.db.store.Get(ctx, ek)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	return append([]byte(nil), rec.Value...), nil
}

// Delete removes the key (tombstone).
//
// Delete 删除 key（写入 tombstone）。
func (b *Bucket) Delete(key []byte) error {
	if err := b.checkWrite(); err != nil {
		return err
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	id, err := b.tx.resolveBucketID(b.ds, b.name)
	if err != nil {
		return err
	}
	ek := encodeEngineKey(id, key)
	b.tx.setPending(ek, &pendingKV{deleted: true})
	return nil
}

// Has reports whether key exists.
//
// Has 判断 key 是否存在。
func (b *Bucket) Has(key []byte) (bool, error) {
	_, err := b.Get(key)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, ErrKeyNotFound) {
		return false, nil
	}
	return false, err
}

func (b *Bucket) checkRead() error {
	if b == nil || b.tx == nil {
		return ErrTxClosed
	}
	if err := b.tx.check(); err != nil {
		return err
	}
	if b.ds != DataStructureBTree {
		return ErrNotSupported
	}
	return nil
}

func (b *Bucket) checkWrite() error {
	if err := b.checkRead(); err != nil {
		return err
	}
	if !b.tx.writable {
		return ErrTxNotWritable
	}
	return nil
}
