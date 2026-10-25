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

var (
	// ErrNotImplemented indicates the API surface exists but is not wired yet.
	// ErrNotImplemented 表示 API 已导出但尚未实现。
	ErrNotImplemented = errors.New("nutsdb: not implemented")

	// ErrDBClosed is returned when operating on a closed DB.
	// ErrDBClosed 表示数据库已关闭。
	ErrDBClosed = errors.New("nutsdb: db closed")

	// ErrTxClosed is returned when operating on a closed transaction.
	// ErrTxClosed 表示事务已关闭。
	ErrTxClosed = errors.New("nutsdb: tx closed")

	// ErrTxNotWritable is returned when a write is attempted in a read-only Tx.
	// ErrTxNotWritable 表示在只读事务中执行了写操作。
	ErrTxNotWritable = errors.New("nutsdb: tx not writable")

	// ErrBucketNotFound is returned when the bucket does not exist.
	// ErrBucketNotFound 表示 bucket 不存在。
	ErrBucketNotFound = errors.New("nutsdb: bucket not found")

	// ErrNotFoundBucket is an alias of ErrBucketNotFound for v1 migration.
	// ErrNotFoundBucket 是 ErrBucketNotFound 的 v1 兼容别名。
	ErrNotFoundBucket = ErrBucketNotFound

	// ErrBucketAlreadyExist is returned when creating a duplicate bucket.
	// ErrBucketAlreadyExist 表示 bucket 已存在。
	ErrBucketAlreadyExist = errors.New("nutsdb: bucket already exist")

	// ErrKeyNotFound is returned when the key does not exist.
	// ErrKeyNotFound 表示 key 不存在。
	ErrKeyNotFound = errors.New("nutsdb: key not found")

	// ErrKeyEmpty is returned when the key is empty.
	// ErrKeyEmpty 表示 key 为空。
	ErrKeyEmpty = errors.New("nutsdb: key empty")

	// ErrNotSupported is returned for features not available in this version
	// (e.g. List / Set / SortedSet).
	// ErrNotSupported 表示当前版本不支持该能力（如 List / Set / SortedSet）。
	ErrNotSupported = errors.New("nutsdb: not supported")

	// ErrDirRequired is returned when Options.Dir is empty.
	// ErrDirRequired 表示未设置数据目录。
	ErrDirRequired = errors.New("nutsdb: dir required")

	// ErrBucketNameEmpty is returned when the bucket name is empty.
	// ErrBucketNameEmpty 表示 bucket 名称为空。
	ErrBucketNameEmpty = errors.New("nutsdb: bucket name empty")
)
