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

import "encoding/binary"

const engineKeyBucketIDSize = 8

// encodeEngineKey builds the LSM key: bucketId (big-endian u64) || userKey.
// encodeEngineKey 构造引擎键：bucketId（大端 u64）|| userKey。
func encodeEngineKey(bucketID uint64, userKey []byte) []byte {
	out := make([]byte, engineKeyBucketIDSize+len(userKey))
	binary.BigEndian.PutUint64(out[:engineKeyBucketIDSize], bucketID)
	copy(out[engineKeyBucketIDSize:], userKey)
	return out
}

// encodeBucketPrefix returns the 8-byte bucketId prefix for iteration/deletes.
// encodeBucketPrefix 返回用于扫描/删除的 8 字节 bucketId 前缀。
func encodeBucketPrefix(bucketID uint64) []byte {
	out := make([]byte, engineKeyBucketIDSize)
	binary.BigEndian.PutUint64(out, bucketID)
	return out
}
