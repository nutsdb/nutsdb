// Copyright 2023 The PromiseDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nutsdb

import (
	"fmt"
	"math/rand"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func GetTestBytes(i int) []byte {
	return []byte(fmt.Sprintf("nutsdb-%09d", i))
}

func GetRandomBytes(length int) []byte {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return b
}

func generateRecords(count int) []*Record {
	bucket := []byte("bucket")
	rand.Seed(time.Now().UnixNano())
	records := make([]*Record, count)
	for i := 0; i < count; i++ {
		key := GetTestBytes(i)
		val := GetRandomBytes(24)

		metaData := &MetaData{
			KeySize:    uint32(len(key)),
			ValueSize:  uint32(len(val)),
			Timestamp:  uint64(time.Now().Unix()),
			TTL:        uint32(rand.Intn(3600)),
			Flag:       uint16(rand.Intn(2)),
			BucketSize: uint32(len(bucket)),
			TxID:       uint64(rand.Intn(1000)),
			Status:     uint16(rand.Intn(2)),
			Ds:         uint16(rand.Intn(3)),
			Crc:        rand.Uint32(),
		}

		record := &Record{
			H: &Hint{
				Key:     key,
				FileID:  int64(i),
				Meta:    metaData,
				DataPos: uint64(rand.Uint32()),
			},
			E: &Entry{
				Key:    key,
				Value:  val,
				Bucket: bucket,
				Meta:   metaData,
			},
			Bucket: string(bucket),
		}
		records[i] = record
	}
	return records
}
