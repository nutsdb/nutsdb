// Copyright 2019 The nutsdb Author. All rights reserved.
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
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/xujiajun/utils/strconv2"
)

// SeparatorForZSetKey represents separator for zSet key.
const SeparatorForZSetKey = "|"

type ZSetMember struct {
	Value []byte
	Score float64
}

// ZAdd adds the specified member key with the specified score and specified val to the sorted set stored at bucket.
func (tx *Tx) ZAdd(bucket string, key []byte, score float64, val []byte) error {
	var buffer bytes.Buffer

	if strings.Contains(string(key), SeparatorForZSetKey) {
		return ErrSeparatorForZSetKey()
	}

	buffer.Write(key)
	buffer.Write([]byte(SeparatorForZSetKey))
	scoreBytes := []byte(strconv.FormatFloat(score, 'f', -1, 64))
	buffer.Write(scoreBytes)
	newKey := buffer.Bytes()

	return tx.put(bucket, newKey, val, Persistent, DataZAddFlag, uint64(time.Now().Unix()), DataStructureSortedSet)
}

// ZMembers returns all the members of the set value stored at bucket.
func (tx *Tx) ZMembers(bucket string, key string) (map[*ZSetMember]struct{}, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return nil, err
	}

	members, err := tx.db.SortedSetIdx[bucket].ZMembers(key)
	if err != nil {
		return nil, err
	}

	res := make(map[*ZSetMember]struct{})
	for record, score := range members {
		value, err := tx.db.getValueByRecord(record)
		if err != nil {
			return nil, err
		}
		res[&ZSetMember{
			Value: value,
			Score: float64(score),
		}] = struct{}{}
	}

	return res, nil
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at bucket.
func (tx *Tx) ZCard(bucket string, key string) (int, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return 0, err
	}

	card, err := tx.db.SortedSetIdx[bucket].ZCard(key)
	if err != nil {
		return 0, err
	}
	return card, nil
}

// ZCount returns the number of elements in the sorted set at bucket with a score between min and max and opts.
// Opt includes the following parameters:
// Limit        int  // limit the max nodes to return
// ExcludeStart bool // exclude start value, so it search in interval (start, end] or (start, end)
// ExcludeEnd   bool // exclude end value, so it search in interval [start, end) or (start, end)
func (tx *Tx) ZCount(bucket, key string, start, end float64, opts *GetByScoreRangeOptions) (int, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return 0, err
	}

	count, err := tx.db.SortedSetIdx[bucket].ZCount(key, SCORE(start), SCORE(end), opts)
	if err != nil {
		return 0, err
	}

	return count, nil
}

// ZPopMax removes and returns the member with the highest score in the sorted set stored at bucket.
func (tx *Tx) ZPopMax(bucket, key string) (*ZSetMember, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return nil, err
	}

	record, score, err := tx.db.SortedSetIdx[bucket].ZPeekMax(key)
	if err != nil {
		return nil, err
	}

	value, err := tx.db.getValueByRecord(record)
	if err != nil {
		return nil, err
	}

	return &ZSetMember{Value: value, Score: float64(score)}, tx.put(bucket, []byte(key), []byte(""), Persistent, DataZPopMaxFlag, uint64(time.Now().Unix()), DataStructureSortedSet)
}

// ZPopMin removes and returns the member with the lowest score in the sorted set stored at bucket.
func (tx *Tx) ZPopMin(bucket, key string) (*ZSetMember, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return nil, err
	}

	record, score, err := tx.db.SortedSetIdx[bucket].ZPeekMin(key)
	if err != nil {
		return nil, err
	}

	value, err := tx.db.getValueByRecord(record)
	if err != nil {
		return nil, err
	}

	return &ZSetMember{Value: value, Score: float64(score)}, tx.put(bucket, []byte(key), []byte(""), Persistent, DataZPopMinFlag, uint64(time.Now().Unix()), DataStructureSortedSet)
}

// ZPeekMax returns the member with the highest score in the sorted set stored at bucket.
func (tx *Tx) ZPeekMax(bucket, key string) (*ZSetMember, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return nil, err
	}

	record, score, err := tx.db.SortedSetIdx[bucket].ZPeekMax(key)
	if err != nil {
		return nil, err
	}

	value, err := tx.db.getValueByRecord(record)
	if err != nil {
		return nil, err
	}

	return &ZSetMember{Value: value, Score: float64(score)}, nil
}

// ZPeekMin returns the member with the lowest score in the sorted set stored at bucket.
func (tx *Tx) ZPeekMin(bucket, key string) (*ZSetMember, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return nil, err
	}

	record, score, err := tx.db.SortedSetIdx[bucket].ZPeekMin(key)
	if err != nil {
		return nil, err
	}

	value, err := tx.db.getValueByRecord(record)
	if err != nil {
		return nil, err
	}

	return &ZSetMember{Value: value, Score: float64(score)}, nil
}

// ZRangeByScore returns all the elements in the sorted set at bucket with a score between min and max.
func (tx *Tx) ZRangeByScore(bucket, key string, start, end float64, opts *GetByScoreRangeOptions) ([]*ZSetMember, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return nil, err
	}

	records, scores, err := tx.db.SortedSetIdx[bucket].ZRangeByScore(key, SCORE(start), SCORE(end), opts)
	if err != nil {
		return nil, err
	}

	members := make([]*ZSetMember, len(records))
	for i, record := range records {
		value, err := tx.db.getValueByRecord(record)
		if err != nil {
			return nil, err
		}
		members[i] = &ZSetMember{Value: value, Score: scores[i]}
	}

	return members, nil
}

// ZRangeByRank returns all the elements in the sorted set in one bucket and key
// with a rank between start and end (including elements with rank equal to start or end).
func (tx *Tx) ZRangeByRank(bucket, key string, start, end int) ([]*ZSetMember, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return nil, err
	}

	records, scores, err := tx.db.SortedSetIdx[bucket].ZRangeByRank(key, start, end)
	if err != nil {
		return nil, err
	}

	members := make([]*ZSetMember, len(records))
	for i, record := range records {
		value, err := tx.db.getValueByRecord(record)
		if err != nil {
			return nil, err
		}
		members[i] = &ZSetMember{Value: value, Score: scores[i]}
	}

	return members, nil
}

// ZRem removes the specified members from the sorted set stored in one bucket at given bucket and key.
func (tx *Tx) ZRem(bucket, key string, value []byte) error {
	if err := tx.ZCheck(bucket); err != nil {
		return err
	}

	exist, err := tx.db.SortedSetIdx[bucket].ZExist(key, value)
	if err != nil {
		return err
	}

	if !exist {
		return ErrZSetMemberNotExist
	}

	return tx.put(bucket, []byte(key), value, Persistent, DataZRemFlag, uint64(time.Now().Unix()), DataStructureSortedSet)
}

// ZRemRangeByRank removes all elements in the sorted set stored in one bucket at given bucket with rank between start and end.
// the rank is 1-based integer. Rank 1 means the first node; Rank -1 means the last node.
func (tx *Tx) ZRemRangeByRank(bucket, key string, start, end int) error {
	if err := tx.ZCheck(bucket); err != nil {
		return err
	}

	startStr := strconv2.IntToStr(start)
	endStr := strconv2.IntToStr(end)
	return tx.put(bucket, []byte(key), []byte(startStr+SeparatorForZSetKey+endStr), Persistent, DataZRemRangeByRankFlag, uint64(time.Now().Unix()), DataStructureSortedSet)
}

// ZRank returns the rank of member in the sorted set stored in the bucket at given bucket, key and value
// with the scores ordered from low to high.
func (tx *Tx) ZRank(bucket, key string, value []byte) (int, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return 0, err
	}

	return tx.db.SortedSetIdx[bucket].ZRank(key, value)
}

// ZRevRank returns the rank of member in the sorted set stored in the bucket at given bucket, key and value
// with the scores ordered from high to low.
func (tx *Tx) ZRevRank(bucket, key string, value []byte) (int, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return 0, err
	}

	return tx.db.SortedSetIdx[bucket].ZRevRank(key, value)
}

// ZScore returns the score of member in the sorted set in the bucket at given bucket, key and value.
func (tx *Tx) ZScore(bucket, key string, value []byte) (float64, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return 0, err
	}

	if score, err := tx.db.SortedSetIdx[bucket].ZScore(key, value); err != nil {
		return 0, err
	} else {
		return score, nil
	}
}

// ZKeys find all keys matching a given pattern
func (tx *Tx) ZKeys(bucket, pattern string, f func(key string) bool) error {
	if err := tx.ZCheck(bucket); err != nil {
		return err
	}
	for key := range tx.db.SortedSetIdx[bucket].M {
		if end, err := MatchForRange(pattern, key, f); end || err != nil {
			return err
		}
	}
	return nil
}

func (tx *Tx) ZCheck(bucket string) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}
	if _, ok := tx.db.SortedSetIdx[bucket]; !ok {
		return ErrBucket
	}
	return nil
}

// ErrSeparatorForZSetKey returns when zSet key contains the SeparatorForZSetKey flag.
func ErrSeparatorForZSetKey() error {
	return errors.New("contain separator (" + SeparatorForZSetKey + ") for ZSet key")
}
