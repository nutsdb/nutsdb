// Copyright 2023 The nutsdb Author. All rights reserved.
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

type SortedSetMember struct {
	Value []byte
	Score float64
}

// ZAdd Adds the specified member with the specified score into the sorted set specified by key in a bucket.
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

// ZMembers Returns all the members and scores of members of the set specified by key in a bucket.
func (tx *Tx) ZMembers(bucket string, key []byte) (map[*SortedSetMember]struct{}, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return nil, err
	}

	members, err := tx.db.Index.sortedSet.getWithDefault(bucket, tx.db).ZMembers(string(key))
	if err != nil {
		return nil, err
	}

	res := make(map[*SortedSetMember]struct{})
	for record, score := range members {
		value, err := tx.db.getValueByRecord(record)
		if err != nil {
			return nil, err
		}
		res[&SortedSetMember{
			Value: value,
			Score: float64(score),
		}] = struct{}{}
	}

	return res, nil
}

// ZCard Returns the sorted set cardinality (number of elements) of the sorted set specified by key in a bucket.
func (tx *Tx) ZCard(bucket string, key []byte) (int, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return 0, err
	}

	card, err := tx.db.Index.sortedSet.getWithDefault(bucket, tx.db).ZCard(string(key))
	if err != nil {
		return 0, err
	}
	return card, nil
}

// ZCount Returns the number of elements in the sorted set specified by key in a bucket with a score between min and max and opts.
// Opt includes the following parameters:
// Limit        int  // limit the max nodes to return
// ExcludeStart bool // exclude start value, so it search in interval (start, end] or (start, end)
// ExcludeEnd   bool // exclude end value, so it search in interval [start, end) or (start, end)
func (tx *Tx) ZCount(bucket string, key []byte, start, end float64, opts *GetByScoreRangeOptions) (int, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return 0, err
	}

	count, err := tx.db.Index.sortedSet.getWithDefault(bucket, tx.db).ZCount(string(key), SCORE(start), SCORE(end), opts)
	if err != nil {
		return 0, err
	}

	return count, nil
}

// ZPopMax Removes and returns the member with the highest score in the sorted set specified by key in a bucket.
func (tx *Tx) ZPopMax(bucket string, key []byte) (*SortedSetMember, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return nil, err
	}

	record, score, err := tx.db.Index.sortedSet.getWithDefault(bucket, tx.db).ZPeekMax(string(key))
	if err != nil {
		return nil, err
	}

	value, err := tx.db.getValueByRecord(record)
	if err != nil {
		return nil, err
	}

	return &SortedSetMember{Value: value, Score: float64(score)}, tx.put(bucket, key, []byte(""), Persistent, DataZPopMaxFlag, uint64(time.Now().Unix()), DataStructureSortedSet)
}

// ZPopMin Removes and returns the member with the lowest score in the sorted set specified by key in a bucket.
func (tx *Tx) ZPopMin(bucket string, key []byte) (*SortedSetMember, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return nil, err
	}

	record, score, err := tx.db.Index.sortedSet.getWithDefault(bucket, tx.db).ZPeekMin(string(key))
	if err != nil {
		return nil, err
	}

	value, err := tx.db.getValueByRecord(record)
	if err != nil {
		return nil, err
	}

	return &SortedSetMember{Value: value, Score: float64(score)}, tx.put(bucket, key, []byte(""), Persistent, DataZPopMinFlag, uint64(time.Now().Unix()), DataStructureSortedSet)
}

// ZPeekMax Returns the member with the highest score in the sorted set specified by key in a bucket.
func (tx *Tx) ZPeekMax(bucket string, key []byte) (*SortedSetMember, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return nil, err
	}

	record, score, err := tx.db.Index.sortedSet.getWithDefault(bucket, tx.db).ZPeekMax(string(key))
	if err != nil {
		return nil, err
	}

	value, err := tx.db.getValueByRecord(record)
	if err != nil {
		return nil, err
	}

	return &SortedSetMember{Value: value, Score: float64(score)}, nil
}

// ZPeekMin Returns the member with the lowest score in the sorted set specified by key in a bucket.
func (tx *Tx) ZPeekMin(bucket string, key []byte) (*SortedSetMember, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return nil, err
	}

	record, score, err := tx.db.Index.sortedSet.getWithDefault(bucket, tx.db).ZPeekMin(string(key))
	if err != nil {
		return nil, err
	}

	value, err := tx.db.getValueByRecord(record)
	if err != nil {
		return nil, err
	}

	return &SortedSetMember{Value: value, Score: float64(score)}, nil
}

// ZRangeByScore Returns all the elements in the sorted set specified by key in a bucket with a score between min and max.
// And the parameter `Opts` is the same as ZCount's.
func (tx *Tx) ZRangeByScore(bucket string, key []byte, start, end float64, opts *GetByScoreRangeOptions) ([]*SortedSetMember, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return nil, err
	}

	records, scores, err := tx.db.Index.sortedSet.getWithDefault(bucket, tx.db).ZRangeByScore(string(key), SCORE(start), SCORE(end), opts)
	if err != nil {
		return nil, err
	}

	members := make([]*SortedSetMember, len(records))
	for i, record := range records {
		value, err := tx.db.getValueByRecord(record)
		if err != nil {
			return nil, err
		}
		members[i] = &SortedSetMember{Value: value, Score: scores[i]}
	}

	return members, nil
}

// ZRangeByRank Returns all the elements in the sorted set specified by key in a bucket
// with a rank between start and end (including elements with rank equal to start or end).
func (tx *Tx) ZRangeByRank(bucket string, key []byte, start, end int) ([]*SortedSetMember, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return nil, err
	}

	records, scores, err := tx.db.Index.sortedSet.getWithDefault(bucket, tx.db).ZRangeByRank(string(key), start, end)
	if err != nil {
		return nil, err
	}

	members := make([]*SortedSetMember, len(records))
	for i, record := range records {
		value, err := tx.db.getValueByRecord(record)
		if err != nil {
			return nil, err
		}
		members[i] = &SortedSetMember{Value: value, Score: scores[i]}
	}

	return members, nil
}

// ZRem removes the specified members from the sorted set stored in one bucket at given bucket and key.
func (tx *Tx) ZRem(bucket string, key []byte, value []byte) error {
	if err := tx.ZCheck(bucket); err != nil {
		return err
	}

	exist, err := tx.db.Index.sortedSet.getWithDefault(bucket, tx.db).ZExist(string(key), value)
	if err != nil {
		return err
	}

	if !exist {
		return ErrSortedSetMemberNotExist
	}

	return tx.put(bucket, key, value, Persistent, DataZRemFlag, uint64(time.Now().Unix()), DataStructureSortedSet)
}

// ZRemRangeByRank removes all elements in the sorted set stored in one bucket at given bucket with rank between start and end.
// the rank is 1-based integer. Rank 1 means the first node; Rank -1 means the last node.
func (tx *Tx) ZRemRangeByRank(bucket string, key []byte, start, end int) error {
	if err := tx.ZCheck(bucket); err != nil {
		return err
	}

	startStr := strconv2.IntToStr(start)
	endStr := strconv2.IntToStr(end)
	return tx.put(bucket, key, []byte(startStr+SeparatorForZSetKey+endStr), Persistent, DataZRemRangeByRankFlag, uint64(time.Now().Unix()), DataStructureSortedSet)
}

// ZRank Returns the rank of member in the sorted set specified by key in a bucket, with the scores ordered from low to high.
func (tx *Tx) ZRank(bucket string, key, value []byte) (int, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return 0, err
	}

	return tx.db.Index.sortedSet.getWithDefault(bucket, tx.db).ZRank(string(key), value)
}

// ZRevRank Returns the rank of member in the sorted set specified by key in a bucket, with the scores ordered from high to low.
func (tx *Tx) ZRevRank(bucket string, key, value []byte) (int, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return 0, err
	}

	return tx.db.Index.sortedSet.getWithDefault(bucket, tx.db).ZRevRank(string(key), value)
}

// ZScore Returns the score of members in a sorted set specified by key in a bucket.
func (tx *Tx) ZScore(bucket string, key, value []byte) (float64, error) {
	if err := tx.ZCheck(bucket); err != nil {
		return 0, err
	}

	if score, err := tx.db.Index.sortedSet.getWithDefault(bucket, tx.db).ZScore(string(key), value); err != nil {
		return 0, err
	} else {
		return score, nil
	}
}

// ZKeys find all keys matching a given pattern in a bucket
func (tx *Tx) ZKeys(bucket, pattern string, f func(key string) bool) error {
	if err := tx.ZCheck(bucket); err != nil {
		return err
	}
	for key := range tx.db.Index.sortedSet.getWithDefault(bucket, tx.db).M {
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
	if _, ok := tx.db.Index.sortedSet.exist(bucket); !ok {
		return ErrBucket
	}
	return nil
}

// ErrSeparatorForZSetKey returns when zSet key contains the SeparatorForZSetKey flag.
func ErrSeparatorForZSetKey() error {
	return errors.New("contain separator (" + SeparatorForZSetKey + ") for SortedSetIdx key")
}
