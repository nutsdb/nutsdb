package nutsdb

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/xujiajun/nutsdb/ds/zset"
	"github.com/xujiajun/utils/strconv2"
)

const SeparatorForZSetKey = "|"

// ZAdd adds all the specified members with the specified scores to the sorted set stored at key.
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

// ZMembers returns all the members of the set value stored at key.
func (tx *Tx) ZMembers(bucket string) (map[string]*zset.SortedSetNode, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	return tx.db.SortedSetIdx[bucket].Dict, nil
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
func (tx *Tx) ZCard(bucket string) (int, error) {
	members, err := tx.ZMembers(bucket)
	if err != nil {
		return 0, err
	}

	return len(members), nil
}

// ZCount returns the number of elements in the sorted set at key with a score between min and max and opts.
// opts includes the following parameters:
// Limit        int  // limit the max nodes to return
// ExcludeStart bool // exclude start value, so it search in interval (start, end] or (start, end)
// ExcludeEnd   bool // exclude end value, so it search in interval [start, end) or (start, end)
func (tx *Tx) ZCount(bucket string, start, end float64, opts *zset.GetByScoreRangeOptions) (int, error) {
	nodes, err := tx.ZRangeByScore(bucket, start, end, opts)
	if err != nil {
		return 0, err
	}

	return len(nodes), nil
}

// ZPopMax removes and returns up to count members with the highest scores in the sorted set stored at key.
func (tx *Tx) ZPopMax(bucket string) (*zset.SortedSetNode, error) {
	item, err := tx.ZPeekMax(bucket)
	if err != nil {
		return nil, err
	}

	return item, tx.put(bucket, []byte(" "), []byte(""), Persistent, DataZPopMaxFlag, uint64(time.Now().Unix()), DataStructureSortedSet)
}

// ZPopMin removes and returns up to count members with the lowest scores in the sorted set stored at key.
func (tx *Tx) ZPopMin(bucket string) (*zset.SortedSetNode, error) {
	item, err := tx.ZPeekMin(bucket)
	if err != nil {
		return nil, err
	}

	return item, tx.put(bucket, []byte(" "), []byte(""), Persistent, DataZPopMinFlag, uint64(time.Now().Unix()), DataStructureSortedSet)
}

// ZPickMax returns up to count members with the highest scores in the sorted set stored at key.
func (tx *Tx) ZPeekMax(bucket string) (*zset.SortedSetNode, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if _, ok := tx.db.SortedSetIdx[bucket]; !ok {
		return nil, ErrBucket
	}

	return tx.db.SortedSetIdx[bucket].PeekMax(), nil
}

// ZPickMin returns up to count members with the lowest scores in the sorted set stored at key.
func (tx *Tx) ZPeekMin(bucket string) (*zset.SortedSetNode, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if _, ok := tx.db.SortedSetIdx[bucket]; !ok {
		return nil, ErrBucket
	}

	return tx.db.SortedSetIdx[bucket].PeekMin(), nil
}

// ZRangeByScore returns all the elements in the sorted set at key with a score between min and max.
func (tx *Tx) ZRangeByScore(bucket string, start, end float64, opts *zset.GetByScoreRangeOptions) ([]*zset.SortedSetNode, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if _, ok := tx.db.SortedSetIdx[bucket]; !ok {
		return nil, ErrBucket
	}

	return tx.db.SortedSetIdx[bucket].GetByScoreRange(zset.SCORE(start), zset.SCORE(end), opts), nil
}

func (tx *Tx) ZRangByRank(bucket string, start, end int) ([]*zset.SortedSetNode, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if _, ok := tx.db.SortedSetIdx[bucket]; !ok {
		return nil, ErrBucket
	}

	return tx.db.SortedSetIdx[bucket].GetByRankRange(start, end, false), nil
}

func (tx *Tx) ZRem(bucket, key string) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	if _, ok := tx.db.SortedSetIdx[bucket]; !ok {
		return ErrBucket
	}

	return tx.put(bucket, []byte(key), []byte(""), Persistent, DataZRemFlag, uint64(time.Now().Unix()), DataStructureSortedSet)
}

func (tx *Tx) ZRemRangeByRank(bucket string, start, end int) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	if _, ok := tx.db.SortedSetIdx[bucket]; !ok {
		return ErrBucket
	}

	newKey := strconv2.IntToStr(start)
	newVal := strconv2.IntToStr(end)
	return tx.put(bucket, []byte(newKey), []byte(newVal), Persistent, DataZRemRangeByRankFlag, uint64(time.Now().Unix()), DataStructureSortedSet)
}

//Returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
func (tx *Tx) ZRank(bucket string, key []byte) (int, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return 0, err
	}

	if _, ok := tx.db.SortedSetIdx[bucket]; !ok {
		return 0, ErrBucket
	}

	return tx.db.SortedSetIdx[bucket].FindRank(string(key)), nil
}

func (tx *Tx) ZScore(bucket string, key []byte) (float64, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return 0, err
	}

	if _, ok := tx.db.SortedSetIdx[bucket]; !ok {
		return 0, ErrBucket
	}

	if node := tx.db.SortedSetIdx[bucket].GetByKey(string(key)); node != nil {
		return float64(node.Score()), nil
	}

	return 0, ErrNotFoundKey
}

func (tx *Tx) ZGetByKey(bucket string, key []byte) (*zset.SortedSetNode, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if _, ok := tx.db.SortedSetIdx[bucket]; !ok {
		return nil, ErrBucket
	}

	if node := tx.db.SortedSetIdx[bucket].GetByKey(string(key)); node != nil {
		return node, nil
	}

	return nil, ErrNotFoundKey
}

func ErrSeparatorForZSetKey() error {
	return errors.New("contain separator (" + SeparatorForZSetKey + ") for ZSet key")
}
