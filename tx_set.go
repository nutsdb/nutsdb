package nutsdb

import (
	"errors"
	"time"

	"github.com/xujiajun/nutsdb/ds/set"
)

func (tx *Tx) sPut(bucket string, key []byte, dataFlag uint16, items ...[]byte) error {
	for _, item := range items {
		err := tx.put(bucket, key, item, Persistent, dataFlag, uint64(time.Now().Unix()), DataStructureSet)
		if err != nil {
			return err
		}
	}

	return nil
}

func (tx *Tx) SAdd(bucket string, key []byte, items ...[]byte) error {
	return tx.sPut(bucket, key, DataSetFlag, items...)
}

func (tx *Tx) SRem(bucket string, key []byte, items ...[]byte) error {
	return tx.sPut(bucket, key, DataDeleteFlag, items...)
}

func (tx *Tx) SAreMembers(bucket string, key []byte, items ...[]byte) (bool, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return false, err
	}

	if sets, ok := tx.db.SetIdx[bucket]; ok {
		return sets.SAreMembers(string(key), items...)
	}

	return false, ErrBucketAndKey(bucket, key)
}

func (tx *Tx) SIsMember(bucket string, key, item []byte) (bool, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return false, err
	}

	if set, ok := tx.db.SetIdx[bucket]; ok {
		if !set.SIsMember(string(key), item) {
			return false, ErrBucketAndKey(bucket, key)
		}
		return true, nil
	}

	return false, ErrBucketAndKey(bucket, key)
}

func (tx *Tx) SMembers(bucket string, key []byte) (list [][]byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if set, ok := tx.db.SetIdx[bucket]; ok {
		return set.SMembers(string(key))
	}

	return nil, ErrBucketAndKey(bucket, key)

}

func (tx *Tx) SHasKey(bucket string, key []byte) (bool, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return false, err
	}

	if set, ok := tx.db.SetIdx[bucket]; ok {
		return set.SHasKey(string(key)), nil
	}

	return false, ErrBucketAndKey(bucket, key)
}

func (tx *Tx) SPop(bucket string, key []byte) ([]byte, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if _, ok := tx.db.SetIdx[bucket]; ok {
		for item := range tx.db.SetIdx[bucket].M[string(key)] {
			return []byte(item), tx.sPut(bucket, key, DataDeleteFlag, []byte(item))
		}
	}

	return nil, ErrBucketAndKey(bucket, key)
}

func (tx *Tx) SCard(bucket string, key []byte) (int, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return 0, err
	}

	if set, ok := tx.db.SetIdx[bucket]; ok {
		return set.SCard(string(key)), nil
	}

	return 0, ErrBucketAndKey(bucket, key)
}

func (tx *Tx) SDiffByOneBucket(bucket string, key1, key2 []byte) (list [][]byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if set, ok := tx.db.SetIdx[bucket]; ok {
		return set.SDiff(string(key1), string(key2))
	}

	return nil, ErrBucketAndKey(bucket, key1)
}

func (tx *Tx) SDiffByTwoBuckets(bucket1 string, key1 []byte, bucket2 string, key2 []byte) (list [][]byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	var (
		set1, set2 *set.Set
		ok         bool
	)

	if set1, ok = tx.db.SetIdx[bucket1]; !ok {
		return nil, ErrBucketAndKey(bucket1, key1)
	}

	if set2, ok = tx.db.SetIdx[bucket2]; !ok {
		return nil, ErrBucketAndKey(bucket2, key2)
	}

	for item1 := range set1.M[string(key1)] {
		if _, ok := set2.M[string(key2)][item1]; !ok {
			list = append(list, []byte(item1))
		}
	}

	return
}

func (tx *Tx) SMoveByOneBucket(bucket string, key1, key2, item []byte) (bool, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return false, err
	}

	if set, ok := tx.db.SetIdx[bucket]; ok {
		return set.SMove(string(key1), string(key2), item)
	}

	return false, ErrBucket
}

func (tx *Tx) SMoveByTwoBuckets(bucket1 string, key1 []byte, bucket2 string, key2, item []byte) (bool, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return false, err
	}

	var (
		set1, set2 *set.Set
		ok         bool
	)

	if set1, ok = tx.db.SetIdx[bucket1]; !ok {
		return false, ErrBucketAndKey(bucket1, key1)
	}

	if set2, ok = tx.db.SetIdx[bucket2]; !ok {
		return false, ErrBucketAndKey(bucket2, key1)
	}

	if !set1.SHasKey(string(key1)) {
		return false, ErrNotFoundKeyInBucket(bucket1, key1)
	}

	if !set2.SHasKey(string(key2)) {
		return false, ErrNotFoundKeyInBucket(bucket2, key2)
	}

	if _, ok := set2.M[string(key2)][string(item)]; !ok {
		set2.SAdd(string(key2), item)
	}

	set1.SRem(string(key1), item)

	return true, nil
}

func (tx *Tx) SUnionByOneBucket(bucket string, key1, key2 []byte) (list [][]byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if set, ok := tx.db.SetIdx[bucket]; ok {
		return set.SUnion(string(key1), string(key2))
	}

	return nil, ErrBucket
}

func (tx *Tx) SUnionByTwoBuckets(bucket1 string, key1 []byte, bucket2 string, key2 []byte) (list [][]byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	var (
		set1, set2 *set.Set
		ok         bool
	)

	if set1, ok = tx.db.SetIdx[bucket1]; !ok {
		return nil, ErrBucketAndKey(bucket1, key1)
	}

	if set2, ok = tx.db.SetIdx[bucket2]; !ok {
		return nil, ErrBucketAndKey(bucket2, key1)
	}

	if !set1.SHasKey(string(key1)) {
		return nil, ErrNotFoundKeyInBucket(bucket1, key1)
	}

	if !set2.SHasKey(string(key2)) {
		return nil, ErrNotFoundKeyInBucket(bucket2, key2)
	}

	for item1 := range set1.M[string(key1)] {
		list = append(list, []byte(item1))
	}

	for item2 := range set2.M[string(key2)] {
		if _, ok := set1.M[string(key1)][item2]; !ok {
			list = append(list, []byte(item2))
		}
	}

	return
}

func ErrBucketAndKey(bucket string, key []byte) error {
	return errors.New("not found bucket:" + bucket + ",key:" + string(key))
}

func ErrNotFoundKeyInBucket(bucket string, key []byte) error {
	return errors.New(string(key) + " is not in the" + bucket)
}
