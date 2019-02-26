package nutsdb

import (
	"bytes"
	"errors"
	"strings"
	"time"

	"github.com/xujiajun/nutsdb/ds/list"
	"github.com/xujiajun/utils/strconv2"
)

const SeparatorForListKey = "|"

func (tx *Tx) RPop(bucket string, key []byte) (item []byte, err error) {
	item, err = tx.RPeek(bucket, key)
	if err != nil {
		return
	}

	return item, tx.push(bucket, key, DataRPopFlag, item)
}

func (tx *Tx) RPeek(bucket string, key []byte) (item []byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if _, ok := tx.db.ListIdx[bucket]; !ok {
		return nil, ErrBucket
	}

	item, _, err = tx.db.ListIdx[bucket].RPeek(string(key))

	return
}

func (tx *Tx) push(bucket string, key []byte, flag uint16, values ...[]byte) error {
	for _, value := range values {
		err := tx.put(bucket, key, value, Persistent, flag, uint64(time.Now().Unix()), DataStructureList)
		if err != nil {
			return err
		}
	}

	return nil
}

func (tx *Tx) RPush(bucket string, key []byte, values ...[]byte) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	if strings.Contains(string(key), SeparatorForListKey) {
		return ErrSeparatorForListKey()
	}

	return tx.push(bucket, key, DataRPushFlag, values...)
}

func (tx *Tx) LPush(bucket string, key []byte, values ...[]byte) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	if strings.Contains(string(key), SeparatorForListKey) {
		return ErrSeparatorForListKey()
	}

	return tx.push(bucket, key, DataLPushFlag, values...)
}

func (tx *Tx) LPop(bucket string, key []byte) (item []byte, err error) {
	item, err = tx.LPeek(bucket, key)
	if err != nil {
		return
	}

	return item, tx.push(bucket, key, DataLPopFlag, item)
}

func (tx *Tx) LPeek(bucket string, key []byte) (item []byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if _, ok := tx.db.ListIdx[bucket]; !ok {
		return nil, ErrBucket
	}

	item, err = tx.db.ListIdx[bucket].LPeek(string(key))

	return
}

func (tx *Tx) LSize(bucket string, key []byte) (int, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return 0, err
	}

	if _, ok := tx.db.ListIdx[bucket]; !ok {
		return 0, ErrBucket
	}

	return tx.db.ListIdx[bucket].Size(string(key))
}

func (tx *Tx) LRange(bucket string, key []byte, start, end int) (list [][]byte, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if _, ok := tx.db.ListIdx[bucket]; !ok {
		return nil, ErrBucket
	}

	return tx.db.ListIdx[bucket].LRange(string(key), start, end)
}

func (tx *Tx) LRem(bucket string, key []byte, count int) error {
	var err error

	size, err := tx.LSize(bucket, key)
	if err != nil {
		return err
	}

	if count >= size {
		return list.ErrCount
	}

	if count < 0 {
		index := size + count
		if index < 0 || index > size {
			return list.ErrCount
		}
	}

	return tx.push(bucket, key, DataLRemFlag, []byte(strconv2.IntToStr(count)))
}

func (tx *Tx) LSet(bucket string, key []byte, index int, value []byte) error {
	var (
		err    error
		buffer bytes.Buffer
	)

	if err = tx.checkTxIsClosed(); err != nil {
		return err
	}

	if _, ok := tx.db.ListIdx[bucket]; !ok {
		return ErrBucket
	}

	if _, ok := tx.db.ListIdx[bucket].Items[string(key)]; !ok {
		return ErrKeyNotFound
	}

	size, _ := tx.LSize(bucket, key)
	if index < 0 || index >= size {
		return list.ErrIndexOutOfRange
	}

	buffer.Write(key)
	buffer.Write([]byte(SeparatorForListKey))
	indexBytes := []byte(strconv2.IntToStr(index))
	buffer.Write(indexBytes)
	newKey := buffer.Bytes()

	return tx.push(bucket, newKey, DataLSetFlag, value)
}

func (tx *Tx) Ltrim(bucket string, key []byte, start, end int) error {
	var (
		err    error
		buffer bytes.Buffer
	)

	if err = tx.checkTxIsClosed(); err != nil {
		return err
	}

	if _, ok := tx.db.ListIdx[bucket]; !ok {
		return ErrBucket
	}

	if _, ok := tx.db.ListIdx[bucket].Items[string(key)]; !ok {
		return ErrKeyNotFound
	}

	if _, err := tx.LRange(bucket, key, start, end); err != nil {
		return err
	}

	buffer.Write(key)
	buffer.Write([]byte(SeparatorForListKey))
	buffer.Write([]byte(strconv2.IntToStr(start)))
	newKey := buffer.Bytes()

	return tx.push(bucket, newKey, DataLTrimFlag, []byte(strconv2.IntToStr(end)))
}

func ErrSeparatorForListKey() error {
	return errors.New("contain separator (" + SeparatorForListKey + ") for List key")
}
