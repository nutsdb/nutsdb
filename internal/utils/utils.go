package utils

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"io"
	"path/filepath"
	"reflect"
)

var fnvHash = fnv.New32a()

func GetFnv32(value []byte) (uint32, error) {
	_, err := fnvHash.Write(value)
	if err != nil {
		return 0, err
	}
	hash := fnvHash.Sum32()
	fnvHash.Reset()
	return hash, nil
}

func ConvertBigEndianBytesToUint64(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}

func ConvertUint64ToBigEndianBytes(value uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, value)
	return b
}

func MarshalInts(ints []int) ([]byte, error) {
	buffer := bytes.NewBuffer([]byte{})
	for _, x := range ints {
		if err := binary.Write(buffer, binary.LittleEndian, int64(x)); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func UnmarshalInts(data []byte) ([]int, error) {
	var ints []int
	buffer := bytes.NewBuffer(data)
	for {
		var i int64
		err := binary.Read(buffer, binary.LittleEndian, &i)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}
		ints = append(ints, int(i))
	}
	return ints, nil
}

func MatchForRange(pattern, bucket string, f func(bucket string) bool) (end bool, err error) {
	match, err := filepath.Match(pattern, bucket)
	if err != nil {
		return true, err
	}
	if match && !f(bucket) {
		return true, nil
	}
	return false, nil
}

func UvarintSize(x uint64) int {
	i := 0
	for x >= 0x80 {
		x >>= 7
		i++
	}
	return i + 1
}

func VarintSize(x int64) int {
	ux := uint64(x<<1) ^ uint64(x>>63)
	return UvarintSize(ux)
}

func GetDiskSizeFromSingleObject(obj interface{}) int64 {
	typ := reflect.TypeOf(obj)
	fields := reflect.VisibleFields(typ)
	if len(fields) == 0 {
		return 0
	}
	var size int64 = 0
	for _, field := range fields {
		// Currently, we only use the unsigned value type for our metadata.go. That's reasonable for us.
		// Because it's not possible to use negative value mark the size of data.
		// But if you want to make it more flexible, please help yourself.
		switch field.Type.Kind() {
		case reflect.Uint8:
			size += 1
		case reflect.Uint16:
			size += 2
		case reflect.Uint32:
			size += 4
		case reflect.Uint64:
			size += 8
		}
	}
	return size
}

func OneOfUint16Array(value uint16, array []uint16) bool {
	for _, v := range array {
		if v == value {
			return true
		}
	}
	return false
}

func EncodeListKey(key []byte, seq uint64) []byte {
	buf := make([]byte, len(key)+8)
	binary.LittleEndian.PutUint64(buf[:8], seq)
	copy(buf[8:], key[:])
	return buf
}

func DecodeListKey(buf []byte) ([]byte, uint64) {
	seq := binary.LittleEndian.Uint64(buf[:8])
	key := make([]byte, len(buf[8:]))
	copy(key[:], buf[8:])
	return key, seq
}
