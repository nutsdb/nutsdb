package utils

import (
	"reflect"
	"slices"
)

func OneOfUint16Array(value uint16, array []uint16) bool {
	return slices.Contains(array, value)
}

func GetDiskSizeFromSingleObject(obj any) int64 {
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

func UvarintSize(x uint64) int {
	i := 0
	for x >= 0x80 {
		x >>= 7
		i++
	}
	return i + 1
}
