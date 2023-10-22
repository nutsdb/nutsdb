package nutsdb

import "reflect"

func GetDiskSizeFromSingleObject(obj interface{}) int64 {
	typ := reflect.TypeOf(obj)
	fields := reflect.VisibleFields(typ)
	if len(fields) == 0 {
		return 0
	}
	var size int64 = 0
	for _, field := range fields {
		// Currently, we only use the unsigned value type for our metadata. That's reasonable for us.
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
