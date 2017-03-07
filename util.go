package couchbase

func makeInt64(i interface{}) (v int64) {
	switch n := i.(type) {
	case uint8:
		v = int64(n)
	case uint16:
		v = int64(n)
	case uint32:
		v = int64(n)
	case uint:
		v = int64(n)
	case uint64:
		v = int64(n)
	case int8:
		v = int64(n)
	case int16:
		v = int64(n)
	case int32:
		v = int64(n)
	case int:
		v = int64(n)
	}
	return
}

func makeUint32(i interface{}) (v uint32) {
	switch n := i.(type) {
	case uint8:
		v = uint32(n)
	case uint16:
		v = uint32(n)
	case uint32:
		v = uint32(n)
	case uint:
		v = uint32(n)
	case uint64:
		v = uint32(n)
	case int8:
		v = uint32(n)
	case int16:
		v = uint32(n)
	case int32:
		v = uint32(n)
	case int:
		v = uint32(n)
	case int64:
		v = uint32(n)
	}
	return
}
