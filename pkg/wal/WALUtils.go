package wal

import "encoding/binary"


func ConvertIntToBytes(val int64) []byte {
	byteArray := make([]byte, 8) // 8 bytes for int64
	binary.BigEndian.PutUint64(byteArray, uint64(val))

	return byteArray
}