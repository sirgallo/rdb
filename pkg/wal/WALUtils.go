package wal

import "encoding/binary"


//=========================================== Write Ahead Log Utils


/*
	Convert Int To Bytes
		used for creating the byte array keys from the entry index
*/

func ConvertIntToBytes(val int64) []byte {
	byteArray := make([]byte, 8) // 8 bytes for int64
	binary.BigEndian.PutUint64(byteArray, uint64(val))

	return byteArray
}