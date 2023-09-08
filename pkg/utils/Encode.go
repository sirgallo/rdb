package utils

import "encoding/json"


//=========================================== Encode/Decode JSON Utils


/*
	encode a struct of type T to a string (json stringify)
*/

func EncodeStructToString [T comparable](data T) (string, error) {
	encoded, err := json.Marshal(data)
	if err != nil { return GetZero[string](), err }
	
	return string(encoded), nil
}

/*
	encode a struct of type T to a string (json stringify)
*/

func EncodeStructToBytes [T comparable](data T) ([]byte, error) {
	encoded, err := json.Marshal(data)
	if err != nil { return nil, err }

	return encoded, nil
}

/*
	decode a string to a struct of type T
*/

func DecodeStringToStruct [T comparable](encoded string) (*T, error) {
	data := new(T)
	err := json.Unmarshal([]byte(encoded), data)
	if err != nil { return nil, err }
	
	return data, nil
}

/*
	decode a byte array to a struct of type T
*/

func DecodeBytesToStruct [T comparable](encoded []byte) (*T, error) {
	data := new(T)
	err := json.Unmarshal(encoded, data)
	if err != nil { return nil, err }
	
	return data, nil
}