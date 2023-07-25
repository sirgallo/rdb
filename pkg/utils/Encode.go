package utils

import "encoding/json"


func EncodeStructToString [T comparable](data T) (string, error) {
	encoded, err := json.Marshal(data)
	if err != nil { return GetZero[string](), err }
	return string(encoded), nil
}

func DecodeStringToStruct [T comparable](encoded string) (*T, error) {
	data := new(T)
	err := json.Unmarshal([]byte(encoded), data)
	if err != nil { return nil, err }
	return data, nil
}