package utils

import "bytes"
import "encoding/base64"
import "encoding/gob"
import "encoding/json"


//=========================================== Encode/Decode JSON Utils


/*
	encode a struct of type T to a string (json stringify)
*/

func EncodeStructToString [T comparable](data T) (string, error) {
	var buf bytes.Buffer
  enc := gob.NewEncoder(&buf)
	
	err := enc.Encode(data)
	if err != nil { return GetZero[string](), err }
	
	binaryData := buf.Bytes()
	base64String := base64.StdEncoding.EncodeToString(binaryData)
	return base64String, nil
}

/*
	encode a struct of type T to a string (json stringify)
*/

func EncodeStructToBytes [T comparable](data T) ([]byte, error) {
	var buf bytes.Buffer
  enc := gob.NewEncoder(&buf)
	
	err := enc.Encode(data)
	if err != nil { return nil, err }
	
	binaryData := buf.Bytes()

	return binaryData, nil
}

/*
	decode a string to a struct of type T
*/

func DecodeStringToStruct [T comparable](encoded string) (*T, error) {
	decodedBinaryData, binaryErr := base64.StdEncoding.DecodeString(encoded)
	if binaryErr != nil { return nil, binaryErr }
	
	decodedObj := new(T)
  dec := gob.NewDecoder(bytes.NewReader(decodedBinaryData))
  decErr := dec.Decode(&decodedObj)
	if decErr != nil { return nil, decErr }

	return decodedObj, nil
}

/*
	decode a byte array to a struct of type T
*/

func DecodeBytesToStruct [T comparable](encoded []byte) (*T, error) {
	decodedObj := new(T)
  dec := gob.NewDecoder(bytes.NewReader(encoded))
  decErr := dec.Decode(&decodedObj)
	if decErr != nil { return nil, decErr }

	return decodedObj, nil
}


func EncodeStructToJSONString [T comparable](data T) (string, error) {
	encoded, err := json.Marshal(data)
	if err != nil { return GetZero[string](), err }
	
	return string(encoded), nil
}

func DecodeJSONStringToStruct [T comparable](encoded string) (*T, error) {
	data := new(T)
	err := json.Unmarshal([]byte(encoded), data)
	if err != nil { return nil, err }
	
	return data, nil
}