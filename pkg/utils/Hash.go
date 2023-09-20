package utils

import "crypto/rand"
import "crypto/sha256"
import "fmt"


func GenerateRandomSHA256Hash() (string, error) {
	randomData := make([]byte, 32)
	_, readErr := rand.Read(randomData)
	if readErr != nil { return GetZero[string](), readErr }

	hasher := sha256.New()
	hasher.Write(randomData)
	hashBytes := hasher.Sum(nil)

	hashHexString := fmt.Sprintf("%x", hashBytes)

	return hashHexString, nil
}