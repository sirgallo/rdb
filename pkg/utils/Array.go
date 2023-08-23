package utils

import "errors"


func Chunk [T comparable](array[]T, chunkSize int) ([][]T, error) {
	if chunkSize <= 0 { return nil, errors.New("chunk size needs to be greater than 0") }
	
	var chunks [][]T
	
	if (len(array) <= chunkSize) { 
		return append(chunks, array), nil
	} else {
		totalChunks := (len(array) / chunkSize) + 1

		for idx := range make([]int, totalChunks - 1) {
			start := idx * chunkSize
			end := (idx + 1) * chunkSize
			chunks = append(chunks, array[start:end])
		}

		startOfRemainder := (totalChunks - 1) * chunkSize
		return append(chunks, array[startOfRemainder:]), nil
	} 
}

func Filter [T comparable](array []T, condition func(T) bool) []T {
	var filtered []T
	for _, elem := range array {
		if condition(elem) { filtered = append(filtered, elem) }
	}

	return filtered
}

func Map [T comparable, V comparable](array []T, transform func(T) V) []V {
	var mapped []V
	for _, elem := range array {
		mapped = append(mapped, transform(elem))
	}

	return mapped
}

func MapChunks [T comparable, V comparable](array [][]T, transform func([]T) V) []V {
	var mapped []V
	for _, elem := range array {
		mapped = append(mapped, transform(elem))
	}

	return mapped
}