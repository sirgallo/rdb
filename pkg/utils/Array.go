package utils

import "errors"


//=========================================== Array Utils


/*
	Array.Chunk 

	pass in an array of type T with a specified chunk size, and return a nested array
	where each entry is an array of elements for the the chunk size, or the remaining elements
*/

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

/*
	Array.Filter 

	pass in an array of type T and a condition. If the element passes the condition, 
	return it in the new array slice
*/

func Filter [T comparable](array []T, condition func(T) bool) []T {
	var filtered []T
	for _, elem := range array {
		if condition(elem) { filtered = append(filtered, elem) }
	}

	return filtered
}

/*
	Array.Map

	pass in an array of type T and a tranformer function. For each element of the array, 
	apply the transformer and return an array of type V, or all of the transformed elements
*/

func Map [T comparable, V comparable](array []T, transform func(T) V) []V {
	var mapped []V
	for _, elem := range array {
		mapped = append(mapped, transform(elem))
	}

	return mapped
}