package utils


/*
	get the null type for any type T
*/

func GetZero [T comparable]() T {
	var result T
	return result
}