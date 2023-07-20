package utils


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