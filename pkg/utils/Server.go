package utils

import "strconv"


func NormalizePort(port int) string {
	return ":" + strconv.Itoa(port)
}