package leaderelection

import "math/rand"
import "time"


func initializeTimeout() time.Duration {
	timeout := rand.Intn(151) + 150
	timeoutDuration := time.Duration(timeout) * time.Millisecond

	return timeoutDuration
}