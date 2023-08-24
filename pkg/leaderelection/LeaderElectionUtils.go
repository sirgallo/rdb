package leaderelection

import "math/rand"
import "time"


//=========================================== RepLog Utils


/*
	initialize the timeout period for the Follower node

	this implementation has chosen a standard 150-300ms timeout, but a more dynamic
	approach could be taken to calculate the timeout
*/

func initializeTimeout() time.Duration {
	timeout := rand.Intn(151) + 150
	timeoutDuration := time.Duration(timeout) * time.Millisecond

	return timeoutDuration
}