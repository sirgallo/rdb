package leaderelection

import "math/rand"
import "time"

import "github.com/sirgallo/raft/pkg/system"


//=========================================== Leader Election Utils


/*
	initialize the timeout period for the Follower node

	this implementation has chosen a standard 150-300ms timeout, but a more dynamic
	approach could be taken to calculate the timeout
*/

func calculateTimeout() time.Duration {
	timeout := rand.Intn(151) + 150
	return time.Duration(timeout) * time.Millisecond
}

func initTimeoutOnStartup() time.Duration {
	timeDuration := calculateTimeout()
	return timeDuration
}

/*
	Get Alive Systems And Min Votes:
		helper method for both determining the current alive systems in the cluster and also the minimum votes
		needed for transitioning to leader

		--> minimum is found by floor(total systems / 2) + 1
*/

func (leService *LeaderElectionService[T]) GetAliveSystemsAndMinVotes() ([]*system.System[T], int64) {
	var aliveSystems []*system.System[T]

	leService.Systems.Range(func(key, value interface{}) bool {
		sys := value.(*system.System[T])
		if sys.Status != system.Dead { aliveSystems = append(aliveSystems, sys) }
		return true
	})

	totAliveSystems := len(aliveSystems) + 1
	return aliveSystems, int64((totAliveSystems / 2) + 1)
}

/*
	Reset Timer:
		1.) Generate a randomized timeout between 150-300ms
		2.) if unable to stop the timer, drain the timer
		3.) reset the timer with the new random timeout period
*/

func (leService *LeaderElectionService[T]) resetTimer() {
	reInitTimeout := func() {
		timeoutDuration := calculateTimeout()
		leService.Timeout = timeoutDuration
	}

	reInitTimeout()

	if ! leService.ElectionTimer.Stop() {
    select {
			case <- leService.ElectionTimer.C:
			default:
		}
	}

	leService.ElectionTimer.Reset(leService.Timeout)
}