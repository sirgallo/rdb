package leaderelection

import "math/rand"
import "time"

import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


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

func (leService *LeaderElectionService[T]) GetAliveSystemsAndMinVotes() ([]*system.System[T], int64) {
	aliveSystems := utils.Filter[*system.System[T]](leService.SystemsList, func (sys *system.System[T]) bool { 
		return sys.Status == system.Alive 
	})

	totAliveSystems := len(aliveSystems) + 1
	return aliveSystems, int64((totAliveSystems / 2) + 1)
}

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