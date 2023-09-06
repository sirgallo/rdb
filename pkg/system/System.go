package system

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== System


const NAME = "System"
var Log = clog.NewCustomLog(NAME)


func (sys *System[T]) TransitionToFollower(opts StateTransitionOpts) bool {
	sys.SystemMutex.Lock()
	defer sys.SystemMutex.Unlock()

	resetVotedFor := func (sys *System[T]) { sys.VotedFor = utils.GetZero[string]() }
	sys.State = Follower

	if opts.VotedFor != nil {
		sys.VotedFor = *opts.VotedFor 
	} else { resetVotedFor(sys) }

	if opts.CurrentTerm != nil { sys.CurrentTerm = *opts.CurrentTerm }

	Log.Warn("service with hostname:", sys.Host, "transitioned to follower.")
	return true
}

func (sys *System[T]) TransitionToCandidate() bool {
	sys.SystemMutex.Lock()
	defer sys.SystemMutex.Unlock()

	sys.State = Candidate
	sys.CurrentTerm = sys.CurrentTerm + int64(1)
	sys.VotedFor = sys.Host

	Log.Warn("service with hostname:", sys.Host, "transitioned to candidate, starting election.")
	return true
}

func (sys *System[T]) TransitionToLeader() bool {
	sys.SystemMutex.Lock()
	defer sys.SystemMutex.Unlock()

	sys.State = Leader

	Log.Warn("service with hostname:", sys.Host, "has been elected leader.")
	return true
}

func (sys *System[T]) UpdateNextIndex(newIndex int64) bool {
	sys.SystemMutex.Lock()
	defer sys.SystemMutex.Unlock()

	sys.NextIndex = newIndex

	return true
}