package system

import "sync/atomic"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== System


const NAME = "System"
var Log = clog.NewCustomLog(NAME)


/*
	Transition To Follower:
		1.) update state to Follower
		2.) votedFor:
			if voted for is supplied --> update voted for to the supplied hostname
			else --> reset voted for to null
		3.) if current term is supplied, update the system term
*/

func (sys *System) TransitionToFollower(opts StateTransitionOpts) bool {
	sys.SystemMutex.Lock()
	defer sys.SystemMutex.Unlock()

	resetVotedFor := func (sys *System) { sys.VotedFor = utils.GetZero[string]() }
	sys.State = Follower

	if opts.VotedFor != nil {
		sys.VotedFor = *opts.VotedFor 
	} else { resetVotedFor(sys) }

	if opts.CurrentTerm != nil { sys.CurrentTerm = *opts.CurrentTerm }

	Log.Warn("service with hostname:", sys.Host, "transitioned to follower.")
	return true
}

/*
	Transition To Candidate:
		1.) update the state to Candidate
		2.) increment the current term by 1
		3.) update voted for to self
*/

func (sys *System) TransitionToCandidate() bool {
	sys.SystemMutex.Lock()
	defer sys.SystemMutex.Unlock()

	sys.State = Candidate
	sys.CurrentTerm = sys.CurrentTerm + int64(1)
	sys.VotedFor = sys.Host

	Log.Warn("service with hostname:", sys.Host, "transitioned to candidate, starting election.")
	return true
}

/*
	Transition To Leader:
		1.) update state to Leader
*/

func (sys *System) TransitionToLeader() bool {
	sys.SystemMutex.Lock()
	defer sys.SystemMutex.Unlock()

	sys.State = Leader

	Log.Warn("service with hostname:", sys.Host, "has been elected leader.")
	return true
}

/*
	Set Current Leader:
		1.) update the current leader id if not already
*/

func (sys *System) SetCurrentLeader(leaderId string) bool {
	sys.SystemMutex.Lock()
	defer sys.SystemMutex.Unlock()

	if sys.CurrentLeader != leaderId { sys.CurrentLeader = leaderId }
	return true
}

/*
	Set Status:
		1.) update the status of the system to either Dead, Ready, or Busy
*/

func (sys *System) SetStatus(status SystemStatus) bool {
	sys.SystemMutex.Lock()
	defer sys.SystemMutex.Unlock()

	switch status {
		case Dead:
			sys.Status = Dead
		case Ready:
			sys.Status = Ready
		case Busy:
			sys.Status = Busy
		default:
	}

	return true
}

/*
	Update Next Index:
		1.) update the next log index to send for a particular system
*/

func (sys *System) UpdateNextIndex(newIndex int64) bool {
	sys.SystemMutex.Lock()
	defer sys.SystemMutex.Unlock()

	sys.NextIndex = newIndex

	return true
}

func(sys *System) IncrementCommitIndex() bool {
	atomic.AddInt64(&sys.CommitIndex, 1)

	return true
}

func(sys *System) UpdateCommitIndex(newCommitIndex int64) bool {
	atomic.StoreInt64(&sys.CommitIndex, newCommitIndex)

	return true
}

func(sys *System) UpdateLastApplied(newLastAppliedIndex int64) bool {
	atomic.StoreInt64(&sys.LastApplied, newLastAppliedIndex)
	
	return true
}