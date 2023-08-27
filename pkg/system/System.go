package system

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== System


const NAME = "System"
var Log = clog.NewCustomLog(NAME)


func (sys *System[T]) TransitionToFollower(opts StateTransitionOpts) {
	sys.State = Follower
	Log.Warn("service with hostname:", sys.Host, "transitioned to follower.")

	if opts.VotedFor != nil {
		sys.VotedFor = *opts.VotedFor 
	} else { sys.ResetVotedFor() }

	if opts.CurrentTerm != nil { sys.CurrentTerm = *opts.CurrentTerm }
}

func (sys *System[T]) TransitionToCandidate() {
	sys.State = Candidate
	sys.CurrentTerm = sys.CurrentTerm + int64(1)
	sys.VotedFor = sys.Host

	Log.Warn("service with hostname:", sys.Host, "transitioned to candidate, starting election.")
}

func (sys *System[T]) TransitionToLeader() {
	sys.State = Leader
	Log.Warn("service with hostname:", sys.Host, "has been elected leader.")
}

func (sys *System[T]) ResetVotedFor() {
	sys.VotedFor = utils.GetZero[string]()
}