package system

import "log"

import "github.com/sirgallo/raft/pkg/utils"


//=========================================== System


func (sys *System[T]) TransitionToFollower(opts StateTransitionOpts) {
	sys.State = Follower
	log.Printf("service with hostname: %s transitioned to follower.\n", sys.Host)

	if opts.VotedFor != nil {
		sys.VotedFor = *opts.VotedFor 
	} else { sys.ResetVotedFor() }

	if opts.CurrentTerm != nil { sys.CurrentTerm = *opts.CurrentTerm }
}

func (sys *System[T]) TransitionToCandidate() {
	sys.State = Candidate
	sys.CurrentTerm = sys.CurrentTerm + int64(1)
	sys.VotedFor = sys.Host

	log.Printf("service with hostname: %s transitioned to candidate, starting election.\n", sys.Host)
}

func (sys *System[T]) TransitionToLeader() {
	sys.State = Leader
	log.Printf("service with hostname: %s has been elected leader.\n", sys.Host)
}

func (sys *System[T]) ResetVotedFor() {
	sys.VotedFor = utils.GetZero[string]()
}