package leaderelection

import "context"

import "github.com/sirgallo/raft/pkg/lerpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== Leader Election Server


/*
	RequestVoteRPC:
		grpc server implementation

		when a RequestVoteRPC is made to the requestVote server
			1.) load the host from the request into the systems map
			2.) if the incoming request has a higher term than the current system, update the current term and set the system 
				to Follower State
			3.) if the system hasn't voted this term or has already voted for the incoming candidate 
				and the last log index and term	of the request are at least as up to date as what is on the current system
				--> grant the vote to the candidate, reset VotedFor, update the current term to the term of the request, and
					revert back to Follower state
			4.) if the request has a higher term than currently on the system, 
			5.) otherwise, do not grant the vote
*/

func (leService *LeaderElectionService) RequestVoteRPC(ctx context.Context, req *lerpc.RequestVote) (*lerpc.RequestVoteResponse, error) {
	lastLogIndex, lastLogTerm, lastLogErr := leService.CurrentSystem.DetermineLastLogIdxAndTerm()
	if lastLogErr != nil { return nil, lastLogErr }

	s, ok := leService.Systems.Load(req.CandidateId)
	if ! ok { 
		sys := &system.System{
			Host: req.CandidateId,
			Status: system.Ready,
			NextIndex: lastLogIndex,
		}

		leService.Systems.Store(sys.Host, sys)
	} else {
		sys := s.(*system.System)
		sys.SetStatus(system.Ready)
		sys.UpdateNextIndex(lastLogIndex)
	}

	leService.Log.Debug("received requestVoteRPC from:", req.CandidateId)
	leService.Log.Debug("req current term:", req.CurrentTerm, "system current term:", leService.CurrentSystem.CurrentTerm)
	leService.Log.Debug("latest log index:", lastLogIndex, "req last log index:", req.LastLogIndex)
	
	if leService.CurrentSystem.VotedFor == utils.GetZero[string]() || leService.CurrentSystem.VotedFor == req.CandidateId {
		if req.LastLogIndex >= lastLogIndex && req.LastLogTerm >= lastLogTerm {
			leService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{
				CurrentTerm: &req.CurrentTerm,
				VotedFor: &req.CandidateId,
			})

			leService.attemptResetTimeoutSignal()

			voteGranted := &lerpc.RequestVoteResponse{
				Term: leService.CurrentSystem.CurrentTerm,
				VoteGranted: true,
			}

			leService.Log.Info("vote granted to:", req.CandidateId)
			return voteGranted, nil
		}
	}

	if req.CurrentTerm > leService.CurrentSystem.CurrentTerm {
		leService.Log.Warn("RequestVoteRPC higher term")
		leService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{ CurrentTerm: &req.CurrentTerm })
		leService.attemptResetTimeoutSignal()
	}

	voteRejected := &lerpc.RequestVoteResponse{
		Term: leService.CurrentSystem.CurrentTerm,
		VoteGranted: false,
	}

	return voteRejected, nil
}