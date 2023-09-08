package leaderelection

import "context"
import "sync"
import "sync/atomic"
import "time"

import "github.com/sirgallo/raft/pkg/lerpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== Leader Election


/*
	Election:
		when the election timeout is reached, an election occurs
		
		1.) the current system updates itself to candidate state, votes for itself, and updates the term monotonically
		2.) send RequestVoteRPCs in parallel
		3.) if the candidate receives the minimum number of votes required to be a leader (so quorum),
			the leader updates its state to Leader and immediately sends heartbeats to establish authority
		4.) if a higher term is discovered, update the current term of the candidate to reflect this and revert back to
			Follower state
		5.) otherwise, set the system back to Follower, reset the VotedFor field, and reinitialize the
			leader election timeout --> so randomly generate new timeout period for the system
*/

func (leService *LeaderElectionService[T]) Election() {
	leService.CurrentSystem.TransitionToCandidate()
	leRespChans := leService.createLERespChannels()
	aliveSystems, minimumVotes := leService.GetAliveSystemsAndMinVotes()
	votesGranted := int64(1)

	var electionWG sync.WaitGroup

	electionWG.Add(1)
	go func() {
		defer electionWG.Done()

		for {
			select {
				case <- *leRespChans.BroadcastClose:
					if votesGranted >= int64(minimumVotes) {
						leService.CurrentSystem.TransitionToLeader()
						lastLogIndex, _ := system.DetermineLastLogIdxAndTerm[T](leService.CurrentSystem.Replog)
						leService.Systems.Range(func(key, value any) bool {
							sys := value.(*system.System[T])
							sys.UpdateNextIndex(lastLogIndex)
							
							return true
						})

						leService.HeartbeatOnElection <- true
					} else {
						leService.Log.Warn("min successful votes not received...")
						leService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{})
						leService.attemptResetTimeoutSignal()
					}

					return
				case <- *leRespChans.VotesChan:
					atomic.AddInt64(&votesGranted, 1)
				case term :=<- *leRespChans.HigherTermDiscovered:
					leService.Log.Warn("higher term discovered.")
					leService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{ CurrentTerm: &term })
					leService.attemptResetTimeoutSignal()
					return
			}
		}
	}()

	electionWG.Add(1)
	go func() {
		defer electionWG.Done()
		leService.broadcastVotes(aliveSystems, leRespChans)
	}()

	electionWG.Wait()

	close(*leRespChans.VotesChan)
	close(*leRespChans.HigherTermDiscovered)
}

/*
	Broadcast Votes:
		utilized by the Election function
		
		RequestVoteRPCs are generated and a go routine is spawned for each system that a request is being sent to. If a higher term 
		is discovered, all go routines are signalled to stop broadcasting.
*/

func (leService *LeaderElectionService[T]) broadcastVotes(aliveSystems []*system.System[T], leRespChans LEResponseChannels) {
	defer close(*leRespChans.BroadcastClose)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lastLogIndex, lastLogTerm := system.DetermineLastLogIdxAndTerm[T](leService.CurrentSystem.Replog)
	request := &lerpc.RequestVote{
		CurrentTerm:  leService.CurrentSystem.CurrentTerm,
		CandidateId:  leService.CurrentSystem.Host,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
 
	var requestVoteWG sync.WaitGroup
	
	for _, sys := range aliveSystems {
		requestVoteWG.Add(1)

		go func(sys *system.System[T]) {
			defer requestVoteWG.Done()
			
			conn, connErr := leService.ConnectionPool.GetConnection(sys.Host, leService.Port)
			if connErr != nil { 
				leService.Log.Error("Failed to connect to", sys.Host + leService.Port, "-->", connErr.Error()) 
				return
			}

			select {
				case <- ctx.Done():
					leService.ConnectionPool.PutConnection(sys.Host, conn)
					return
				default:
					conn, connErr := leService.ConnectionPool.GetConnection(sys.Host, leService.Port)
					if connErr != nil { leService.Log.Error("Failed to connect to", sys.Host + leService.Port, "-->", connErr.Error()) }

					client := lerpc.NewLeaderElectionServiceClient(conn)

					requestVoteRPC := func() (*lerpc.RequestVoteResponse, error) {
						ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Millisecond)
						defer cancel()

						res, err := client.RequestVoteRPC(ctx, request)
						if err != nil { return utils.GetZero[*lerpc.RequestVoteResponse](), err }
						return res, nil
					}

					maxRetries := 5
					expOpts := utils.ExpBackoffOpts{ MaxRetries: &maxRetries, TimeoutInMilliseconds: 1 }
					expBackoff := utils.NewExponentialBackoffStrat[*lerpc.RequestVoteResponse](expOpts)

					res, err := expBackoff.PerformBackoff(requestVoteRPC)
					if err != nil { 
						leService.Log.Warn("system", sys.Host, "unreachable, setting status to dead")

						sys.SetStatus(system.Dead)
						leService.ConnectionPool.CloseConnections(sys.Host)
						
						return 
					}

					if res.VoteGranted { *leRespChans.VotesChan <- 1 }
					if res.Term > leService.CurrentSystem.CurrentTerm {
						*leRespChans.HigherTermDiscovered <- res.Term
						cancel()
					}
				
					leService.ConnectionPool.PutConnection(sys.Host, conn)
			}
		}(sys)
	}

	requestVoteWG.Wait()
}

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

func (leService *LeaderElectionService[T]) RequestVoteRPC(ctx context.Context, req *lerpc.RequestVote) (*lerpc.RequestVoteResponse, error) {
	s, ok := leService.Systems.Load(req.CandidateId)
	if ! ok { 
		sys := &system.System[T]{
			Host: req.CandidateId,
			Status: system.Ready,
		}

		leService.Systems.Store(sys.Host, sys)
	} else {
		sys := s.(*system.System[T])
		sys.SetStatus(system.Ready)
	}

	lastLogIndex, lastLogTerm := system.DetermineLastLogIdxAndTerm[T](leService.CurrentSystem.Replog)

	leService.Log.Info("req current term:", req.CurrentTerm, "system current term:", leService.CurrentSystem.CurrentTerm)
	leService.Log.Debug("latest log index:", lastLogIndex, "rep log length:", len(leService.CurrentSystem.Replog))
	
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

func (leService *LeaderElectionService[T]) createLERespChannels() LEResponseChannels {
	broadcastClose := make(chan struct{})
	votesChan := make(chan int)
	higherTermDiscovered := make(chan int64)

	return LEResponseChannels{
		BroadcastClose: &broadcastClose,
		VotesChan: &votesChan,
		HigherTermDiscovered: &higherTermDiscovered,
	}
}

func (leService *LeaderElectionService[T]) attemptResetTimeoutSignal() {
	select {
		case leService.ResetTimeoutSignal <- true:
		default:
	}
}