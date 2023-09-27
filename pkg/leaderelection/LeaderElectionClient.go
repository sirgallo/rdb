package leaderelection

import "context"
import "sync"
import "sync/atomic"

import "github.com/sirgallo/raft/pkg/lerpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== Leader Election Client


/*
	Election:
		when the election timeout is reached, an election occurs
		
		1.) the current system updates itself to candidate state, votes for itself, and updates the term monotonically
		2.) send RequestVoteRPCs in parallel
		3.) if the candidate receives the minimum number of votes required to be a leader (so quorum),
			the leader updates its state to Leader and immediately sends heartbeats to establish authority. On transition
			to leader, the new leader will also update the next index of all of the known systems to reflect the last log
			index on the system
		4.) if a higher term is discovered, update the current term of the candidate to reflect this and revert back to
			Follower state
		5.) otherwise, set the system back to Follower, reset the VotedFor field, and reinitialize the
			leader election timeout --> so randomly generate new timeout period for the system
*/

func (leService *LeaderElectionService) Election() error {
	leService.CurrentSystem.TransitionToCandidate()
	leRespChans := leService.createLERespChannels()

	defer close(leRespChans.VotesChan)
	defer close(leRespChans.HigherTermDiscovered)

	aliveSystems, minimumVotes := leService.GetAliveSystemsAndMinVotes()
	votesGranted := int64(1)
	electionErrChan := make(chan error, 1)


	var electionWG sync.WaitGroup

	electionWG.Add(1)
	go func() {
		defer electionWG.Done()

		for {
			select {
				case <- leRespChans.BroadcastClose:
					if votesGranted >= int64(minimumVotes) {
						leService.CurrentSystem.TransitionToLeader()
						
						lastLogIndex, _, lastLogErr := leService.CurrentSystem.DetermineLastLogIdxAndTerm()
						if lastLogErr != nil { 
							electionErrChan <- lastLogErr 
							return
						}

						leService.Systems.Range(func(key, value interface{}) bool {
							sys := value.(*system.System)
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
				case <- leRespChans.VotesChan:
					atomic.AddInt64(&votesGranted, 1)
				case term :=<- leRespChans.HigherTermDiscovered:
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
		broadcastErr := leService.broadcastVotes(aliveSystems, leRespChans)
		if broadcastErr != nil { leService.Log.Error("error on broadcast", broadcastErr.Error()) }
	}()

	electionWG.Wait()

	select {
		case electionErr :=<- electionErrChan:
			return electionErr
		default:
			return nil
	}
}

/*
	Broadcast Votes:
		utilized by the Election function
		
		RequestVoteRPCs are generated and a go routine is spawned for each system that a request is being sent to. If a higher term 
		is discovered, all go routines are signalled to stop broadcasting.
*/

func (leService *LeaderElectionService) broadcastVotes(aliveSystems []*system.System, leRespChans LEResponseChannels) error {
	defer close(leRespChans.BroadcastClose)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lastLogIndex, lastLogTerm, lastLogErr := leService.CurrentSystem.DetermineLastLogIdxAndTerm()
	if lastLogErr != nil { return lastLogErr }

	request := &lerpc.RequestVote{
		CurrentTerm:  leService.CurrentSystem.CurrentTerm,
		CandidateId:  leService.CurrentSystem.Host,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
 
	var requestVoteWG sync.WaitGroup
	
	for _, sys := range aliveSystems {
		requestVoteWG.Add(1)

		go func(sys *system.System) {
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
					if connErr != nil { leService.Log.Error("Failed to connect to", sys.Host + leService.Port, ":", connErr.Error()) }

					client := lerpc.NewLeaderElectionServiceClient(conn)

					requestVoteRPC := func() (*lerpc.RequestVoteResponse, error) {
						ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
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

					if res.VoteGranted { leRespChans.VotesChan <- 1 }
					if res.Term > leService.CurrentSystem.CurrentTerm {
						leRespChans.HigherTermDiscovered <- res.Term
						cancel()
					}
				
					leService.ConnectionPool.PutConnection(sys.Host, conn)
			}
		}(sys)
	}

	requestVoteWG.Wait()

	return nil
}

func (leService *LeaderElectionService) createLERespChannels() LEResponseChannels {
	broadcastClose := make(chan struct{})
	votesChan := make(chan int)
	higherTermDiscovered := make(chan int64)

	return LEResponseChannels{
		BroadcastClose: broadcastClose,
		VotesChan: votesChan,
		HigherTermDiscovered: higherTermDiscovered,
	}
}