package leaderelection

import "context"
import "log"
import "sync"
import "sync/atomic"

import "github.com/sirgallo/raft/pkg/lerpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== Leader Election


/*
	Election:
		when the election timeout is reached, an election occurs
		
		1.) the current system updates itself to candidate state, votes for itself, and updates the term monotonically
		2.) for all systems with status Alive send RequestVoteRPCs in parallel
		3.) if the candidate receives the minimum number of votes required to be a leader (so quorum),
			the leader updates its state to Leader and begins the leader election process
		4.) otherwise, set the system back to Follower, reset the VotedFor field, and reinitialize the
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
						leService.HeartbeatOnElection <- true
					} else {
						log.Println("min successful votes not received...")
						leService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{})
					}

					return
				case <- *leRespChans.VotesChan:
					atomic.AddInt64(&votesGranted, 1)
				case term :=<- *leRespChans.HigherTermDiscovered:
					log.Println("higher term discovered.")
					leService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{ 
						CurrentTerm: &term, 
					})

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
}

/*
	Broadcast Votes:
		utilized by the Election function
		
		1.) RequestVoteRPCs are generated and a go routine is spawned for each Alive system
		2.) for all systems with status Alive send RequestVoteRPCs in parallel
		3.) if the candidate receives the minimum number of votes required to be a leader (so quorum),
			the leader updates its state to Leader and begins the leader election process
		4.) otherwise, set the system back to Follower, reset the VotedFor field, and reinitialize the
			leader election timeout --> so randomly generate new timeout period for the system
		5.) if a higher term is discovered on a response, immediately revert to follower and cancel any
			additional requests
*/

func (leService *LeaderElectionService[T]) broadcastVotes(aliveSystems []*system.System[T], leRespChans LEResponseChannels) {		
	defer close(*leRespChans.BroadcastClose)
	
	signalStopRPC := make(chan bool)
	stopRPC := make(chan struct{})
	
	defer close(signalStopRPC)

	go func() {
		<- signalStopRPC
		close(stopRPC)
	}()

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
			if connErr != nil { log.Fatalf("Failed to connect to %s: %v", sys.Host + leService.Port, connErr) }

			client := lerpc.NewLeaderElectionServiceClient(conn)

			requestVoteRPC := func() (*lerpc.RequestVoteResponse, error) {
				res, err := client.RequestVoteRPC(context.Background(), request)
				if err != nil { return utils.GetZero[*lerpc.RequestVoteResponse](), err }
				return res, nil
			}
			
			func() {
				select {
					case <- stopRPC:
						return 
					default:
						maxRetries := 5
						expOpts := utils.ExpBackoffOpts{ MaxRetries: &maxRetries, TimeoutInMilliseconds: 1 }
						expBackoff := utils.NewExponentialBackoffStrat[*lerpc.RequestVoteResponse](expOpts)

						res, err := expBackoff.PerformBackoff(requestVoteRPC)
						if err != nil { 
							log.Printf("setting sytem %s to status dead", sys.Host)
							system.SetStatus[T](sys, false)
							return 
						}

						if res.VoteGranted { *leRespChans.VotesChan <- 1 }
						
						if res.Term > leService.CurrentSystem.CurrentTerm {
							*leRespChans.HigherTermDiscovered <- res.Term
							signalStopRPC <- true
						}
				}
			}()
		
			leService.ConnectionPool.PutConnection(sys.Host, conn)
		}(sys)
	}

	requestVoteWG.Wait()
}

/*
	RequestVoteRPC:
		grpc implementation

		when a RequestVoteRPC is made to the requestVote server
			1.) set the system to Alive if it is not already
			2.) if the system hasn't voted this term or has already voted for the incoming candidate 
				and the last log index and term	of the request are at least as up to date as what is on the current system
				--> grant the vote to the candidate, reset VotedFor, update the current term to the term of the request, and
					revert back to Follower state
			3.) otherwise, do not grant the vote
*/

func (leService *LeaderElectionService[T]) RequestVoteRPC(ctx context.Context, req *lerpc.RequestVote) (*lerpc.RequestVoteResponse, error) {
	sys := utils.Filter[*system.System[T]](leService.SystemsList, func (sys *system.System[T]) bool { return sys.Host == req.CandidateId })[0]
	system.SetStatus[T](sys, true)

	lastLogIndex, lastLogTerm := system.DetermineLastLogIdxAndTerm[T](leService.CurrentSystem.Replog)

	log.Printf("req current term: %d, current system current term: %d\n", req.CurrentTerm, leService.CurrentSystem.CurrentTerm)
	log.Printf("latest log index: %d, rep log length: %d\n", lastLogIndex, len(leService.CurrentSystem.Replog))

	if leService.CurrentSystem.VotedFor == utils.GetZero[string]() || leService.CurrentSystem.VotedFor == req.CandidateId {
		if req.LastLogIndex >= lastLogIndex && req.LastLogTerm >= lastLogTerm {
			leService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{
				CurrentTerm: &req.CurrentTerm,
				VotedFor: &req.CandidateId,
			})

			leService.ResetTimeoutSignal <- true

			voteGranted := &lerpc.RequestVoteResponse{
				Term: leService.CurrentSystem.CurrentTerm,
				VoteGranted: true,
			}

			log.Printf("vote granted to: %s", req.CandidateId)
			return voteGranted, nil
		}
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