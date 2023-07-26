package leaderelection

import "context"
import "log"
import "math/rand"
import "net"
import "sync"
import "time"
import "google.golang.org/grpc"

import "github.com/sirgallo/raft/pkg/lerpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


func NewLeaderElectionService [T comparable](opts *LeaderElectionOpts[T]) *LeaderElectionService[T] {
	leService := &LeaderElectionService[T]{
		Port:               utils.NormalizePort(opts.Port),
		ConnectionPool:     opts.ConnectionPool,
		CurrentSystem:      opts.CurrentSystem,
		SystemsList:        opts.SystemsList,
		VotedFor:           utils.GetZero[string](),
		Timeout:            initializeTimeout(),
		ResetTimeoutSignal: make(chan bool),
	}

	leService.CurrentSystem.State = system.Follower

	return leService
}

func (leService *LeaderElectionService[T]) StartLeaderElectionService(listener *net.Listener) {
	log.Println("service timeout period:", leService.Timeout)

	srv := grpc.NewServer()
	log.Println("leader election gRPC server is listening on port:", leService.Port)
	lerpc.RegisterLeaderElectionServiceServer(srv, leService)

	go func() {
		err := srv.Serve(*listener)
		if err != nil { log.Fatalf("Failed to serve: %v", err) }
	}()

	for {
		select {
			case <- leService.ResetTimeoutSignal: // if an appendEntry rpc is received on replicated log module
			case <- time.After(leService.Timeout):
				if leService.CurrentSystem.State == system.Follower {
					log.Println("timeout reached, starting election process on", leService.CurrentSystem.Host)
					leService.Election()
				}
		}
	}
}

func (leService *LeaderElectionService[T]) Election() {
	leService.CurrentSystem.CurrentTerm = leService.CurrentSystem.CurrentTerm + int64(1)
	leService.CurrentSystem.State = system.Candidate
	leService.VotedFor = leService.CurrentSystem.Host

	aliveSystems := utils.Filter[*system.System[T]](leService.SystemsList, func (sys *system.System[T]) bool { 
		return sys.Status == system.Alive 
	})

	totalAliveSystems := len(aliveSystems) + 1
	minimumVotes := (totalAliveSystems / 2) + 1

	totalVotes := leService.broadcastVotes()

	if totalVotes >= minimumVotes {
		leService.CurrentSystem.State = system.Leader
		log.Printf("service with hostname: %s has been elected leader\n", leService.CurrentSystem.Host)
	} else { leService.CurrentSystem.State = system.Follower }

	leService.VotedFor = utils.GetZero[string]()
	leService.Timeout = initializeTimeout()	// re-init timeout after election
}

func (leService *LeaderElectionService[T]) broadcastVotes() int {
	lastLogIndex, lastLogTerm := system.DetermineLastLogIdxAndTerm[T](leService.CurrentSystem.Replog)
	request := &lerpc.RequestVote{
		CurrentTerm:  leService.CurrentSystem.CurrentTerm,
		CandidateId:  leService.CurrentSystem.Host,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	totalVotes := 1 // vote for self
	higherTermDiscovered := make(chan bool, 1)

	var requestVoteWG sync.WaitGroup

	for _, sys := range leService.SystemsList {
		if sys.Status == system.Alive { 
			requestVoteWG.Add(1)

			go func(sys *system.System[T]) {
				defer requestVoteWG.Done()
	
				conn, connErr := leService.ConnectionPool.GetConnection(sys.Host, leService.Port)
				if connErr != nil { log.Fatalf("Failed to connect to %s: %v", sys.Host + leService.Port, connErr) }
	
				client := lerpc.NewLeaderElectionServiceClient(conn)
	
				requestVoteRPC := func () (*lerpc.RequestVoteResponse, error) {
					res, err := client.RequestVoteRPC(context.Background(), request)
					if err != nil { return utils.GetZero[*lerpc.RequestVoteResponse](), err }
					return res, nil
				}
	
				maxRetries := 5
				expOpts := utils.ExpBackoffOpts{ MaxRetries: &maxRetries, TimeoutInMilliseconds: 1 }
				expBackoff := utils.NewExponentialBackoffStrat[*lerpc.RequestVoteResponse](expOpts)
	
				res, err := expBackoff.PerformBackoff(requestVoteRPC)
				if err != nil { 
					log.Printf("setting sytem %s to status dead", sys.Host)
					system.SetStatus[T](sys, false)

					return 
				}
	
				if res.Term > leService.CurrentSystem.CurrentTerm { leService.CurrentSystem.CurrentTerm = res.Term }
				if res.VoteGranted { totalVotes += 1 }
	
				leService.ConnectionPool.PutConnection(sys.Host, conn)
			}(sys)
		}
	}

	requestVoteWG.Wait()

	select {
		case <- higherTermDiscovered:
			return 0
		default:
			return totalVotes
	}
}

func (leService *LeaderElectionService[T]) RequestVoteRPC(ctx context.Context, req *lerpc.RequestVote) (*lerpc.RequestVoteResponse, error) {
	log.Printf("req current term: %d, current system current term: %d\n", req.CurrentTerm, leService.CurrentSystem.CurrentTerm)
	
	sys := utils.Filter[*system.System[T]](leService.SystemsList, func (sys *system.System[T]) bool { return sys.Host == req.CandidateId })[0]
	system.SetStatus[T](sys, true)

	if leService.VotedFor == utils.GetZero[string]() || leService.VotedFor == req.CandidateId {
		lastLogIndex, lastLogTerm := system.DetermineLastLogIdxAndTerm[T](leService.CurrentSystem.Replog)
		
		if req.LastLogIndex >= lastLogIndex && req.LastLogTerm >= lastLogTerm {
			leService.VotedFor = req.CandidateId
			leService.CurrentSystem.CurrentTerm = req.CurrentTerm
			leService.CurrentSystem.State = system.Follower

			voteGranted := &lerpc.RequestVoteResponse{
				Term: leService.CurrentSystem.CurrentTerm,
				VoteGranted: true,
			}

			return voteGranted, nil
		}
	}

	voteRejected := &lerpc.RequestVoteResponse{
		Term: leService.CurrentSystem.CurrentTerm,
		VoteGranted: false,
	}

	return voteRejected, nil
}


//========================================== helper methods


func initializeTimeout() time.Duration {
	timeout := rand.Intn(151) + 150
	timeoutDuration := time.Duration(timeout) * time.Millisecond

	return timeoutDuration
}

func (leService *LeaderElectionService[T]) DeferenceSystems() []system.System[T] {
	var systems []system.System[T]
	for _, sys := range leService.SystemsList {
		systems = append(systems, *sys)
	}

	return systems
}