package leaderelection

import "context"
import "log"
import "math/rand"
import "net"
import "sync"
import "time"
import "google.golang.org/grpc"

import "github.com/sirgallo/raft/pkg/lerpc"
import "github.com/sirgallo/raft/pkg/shared"
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

	leService.CurrentSystem.State = shared.Follower

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

	time.Sleep(1 * time.Second)	// wait for server to start up

	for {
		select {
			case <- leService.ResetTimeoutSignal: // if an appendEntry rpc is received on replicated log module
			case <- time.After(leService.Timeout):
				if leService.CurrentSystem.State == shared.Follower {
					log.Println("timeout reached, starting election process on", leService.CurrentSystem.Host)
					leService.Election()
				}
		}
	}
}

func (leService *LeaderElectionService[T]) Election() {
	leService.CurrentSystem.CurrentTerm = leService.CurrentSystem.CurrentTerm + int64(1)
	leService.CurrentSystem.State = shared.Candidate
	leService.VotedFor = leService.CurrentSystem.Host

	totalSystems := len(leService.SystemsList) + 1
	minimumVotes := (totalSystems / 2) + 1

	totalVotes := leService.BroadcastVotes()

	if totalVotes >= minimumVotes {
		leService.CurrentSystem.State = shared.Leader
		log.Printf("service with hostname: %s has been elected leader\n", leService.CurrentSystem.Host)
	} else {
		leService.CurrentSystem.State = shared.Follower
		leService.VotedFor = utils.GetZero[string]()
	}

	leService.Timeout = initializeTimeout()	// re-init timeout after election
}

func (leService *LeaderElectionService[T]) BroadcastVotes() int {
	lastLogIndex, lastLogTerm := shared.DetermineLastLogIdxAndTerm[T](leService.CurrentSystem.Replog)
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
		requestVoteWG.Add(1)

		go func(sys *shared.System[T]) {
			defer requestVoteWG.Done()

			conn, err := leService.ConnectionPool.GetConnection(sys.Host, leService.Port)
			if err != nil { log.Fatalf("Failed to connect to %s: %v", sys.Host + leService.Port, err) }

			client := lerpc.NewLeaderElectionServiceClient(conn)

			res, err := client.RequestVoteRPC(context.Background(), request)
			if err != nil { log.Println("failed to request vote -->", err) }

			if res.Term > leService.CurrentSystem.CurrentTerm { 
				leService.CurrentSystem.CurrentTerm = res.Term 
				higherTermDiscovered <- true
			}

			if res.VoteGranted { totalVotes += 1 }

			leService.ConnectionPool.PutConnection(sys.Host, conn)
		}(sys)
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
	if req.CurrentTerm > leService.CurrentSystem.CurrentTerm {
		leService.VotedFor = req.CandidateId
		leService.CurrentSystem.CurrentTerm = req.CurrentTerm
		leService.CurrentSystem.State = shared.Follower

		voteGranted := &lerpc.RequestVoteResponse{
			Term: req.CurrentTerm,
			VoteGranted: true,
		}

		return voteGranted, nil
	} else if req.CurrentTerm == leService.CurrentSystem.CurrentTerm && (leService.VotedFor == utils.GetZero[string]() || leService.VotedFor == req.CandidateId) {
		lastLogIndex, lastLogTerm := shared.DetermineLastLogIdxAndTerm[T](leService.CurrentSystem.Replog)
		
		if req.LastLogIndex >= lastLogIndex && req.LastLogTerm >= lastLogTerm {
			leService.VotedFor = req.CandidateId
			leService.CurrentSystem.CurrentTerm = req.CurrentTerm
			leService.CurrentSystem.State = shared.Follower

			voteGranted := &lerpc.RequestVoteResponse{
				Term: utils.GetZero[int64](),
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

func initializeTimeout() time.Duration {
	timeout := rand.Intn(151) + 150
	timeoutDuration := time.Duration(timeout) * time.Millisecond

	return timeoutDuration
}