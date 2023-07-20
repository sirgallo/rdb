package leaderelection

import "context"
import "log"
import "math/rand"
import "net"
import "sync"
import "time"
import "google.golang.org/grpc"
import "google.golang.org/grpc/credentials/insecure"

import "github.com/sirgallo/raft/pkg/lerpc"
import "github.com/sirgallo/raft/pkg/shared"
import "github.com/sirgallo/raft/pkg/utils"


func NewLeaderElectionService(opts *LeaderElectionOpts) *LeaderElectionService {
	return &LeaderElectionService{
		Port:               utils.NormalizePort(opts.Port),
		CurrentTerm:        opts.CurrentTerm,
		LastLogIndex:       opts.LastLogIndex,
		LastLogTerm:        opts.LastLogTerm,
		CurrentSystem:      opts.CurrentSystem,
		SystemsList:        opts.SystemsList,
		State:              Follower,
		VotedFor:           utils.GetZero[string](),
		Timeout:            initializeTimeout(),
		ResetTimeoutSignal: make(chan bool),
	}
}

func (leService *LeaderElectionService) StartLeaderElectionService(listener *net.Listener) {
	log.Println("service timeout period:", leService.Timeout)
	timeoutDuration := time.Duration(leService.Timeout) * time.Millisecond

	srv := grpc.NewServer()
	log.Println("gRPC server is listening on port:", leService.Port)
	lerpc.RegisterLeaderElectionServiceServer(srv, leService)

	log.Printf("systems list on %s: %v", leService.CurrentSystem.Host, utils.Map[*shared.System, string](leService.SystemsList, func(sys *shared.System) string { return sys.Host }))

	go func() {
		err := srv.Serve(*listener)
		if err != nil { log.Fatalf("Failed to serve: %v", err) }
	}()

	for {
		timeout := time.After(timeoutDuration)

		select {
			case <- leService.ResetTimeoutSignal:
			case <- timeout:
				log.Println("Starting election process on", leService.CurrentSystem.Host)
				leService.Election()
			default:
		}
	}
}

func (leService *LeaderElectionService) Election() {
	sysHostPtr := &leService.CurrentSystem.Host
	request := &lerpc.RequestVote{
		CurrentTerm:  *leService.CurrentTerm,
		CandidateId:  *sysHostPtr,
		LastLogIndex: *leService.LastLogIndex,
		LastLogTerm:  *leService.LastLogTerm,
	}

	*leService.CurrentTerm = *leService.CurrentTerm + int64(1)
	leService.State = Candidate
	leService.VotedFor = leService.CurrentSystem.Host

	totalSystems := len(leService.SystemsList) + 1
	minimumVotes := (totalSystems / 2) + 1
	totalVotes := 1

	var requestVoteWG sync.WaitGroup

	for _, sys := range leService.SystemsList {
		requestVoteWG.Add(1)

		go func(sys *shared.System) {
			defer requestVoteWG.Done()

			conn, err := grpc.Dial(sys.Host+leService.Port, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil { log.Fatalf("Failed to connect to %s: %v", sys.Host+leService.Port, err) }

			client := lerpc.NewLeaderElectionServiceClient(conn)

			res, err := client.RequestVoteRPC(context.Background(), request)
			if err != nil { log.Println("failed to request vote -->", err) }

			log.Printf("res on %s: %s", leService.CurrentSystem.Host, res)

			if res.VoteGranted { totalVotes += 1 }

			conn.Close()
		}(sys)
	}

	requestVoteWG.Wait()

	log.Printf("total votes for %s: %d, minimum votes needed: %d\n", leService.CurrentSystem.Host, totalVotes, minimumVotes)

	if totalVotes >= minimumVotes {
		leService.State = Leader
		log.Printf("service with hostname: %s has been elected leader\n", leService.CurrentSystem.Host)
	}
}

func (leService *LeaderElectionService) RequestVoteRPC(ctx context.Context, req *lerpc.RequestVote) (*lerpc.RequestVoteResponse, error) {
	log.Println("RequestVoteRPC received:", req)

	if req.CurrentTerm >= *leService.CurrentTerm {
		return &lerpc.RequestVoteResponse{
			Term:        *leService.CurrentTerm,
			VoteGranted: false,
		}, nil
	}

	if req.LastLogIndex >= *leService.LastLogIndex && req.LastLogTerm >= *leService.LastLogTerm {
		return &lerpc.RequestVoteResponse{
			Term:        utils.GetZero[int64](),
			VoteGranted: true,
		}, nil
	}

	res := &lerpc.RequestVoteResponse{
		Term:        utils.GetZero[int64](),
		VoteGranted: false,
	}

	return res, nil
}

func initializeTimeout() int {
	return rand.Intn(151) + 150
}