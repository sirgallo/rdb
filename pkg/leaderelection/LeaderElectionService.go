package leaderelection

import "log"
import "net"
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

	leService.StartElectionTimeout()
}

func (leService *LeaderElectionService[T]) StartElectionTimeout() {
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