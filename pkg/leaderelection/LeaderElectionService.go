package leaderelection

// import "log"
import "net"
import "time"

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/lerpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"
import "google.golang.org/grpc"


const NAME = "Leader Election"


//=========================================== Leader Election Service

/*
	create a new service instance with passable options
	--> initialize state to Follower and initialize a random timeout period for leader election
*/

func NewLeaderElectionService[T comparable](opts *LeaderElectionOpts[T]) *LeaderElectionService[T] {
	leService := &LeaderElectionService[T]{
		Port:                utils.NormalizePort(opts.Port),
		ConnectionPool:      opts.ConnectionPool,
		CurrentSystem:       opts.CurrentSystem,
		SystemsList:         opts.SystemsList,
		Timeout:             initTimeoutOnStartup(),
		ResetTimeoutSignal:  make(chan bool),
		HeartbeatOnElection: make(chan bool),
		Log: *clog.NewCustomLog(NAME),
	}

	leService.CurrentSystem.State = system.Follower
	leService.CurrentSystem.ResetVotedFor()

	return leService
}

/*
	start the replicated log module/service:
		--> launch the grc server for AppendEntryRPC
		--> start the leader election timeout
*/

func (leService *LeaderElectionService[T]) StartLeaderElectionService(listener *net.Listener) {
	srv := grpc.NewServer()
	leService.Log.Info("leader election gRPC server is listening on port:", leService.Port)
	lerpc.RegisterLeaderElectionServiceServer(srv, leService)

	go func() {
		err := srv.Serve(*listener)
		if err != nil {
			leService.Log.Error("Failed to serve:", err)
		}
	}()

	leService.StartElectionTimeout()
}

/*
	start the election timeouts:
		1.) if a signal is passed indicating that an AppendEntryRPC has been received from a
			legitimate leader, reset the election timeout
		2.) otherwise, on timeout, start the leader election process
*/

func (leService *LeaderElectionService[T]) StartElectionTimeout() {
	leService.ElectionTimer = time.NewTimer(leService.Timeout)

	for {
		select {
		case <-leService.ResetTimeoutSignal: // if an appendEntry rpc is received on replicated log module
			leService.resetTimer()
		case <-leService.ElectionTimer.C:
			if leService.CurrentSystem.State == system.Follower {
				leService.Election()
			}
			leService.resetTimer()
		}
	}
}
