package leaderelection

import "net"
import "time"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/lerpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"
import "google.golang.org/grpc"


//=========================================== Leader Election Service


const NAME = "Leader Election"


/*
	create a new service instance with passable options
	--> initialize state to Follower and initialize a random timeout period for leader election
*/

func NewLeaderElectionService[T system.MachineCommands](opts *LeaderElectionOpts[T]) *LeaderElectionService[T] {
	leService := &LeaderElectionService[T]{
		Port: utils.NormalizePort(opts.Port),
		ConnectionPool: opts.ConnectionPool,
		CurrentSystem: opts.CurrentSystem,
		Systems: opts.Systems,
		Timeout: initTimeoutOnStartup(),
		ResetTimeoutSignal: make(chan bool),
		HeartbeatOnElection: make(chan bool),
		Log: *clog.NewCustomLog(NAME),
	}

	leService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{})
	return leService
}

/*
	start the replicated log module/service:
		--> launch the grpc server for AppendEntryRPC
		--> start the leader election timeout
*/

func (leService *LeaderElectionService[T]) StartLeaderElectionService(listener *net.Listener) {
	srv := grpc.NewServer()
	leService.Log.Info("leader election gRPC server is listening on port:", leService.Port)
	lerpc.RegisterLeaderElectionServiceServer(srv, leService)

	go func() {
		err := srv.Serve(*listener)
		if err != nil { leService.Log.Error("Failed to serve:", err.Error()) }
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
	timeoutChannel := make(chan bool)

	go func() {
		for {
			select {
				case <- leService.ResetTimeoutSignal:
					leService.resetTimer()
				case <- leService.ElectionTimer.C:
					timeoutChannel <- true
					leService.resetTimer()
			}
		}
	}()

	go func() {
		for {
			<- timeoutChannel
			if leService.CurrentSystem.State == system.Follower { leService.Election() }
		}
	}()
}