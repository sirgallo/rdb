package relay

import "net"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/relayrpc"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"
import "google.golang.org/grpc"


//=========================================== Relay Service


/*
	create a new service instance with passable options
*/

func NewRelayService(opts *RelayOpts) *RelayService {
	rService := &RelayService{
		Port: utils.NormalizePort(opts.Port),
		ConnectionPool: opts.ConnectionPool,
		CurrentSystem: opts.CurrentSystem,
		Systems: opts.Systems,
		RelayChannel: make(chan statemachine.StateMachineOperation, RelayChannelBuffSize),
		RelayedAppendLogSignal: make(chan statemachine.StateMachineOperation),
		Log: *clog.NewCustomLog(NAME),
	}

	return rService
}

/*
	start the relay module/service
		--> launch the grpc server for RelayRPC
		--> start the pass through for entries from follower to leader
*/

func (rService *RelayService) StartRelayService(listener *net.Listener) {
	srv := grpc.NewServer()
	rService.Log.Info("relay gRPC server is listening on port:", rService.Port)
	relayrpc.RegisterRelayServiceServer(srv, rService)

	go func() {
		err := srv.Serve(*listener)
		if err != nil { rService.Log.Error("Failed to serve:", err.Error()) }
	}()

	rService.RelayListener()
}

/*
	Relay Listener:
		launch go routines for both relaying commands to the leader or appeneding to the failed
		buffer for re-processing
			1.) failed buffer
				if a relay fails exponential backoff, add it to the failed buffer to reattempt
			2.) relay channel
				if a new command enters the relay channel, which occurs when a follower receives a request from a client,
				send a relay of the command to the current leader
			3.) relayed response channel
				on responses to responses from the state machine, check the request id of the response and 
				forward the state machine response to the request associated with the id
				--> for leaders 
*/

func (rService *RelayService) RelayListener() {
	failedBuffer := make(chan statemachine.StateMachineOperation, FailedBuffSize)
	
	go func() {
		for cmd := range failedBuffer {
			if rService.CurrentSystem.State == system.Follower {
				rService.RelayChannel <- cmd
			} else if rService.CurrentSystem.State == system.Leader { rService.RelayedAppendLogSignal <- cmd }
		}
	}()

	go func() {
		for cmd := range rService.RelayChannel {
			if rService.CurrentSystem.State == system.Follower {
				_, err := rService.RelayClientRPC(cmd)
				if err != nil { rService.Log.Error("dropping relay request, appending to failed buffer for retry", err.Error()) }
			}
		}
	}()
}