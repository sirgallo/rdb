package forwardresp

import "net"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/forwardresprpc"
import "github.com/sirgallo/raft/pkg/statemachine"
// import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"
import "google.golang.org/grpc"


//=========================================== Relay Service


/*
	create a new service instance with passable options
*/

func NewForwardRespService(opts *ForwardRespOpts) *ForwardRespService {
	frService := &ForwardRespService{
		Port: utils.NormalizePort(opts.Port),
		ConnectionPool: opts.ConnectionPool,
		CurrentSystem: opts.CurrentSystem,
		Systems: opts.Systems,
		LeaderRelayResponseChannel: make(chan statemachine.StateMachineResponse, RelayRespBuffSize),
		ForwardRespChannel: make(chan statemachine.StateMachineResponse, ForwardRespChannel),
		Log: *clog.NewCustomLog(NAME),
	}

	return frService
}

/*
	start the relay module/service
		--> launch the grpc server for RelayRPC
		--> start the pass through for entries from follower to leader
*/

func (frService *ForwardRespService) StartForwardRespService(listener *net.Listener) {
	srv := grpc.NewServer()
	frService.Log.Info("forward resp gRPC server is listening on port:", frService.Port)
	forwardresprpc.RegisterForwardRespServiceServer(srv, frService)

	go func() {
		err := srv.Serve(*listener)
		if err != nil { frService.Log.Error("Failed to serve:", err.Error()) }
	}()

	frService.ForwardListener()
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

func (frService *ForwardRespService) ForwardListener() {
	go func(){
		for response := range frService.LeaderRelayResponseChannel {
			_, forwardErr := frService.ForwardRespClientRPC(response)
			if forwardErr != nil { frService.Log.Error("unable to forward response back to origin:", forwardErr.Error()) }
		}
	}()
}