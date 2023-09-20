package relay

import "context"
import "errors"
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
		RelayedResponseChannel: make(chan statemachine.StateMachineResponse, RelayChannelBuffSize),
		ClientMappedResponseChannel: make(map[string]*chan statemachine.StateMachineResponse),
		ForwardRespChannel: make(chan statemachine.StateMachineResponse, ForwardRespChannel),
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
		cmd :=<- failedBuffer
		if rService.CurrentSystem.State == system.Follower {
			rService.RelayChannel <- cmd
		} else if rService.CurrentSystem.State == system.Leader { rService.RelayedAppendLogSignal <- cmd }
	}()

	go func() {
		for {
			cmd :=<- rService.RelayChannel
			if rService.CurrentSystem.State == system.Follower {
				_, err := rService.RelayClientRPC(cmd)
				if err != nil { rService.Log.Error("dropping relay request, appending to failed buffer for retry", err.Error()) }
			} else if rService.CurrentSystem.State == system.Leader { rService.RelayedAppendLogSignal <- cmd }
		}
	}()

	go func(){
		for {
			response :=<- rService.RelayedResponseChannel
			rService.Mutex.Lock()
			clientChannel, ok := rService.ClientMappedResponseChannel[response.RequestID]
			rService.Mutex.Unlock()

			if ok {
				*clientChannel <- response
			} else { rService.Log.Warn("no channel for resp associated with req uuid:", response.RequestID) }
		}
	}()
}

/*
	Relay Client RPC:
		used by followers to relay commands to the leader for processing
		2.) encode the command to string
		3.) attempt relay using exponential backoff
		4.) if entry is not processed and a response is not returned for the client, return error
		5.) otherwise, forward the response to the client associated with the request id and return success
*/

func (rService *RelayService) RelayClientRPC(cmd statemachine.StateMachineOperation) (*relayrpc.RelayResponse, error){
	if rService.CurrentSystem.CurrentLeader == utils.GetZero[string]() { 
		return nil, errors.New("current leader not set") 
	}

	leader := rService.CurrentSystem.CurrentLeader
	strCmd, encErr := utils.EncodeStructToString[statemachine.StateMachineOperation](cmd)
	if encErr != nil { 
		rService.Log.Error("error encoding log struct to string") 
		return nil, encErr
	}
	
	relayReq := &relayrpc.RelayRequest{
		Host: rService.CurrentSystem.Host,
		Command: strCmd,
	}

	conn, connErr := rService.ConnectionPool.GetConnection(leader, rService.Port)
	if connErr != nil { 
		rService.Log.Error("Failed to connect to", leader + rService.Port, ":", connErr.Error()) 
		return nil, connErr
	}

	client := relayrpc.NewRelayServiceClient(conn)

	relayRPC := func() (*relayrpc.RelayResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
		defer cancel()
		
		res, err := client.RelayRPC(ctx, relayReq)
		if err != nil { return utils.GetZero[*relayrpc.RelayResponse](), err }
		if ! res.ProcessedRequest { 
			rService.Log.Error("relay not processed")
			return utils.GetZero[*relayrpc.RelayResponse](), err
		}

		return res, nil
	}
	
	maxRetries := 5
	expOpts := utils.ExpBackoffOpts{ MaxRetries: &maxRetries, TimeoutInMilliseconds: 1 }
	expBackoff := utils.NewExponentialBackoffStrat[*relayrpc.RelayResponse](expOpts)
			
	res, err := expBackoff.PerformBackoff(relayRPC)
	if err != nil { 
		rService.Log.Warn("system", leader, "unreachable, setting status to dead")

		sys, ok := rService.Systems.Load(leader)
		if ok { sys.(*system.System).SetStatus(system.Dead) }

		rService.ConnectionPool.CloseConnections(leader)
		
		return nil, err
	}

	if res.ProcessedRequest {
		resp, decErr := utils.DecodeStringToStruct[statemachine.StateMachineResponse](res.StateMachineResponse)
		if decErr != nil { return nil, decErr }

		rService.ForwardRespChannel <- *resp
	}

	return res, nil
}

/*
	Relay RPC:
		grpc server implementation

		when relay is received:
			1.) set the request system in the system map if not already
			2.) decode the command
			3.) if current system is leader, pass through to the replicated log module
			4.) if system is not leader, append to the relay channel to be relayed again
			5.) wait for a response from the channel associated with request id and once a response is received, 
				return a response back to the relayed follower with the response to pass from the follower back to the
				client
				--> a context with timeout is created, and if the resp from the leader does not return before the 
				timeout, a failed response is returned
*/

func (rService *RelayService) RelayRPC(ctx context.Context, req *relayrpc.RelayRequest) (*relayrpc.RelayResponse, error) {
	s, ok := rService.Systems.Load(req.Host)
	if ! ok { 
		sys := &system.System{
			Host: req.Host,
			Status: system.Ready,
		}

		rService.Systems.Store(sys.Host, sys)
	} else {
		sys := s.(*system.System)
		if sys.Status == system.Dead { 
			sys.SetStatus(system.Ready)
		}
	}

	cmd, decErr := utils.DecodeStringToStruct[statemachine.StateMachineOperation](req.Command)
	if decErr != nil { 
		rService.Log.Error("error decoding command", decErr.Error())
		failedResp := &relayrpc.RelayResponse{ ProcessedRequest: false }
		return failedResp, decErr
	}

	if rService.CurrentSystem.State == system.Follower { 
		rService.RelayChannel <- *cmd 
	} else if rService.CurrentSystem.State == system.Leader { 
		clientResponseChannel := make(chan statemachine.StateMachineResponse)

		rService.Mutex.Lock()
		rService.ClientMappedResponseChannel[cmd.RequestID] = &clientResponseChannel
		rService.Mutex.Unlock()
		
		rService.RelayedAppendLogSignal <- *cmd 

		ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
		defer cancel()

		select {
			case <- ctx.Done():
				delete(rService.ClientMappedResponseChannel, cmd.RequestID)
			default:
				resp :=<- clientResponseChannel
				delete(rService.ClientMappedResponseChannel, cmd.RequestID)

				strResp, encErr := utils.EncodeStructToString[statemachine.StateMachineResponse](resp)
				if encErr != nil {
					failedResp := &relayrpc.RelayResponse{ ProcessedRequest: false }
					return failedResp, encErr
				}
		
				successResp := &relayrpc.RelayResponse{ 
					ProcessedRequest: true,
					StateMachineResponse: strResp,
				}
		
				return successResp, nil
		}
	}

	failedResp := &relayrpc.RelayResponse{ ProcessedRequest: false }
	return failedResp, nil
}