package relay

import "context"
import "errors"
import "net"
import "time"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/relayrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"
import "google.golang.org/grpc"


//=========================================== Relay Service


const NAME = "Relay"


/*
	create a new service instance with passable options
*/

func NewRelayService[T comparable](opts *RelayOpts[T]) *RelayService[T] {
	rService := &RelayService[T]{
		Port: utils.NormalizePort(opts.Port),
		ConnectionPool: opts.ConnectionPool,
		CurrentSystem: opts.CurrentSystem,
		Systems: opts.Systems,
		RelayChannel: make(chan T, 100000),
		RelayedAppendLogSignal: make(chan T),
		Log: *clog.NewCustomLog(NAME),
	}

	rService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{})
	return rService
}

/*
	start the relay module/service
		--> launch the grpc server for RelayRPC
		--> start the pass through for entries from follower to leader
*/

func (rService *RelayService[T]) StartRelayService(listener *net.Listener) {
	srv := grpc.NewServer()
	rService.Log.Info("relay gRPC server is listening on port:", rService.Port)
	relayrpc.RegisterRelayServiceServer(srv, rService)

	go func() {
		err := srv.Serve(*listener)
		if err != nil { rService.Log.Error("Failed to serve:", err) }
	}()

	rService.RelayListener()
}

/*
	Relay Listener:
		launch go routines for both relaying commands to the leader or appeneding to the failed
		buffer for re-processing
*/

func (rService *RelayService[T]) RelayListener() {
	failedBuffer := make(chan T, 1000)
	
	go func() {
		cmd :=<- failedBuffer
		rService.RelayedAppendLogSignal <- cmd
	}()

	go func() {
		for {
			cmd :=<- rService.RelayChannel
			_, err := rService.RelayClientRPC(cmd)
			if err != nil { rService.Log.Error("dropping relay request, appending to failed buffer for retry") }
		}
	}()
}

/*
	Relay Client RPC:
		used by followers to relay commands to the leader for processing
		1.) if leader is not set, return for retry
		2.) encode the command to string
		3.) attempt relay using exponential backoff
		4.) if entry is not processed, return error
		5.) otherwise, return success
*/

func (rService *RelayService[T]) RelayClientRPC(cmd T) (*relayrpc.RelayResponse, error){
	if rService.CurrentSystem.CurrentLeader == utils.GetZero[string]() { 
		return nil, errors.New("current leader not set") 
	}

	leader := rService.CurrentSystem.CurrentLeader
	strCmd, encErr := utils.EncodeStructToString[T](cmd)
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
		rService.Log.Error("Failed to connect to", leader + rService.Port, ":", connErr) 
		return nil, connErr
	}

	client := relayrpc.NewRelayServiceClient(conn)

	relayRPC := func() (*relayrpc.RelayResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Millisecond)
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
		rService.Log.Warn("system", leader, "unreachable, removing from registered systems.")
		rService.Systems.Delete(leader)
		rService.ConnectionPool.CloseConnections(leader)
		
		return nil, err
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
*/

func (rService *RelayService[T]) RelayRPC(ctx context.Context, req *relayrpc.RelayRequest) (*relayrpc.RelayResponse, error) {
	sys := &system.System[T]{
		Host: req.Host,
		NextIndex: 0,
	}

	rService.Systems.LoadOrStore(sys.Host, sys)

	cmd, decErr := utils.DecodeStringToStruct[T](req.Command)
	if decErr != nil { 
		rService.Log.Error("error decoding command")
		failedResp := &relayrpc.RelayResponse{ ProcessedRequest: false }
		return failedResp, decErr
	}

	if rService.CurrentSystem.State != system.Leader { 
		rService.RelayChannel <- *cmd 
	} else { rService.RelayedAppendLogSignal <- *cmd }

	successResp := &relayrpc.RelayResponse{ ProcessedRequest: true }
	return successResp, nil
}