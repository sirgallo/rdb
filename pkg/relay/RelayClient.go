package relay

import "context"
import "errors"

import "github.com/sirgallo/raft/pkg/relayrpc"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"




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

	return res, nil
}