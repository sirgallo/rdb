package forwardresp

import "context"

import "github.com/sirgallo/raft/pkg/forwardresprpc"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


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

func (frService *ForwardRespService) ForwardRespRPC(ctx context.Context, req *forwardresprpc.ForwardRespRequest) (*forwardresprpc.ForwardRespResponse, error) {
	s, ok := frService.Systems.Load(req.Host)
	if ! ok { 
		sys := &system.System{
			Host: req.Host,
			Status: system.Ready,
		}

		frService.Systems.Store(sys.Host, sys)
	} else {
		sys := s.(*system.System)
		if sys.Status == system.Dead { 
			sys.SetStatus(system.Ready)
		}
	}

	resp, decErr := utils.DecodeStringToStruct[statemachine.StateMachineResponse](req.StateMachineResponse)
	if decErr != nil { 
		frService.Log.Error("error decoding command", decErr.Error())
		failedResp := &forwardresprpc.ForwardRespResponse{ ProcessedRequest: false }
		return failedResp, decErr
	}

	frService.ForwardRespChannel <- *resp
		
	return &forwardresprpc.ForwardRespResponse{ 
		ProcessedRequest: true,
	}, nil
}