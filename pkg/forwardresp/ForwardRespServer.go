package forwardresp

import "context"

import "github.com/sirgallo/raft/pkg/forwardresprpc"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== Forward Response Server


/*
	Forward Response RPC:
		grpc server implementation

		when response is received:
			1.) set the request system in the system map if not already
			2.) decode the response, if error return failed response to the leader
			3.) forward the response to the http client, return success
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