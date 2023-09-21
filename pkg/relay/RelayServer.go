package relay

import "context"

import "github.com/sirgallo/raft/pkg/relayrpc"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== Relay Server


/*
	Relay RPC:
		grpc server implementation

		when relay is received:
			1.) set the request system in the system map if not already
			2.) decode the command
			3.) if current system is leader, pass through to the replicated log module to append to the replicated log
			4.) if system is not leader, append to the relay channel to be relayed again to the actual leader
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
		rService.RelayChannel <- cmd 
	} else if rService.CurrentSystem.State == system.Leader { rService.RelayedAppendLogSignal <- cmd }

	return &relayrpc.RelayResponse{ 
		ProcessedRequest: true,
	}, nil
}