package replog

import "net"
import "time"
import "google.golang.org/grpc"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== RepLog Service


const NAME = "Replicated Log"


/*
	create a new service instance with passable options
*/

func NewReplicatedLogService [T comparable](opts *ReplicatedLogOpts[T]) *ReplicatedLogService[T] {
	rlService := &ReplicatedLogService[T]{
		Port: utils.NormalizePort(opts.Port),
		ConnectionPool: opts.ConnectionPool,
		CurrentSystem: opts.CurrentSystem,
		Systems: opts.Systems,
		AppendLogSignal: make(chan T, 100000),
		LeaderAcknowledgedSignal: make(chan bool),
		LogCommitChannel: make(chan []LogCommitChannelEntry[T]),
		ForceHeartbeatSignal: make(chan bool),
		Log: *clog.NewCustomLog(NAME),
	}

	for _, sys := range opts.SystemsList {
		rlService.Systems.Store(sys.Host, sys)
	}

	return rlService
}

/*
	start the replicated log module/service:
		--> launch the grc server for AppendEntryRPC
		--> start the log timeouts
*/

func (rlService *ReplicatedLogService[T]) StartReplicatedLogService(listener *net.Listener) {
	srv := grpc.NewServer()
	rlService.Log.Info("replog gRPC server is listening on port:", rlService.Port)

	replogrpc.RegisterRepLogServiceServer(srv, rlService)

	go func() {
		err := srv.Serve(*listener)
		if err != nil { rlService.Log.Error("Failed to serve:", err) }
	}()

	rlService.StartReplicatedLogTimeout()
}

/*
	start the log timeouts:
		separate go routines:
			1.) replicated log
				--> if a new log is signalled for append to the log, replicate the log to followers
			2.) heartbeat timeout
				--> on a set interval, heartbeat all of the followers in the cluster if leader
*/

func (rlService *ReplicatedLogService[T]) StartReplicatedLogTimeout() {
	rlService.HeartBeatTimer = time.NewTimer(HeartbeatIntervalInMs * time.Millisecond)
	timeoutChan := make(chan bool)
	
	go func() {
		for {
			select {
				case <- rlService.ResetTimeoutSignal:
					rlService.resetTimer()
				case <- rlService.HeartBeatTimer.C:
					timeoutChan <- true
					rlService.resetTimer()
			}
		}
	}()

	go func() {
		for {
			newCmd :=<- rlService.AppendLogSignal
			if rlService.CurrentSystem.State == system.Leader { rlService.ReplicateLogs(newCmd) }
		}
	}()

	go func() {
		for {
			select {
				case <- timeoutChan:
					if rlService.CurrentSystem.State == system.Leader {
						rlService.Log.Info("sending heartbeats...")
						rlService.Heartbeat()
					}
				case <- rlService.ForceHeartbeatSignal:
					if rlService.CurrentSystem.State == system.Leader {
						rlService.attemptResetTimeout()
						rlService.Log.Info("sending heartbeats after election...")
						rlService.Heartbeat()
					}
				}
		}
	}()
}

func (rlService *ReplicatedLogService[T]) attemptResetTimeout() {
	select {
		case rlService.ResetTimeoutSignal <- true:
		default:
	}
}