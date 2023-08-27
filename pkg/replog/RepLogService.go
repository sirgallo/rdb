package replog

import "net"
import "time"
import "google.golang.org/grpc"

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


const NAME = "Leader Election"


//=========================================== RepLog Service


/*
	create a new service instance with passable options
*/

func NewReplicatedLogService [T comparable](opts *ReplicatedLogOpts[T]) *ReplicatedLogService[T] {
	return &ReplicatedLogService[T]{
		Port: utils.NormalizePort(opts.Port),
		ConnectionPool: opts.ConnectionPool,
		CurrentSystem: opts.CurrentSystem,
		SystemsList: opts.SystemsList,
		AppendLogSignal: make(chan T, 100000),
		LeaderAcknowledgedSignal: make(chan bool),
		LogCommitChannel: make(chan []LogCommitChannelEntry[T]),
		ForceHeartbeatSignal: make(chan bool),
		Log: *clog.NewCustomLog(NAME),
	}
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
			1.) replicated log timeouts
				--> if a new log is signalled for append to the log, replicate the log to followers
				--> if no logs are received before a heartbeat, sync all existing followers logs to leader (or attempt for batch size)
			2.) heartbeat timeout
				--> on a set interval, heartbeat all of the followers in the cluster if leader
*/

func (rlService *ReplicatedLogService[T]) StartReplicatedLogTimeout() {
	rlService.HeartBeatTimer = time.NewTimer(HeartbeatIntervalInMs * time.Millisecond)

	for {
		select {
		
		case newCmd :=<- rlService.AppendLogSignal:
			if rlService.CurrentSystem.State == system.Leader { rlService.ReplicateLogs(newCmd) }
			rlService.resetTimer()
		case <- rlService.HeartBeatTimer.C:
			if rlService.CurrentSystem.State == system.Leader {
				rlService.Log.Info("sending heartbeats...")
				rlService.Heartbeat()
			}

			rlService.resetTimer()
		case <- rlService.ForceHeartbeatSignal:
			if rlService.CurrentSystem.State == system.Leader {
				rlService.Log.Info("sending heartbeats after election...")
				rlService.Heartbeat()
			}

			rlService.resetTimer()
		}
	}
}