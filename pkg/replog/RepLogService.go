package replog

import "net"
import "time"
import "google.golang.org/grpc"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


//=========================================== RepLog Service


/*
	create a new service instance with passable options
*/

func NewReplicatedLogService(opts *ReplicatedLogOpts) *ReplicatedLogService {
	rlService := &ReplicatedLogService{
		Port: utils.NormalizePort(opts.Port),
		ConnectionPool: opts.ConnectionPool,
		CurrentSystem: opts.CurrentSystem,
		Systems: opts.Systems,
		AppendLogSignal: make(chan statemachine.StateMachineOperation, AppendLogBuffSize),
		LeaderAcknowledgedSignal: make(chan bool),
		ForceHeartbeatSignal: make(chan bool),
		SyncLogChannel: make(chan string),
		SignalStartSnapshot: make(chan bool),
		SignalCompleteSnapshot: make(chan bool),
		SendSnapshotToSystemSignal: make(chan string),
		StateMachineResponseChannel: make(chan statemachine.StateMachineResponse, ResponseBuffSize),
		Log: *clog.NewCustomLog(NAME),
	}

	return rlService
}

/*
	start the replicated log module/service:
		--> launch the grc server for AppendEntryRPC
		--> start the log timeouts
*/

func (rlService *ReplicatedLogService) StartReplicatedLogService(listener *net.Listener) {
	srv := grpc.NewServer()
	rlService.Log.Info("replog gRPC server is listening on port:", rlService.Port)

	replogrpc.RegisterRepLogServiceServer(srv, rlService)

	go func() {
		err := srv.Serve(*listener)
		if err != nil { rlService.Log.Error("Failed to serve:", err.Error()) }
	}()

	rlService.StartReplicatedLogTimeout()
}

/*
	start the log timeouts:
		separate go routines:
			1.) heartbeat timeout
				--> wait for timer to drain, signal heartbeat, and reset timer
			2.) signal complete snapshot
				--> when a snapshot has been completed, signal to unpause the replicated log
			3.) replicated log
				--> if a new log is signalled for append to the log, replicate the log to followers
			4.) heartbeat
				--> on a set interval, heartbeat all of the followers in the cluster if leader
			5.) sync logs
				--> for systems with inconsistent replicated logs, start a separate go routine to sync
					them back up to the leader
*/

func (rlService *ReplicatedLogService) StartReplicatedLogTimeout() {
	rlService.HeartBeatTimer = time.NewTimer(HeartbeatInterval)
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
		for newCmd := range rlService.AppendLogSignal {
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

	go func() {
		for host := range rlService.SyncLogChannel {
			go rlService.SyncLogs(host)
		}
	}()
}

func (rlService *ReplicatedLogService) attemptResetTimeout() {
	select {
		case rlService.ResetTimeoutSignal <- true:
		default:
	}
}