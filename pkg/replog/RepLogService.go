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
		AppendLogSignal: make(chan *statemachine.StateMachineOperation, AppendLogBuffSize),
		ReadChannel: make(chan *statemachine.StateMachineOperation, AppendLogBuffSize),
		WriteChannel: make(chan *statemachine.StateMachineOperation, AppendLogBuffSize),
		LeaderAcknowledgedSignal: make(chan bool),
		ForceHeartbeatSignal: make(chan bool),
		SyncLogChannel: make(chan string),
		SendSnapshotToSystemSignal: make(chan string),
		StateMachineResponseChannel: make(chan *statemachine.StateMachineResponse, ResponseBuffSize),
		AppendLogsFollowerChannel: make(chan *replogrpc.AppendEntry, AppendLogBuffSize),
		AppendLogsFollowerRespChannel: make(chan bool),
		ApplyLogsFollowerChannel: make(chan int64),
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
	Start both leader and follower specific go routines
*/

func (rlService *ReplicatedLogService) StartReplicatedLogTimeout() {
	go rlService.LeaderGoRoutines()
	go rlService.FollowerGoRoutines()
}

/*
	Leader Go Routines:
		separate go routines:
			1.) heartbeat timeout
				--> wait for timer to drain, signal heartbeat, and reset timer
			2.) replicate log timeout:
				--> wait for timer to drain, signal to replicate logs to followers, and reset timer
			3.) heartbeat
				--> on a set interval, heartbeat all of the followers in the cluster if leader
			4.) append log signal
				--> on incoming logs from the request module, determine if the log is a read or write op
					and handle accordingly
			5.) read operation handler
				--> on read operations, do not apply to replicated log and instead read directly from 
					db -- since data is not modified, this is an optimization to improve latency on reads
			6.) write operation handler
				--> append logs to the replicated log in order as the leader receives them
			7.) replicated log
				--> after the replicate log timeout completes, begin replication to followers
			8.) sync logs
				--> for systems with inconsistent replicated logs, start a separate go routine to sync
					them back up to the leader
*/

func (rlService *ReplicatedLogService) LeaderGoRoutines() {
	rlService.HeartBeatTimer = time.NewTimer(HeartbeatInterval)
	rlService.ReplicateLogsTimer = time.NewTimer(RepLogInterval)
	
	timeoutChan := make(chan bool)
	replicateLogsChan := make(chan bool)

	go func() {
		for {
			select {
				case <- rlService.ResetTimeoutSignal:
					rlService.resetHeartbeatTimer()
				case <- rlService.HeartBeatTimer.C:
					timeoutChan <- true
					rlService.resetHeartbeatTimer()
			}
		}
	}()

	go func() {
		for range rlService.ReplicateLogsTimer.C {
			replicateLogsChan <- true
			rlService.resetReplogTimer()
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
		for newCmd := range rlService.AppendLogSignal {
			if rlService.CurrentSystem.State == system.Leader { 
				isReadOperation := func() bool {
					return newCmd.Action == statemachine.FIND || newCmd.Action == statemachine.LISTCOLLECTIONS
				}()

				if isReadOperation {
					rlService.ReadChannel <- newCmd
				}	else { rlService.WriteChannel <- newCmd  }
			}
		}
	}()

	go func() {
		for readCmd := range rlService.ReadChannel {
			resp, readErr := rlService.CurrentSystem.StateMachine.Read(readCmd)
			if readErr != nil { rlService.Log.Error("error reading:", readErr.Error()) }
			
			rlService.StateMachineResponseChannel <- resp
		}
	}()

	go func() {
		for writeCmd := range rlService.WriteChannel {
			appendErr := rlService.AppendWALSync(writeCmd)
			if appendErr != nil { rlService.Log.Error("append error:", appendErr.Error()) }
		}
	}()

	go func() {
		for range replicateLogsChan {
			if rlService.CurrentSystem.State == system.Leader {
				replicationErr := rlService.ReplicateLogs()
				if replicationErr != nil { rlService.Log.Error("error on replication:", replicationErr.Error()) }
			}
		}
	}()

	go func() {
		for host := range rlService.SyncLogChannel {
			go func(host string) {
				_, syncErr := rlService.SyncLogs(host)
				if syncErr != nil { rlService.Log.Error("error syncing logs for host", host, ":", syncErr.Error()) }
			}(host)
		}
	}()
}

/*
	Follower Go Routines:
		separate go routines:
			1.) process logs
				--> as logs are received from AppendEntryRPCs from the leader, process the logs synchronously 
				and signal to the request when complete
			2.) apply logs to state machine
				--> when signalled by a request, and available, apply logs to the state machine up to the request
					commit index
*/

func (rlService *ReplicatedLogService) FollowerGoRoutines() {
	go func() {
		for req := range rlService.AppendLogsFollowerChannel {
			_, appendErr := rlService.ProcessLogsFollower(req)
			if appendErr != nil { 
				rlService.Log.Error("error appending logs to follower:", appendErr.Error()) 
				rlService.AppendLogsFollowerRespChannel <- false
			} else {
				rlService.AppendLogsFollowerRespChannel <- true
			}
		}
	}()

	go func() {
		for commitIndex := range rlService.ApplyLogsFollowerChannel {
			applyErr := rlService.ApplyLogsToStateMachineFollower(commitIndex)
			if applyErr != nil { rlService.Log.Error("error applying logs to state machine:", applyErr.Error()) }
		}
	}()
}

func (rlService *ReplicatedLogService) attemptResetTimeout() {
	select {
		case rlService.ResetTimeoutSignal <- true:
		default:
	}
}