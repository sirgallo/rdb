package replog

import "log"
import "net"
import "time"
import "google.golang.org/grpc"

import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


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
	}
}

/*
	start the replicated log module/service:
		--> launch the grc server for AppendEntryRPC
		--> start the log timeouts
*/

func (rlService *ReplicatedLogService[T]) StartReplicatedLogService(listener *net.Listener) {
	srv := grpc.NewServer()
	log.Println("replog gRPC server is listening on port:", rlService.Port)

	replogrpc.RegisterRepLogServiceServer(srv, rlService)

	go func() {
		err := srv.Serve(*listener)
		if err != nil { log.Fatalf("Failed to serve: %v", err) }
	}()

	rlService.StartReplicatedLogTimeout()
}

/*
	start the log timeouts:
		separate go routines:
			1.) replicated log timeouts
				--> if a new log is signalled for append to the log, replicate the log to followers
				--> if no logs are received before a heartbeat, sync all existing followers logs to leader (or attempt for batch size)
			2.) heart beat timeout
				--> on a set interval, heartbeat all of the followers in the cluster if leader
*/

func (rlService *ReplicatedLogService[T]) StartReplicatedLogTimeout() {
	didHeartbeat := make(chan bool)
	
	go func() {
		for {
			select {
				case newCmd :=<- rlService.AppendLogSignal:
					if rlService.CurrentSystem.State == system.Leader { rlService.ReplicateLogs(newCmd) }
				case <- didHeartbeat:
					if rlService.CurrentSystem.State == system.Leader { rlService.SyncLogs() }
			}
		}
	}()

	go func() {
		for {
			<- time.After(HeartbeatIntervalInMs * time.Millisecond)
			if rlService.CurrentSystem.State == system.Leader {
				log.Println("sending heartbeats...")
				rlService.Heartbeat()
				didHeartbeat <- true
			}
		}
	}()
}