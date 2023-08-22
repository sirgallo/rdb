package replog

import "log"
import "net"
import "time"
import "google.golang.org/grpc"

import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"
import "github.com/sirgallo/raft/pkg/utils"


func NewReplicatedLogService [T comparable](opts *ReplicatedLogOpts[T]) *ReplicatedLogService[T] {
	return &ReplicatedLogService[T]{
		Port: utils.NormalizePort(opts.Port),
		ConnectionPool: opts.ConnectionPool,
		CurrentSystem: opts.CurrentSystem,
		SystemsList: opts.SystemsList,
		AppendLogSignal: make(chan T),
		LeaderAcknowledgedSignal: make(chan bool),
		LogCommitChannel: make(chan []LogCommitChannelEntry[T]),
	}
}

func (rlService *ReplicatedLogService[T]) StartReplicatedLogService(listener *net.Listener) {
	srv := grpc.NewServer()
	log.Println("replog gRPC server is listening on port:", rlService.Port)

	replogrpc.RegisterRepLogServiceServer(srv, rlService)

	go func() {
		err := srv.Serve(*listener)
		if err != nil { log.Fatalf("Failed to serve: %v", err) }
	}()

	for {
		select {
			case newCmd :=<- rlService.AppendLogSignal:
				if rlService.CurrentSystem.State == system.Leader {
					rlService.ReplicateLogs(newCmd)
				}
			case <- time.After(HeartbeatIntervalInMs * time.Millisecond):
				if rlService.CurrentSystem.State == system.Leader {
					log.Println("sending heartbeats...")
					rlService.Heartbeat()
				}
		}
	}
}