package replog

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"


const HeartbeatIntervalInMs = 50

type ReplicatedLogOpts [T comparable] struct {
	Port int
	ConnectionPool *connpool.ConnectionPool

	CurrentSystem *system.System[T]
	SystemsList   []*system.System[T]
}

type ReplicatedLogService [T comparable] struct {
	replogrpc.UnimplementedRepLogServiceServer
	Port string
	ConnectionPool *connpool.ConnectionPool

	// Persistent State
	CurrentSystem *system.System[T]
	SystemsList   []*system.System[T]

	// Module Specific

	AppendLogSignal chan *system.LogEntry[T]
	LeaderAcknowledgedSignal chan bool
}