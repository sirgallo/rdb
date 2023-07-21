package replog

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/shared"


const HeartbeatIntervalInMs = 50

type ReplicatedLogOpts [T comparable] struct {
	Port int
	ConnectionPool *connpool.ConnectionPool

	CurrentSystem *shared.System[T]
	SystemsList   []*shared.System[T]
}

type ReplicatedLogService [T comparable] struct {
	replogrpc.UnimplementedRepLogServiceServer
	Port string
	ConnectionPool *connpool.ConnectionPool

	// Persistent State
	CurrentSystem *shared.System[T]
	SystemsList   []*shared.System[T]

	// Module Specific

	AppendLogSignal chan *shared.LogEntry[T]
	LeaderAcknowledgedSignal chan bool
}