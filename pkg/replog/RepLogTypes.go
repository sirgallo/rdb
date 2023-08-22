package replog

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"


type ReplicatedLogOpts [T system.MachineCommands] struct {
	Port           int
	ConnectionPool *connpool.ConnectionPool

	CurrentSystem  *system.System[T]
	SystemsList    []*system.System[T]
}

type LogCommitChannelEntry [T system.MachineCommands] struct {
	LogEntry *system.LogEntry[T]
	Complete bool
}

type ReplicatedLogService [T system.MachineCommands] struct {
	replogrpc.UnimplementedRepLogServiceServer
	Port           string
	ConnectionPool *connpool.ConnectionPool

	// Persistent State
	CurrentSystem *system.System[T]
	SystemsList   []*system.System[T]

	// Module Specific
	AppendLogSignal          chan T
	LeaderAcknowledgedSignal chan bool
	LogCommitChannel         chan []LogCommitChannelEntry[T]
}

type ReplicatedLogRequest struct {
	Host        string
	AppendEntry *replogrpc.AppendEntry
}


const HeartbeatIntervalInMs = 50