package replog

import "time"

import "github.com/sirgallo/raft/pkg/log"
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

	HeartBeatTimer *time.Timer

	// Module Specific
	AppendLogSignal          chan T
	LeaderAcknowledgedSignal chan bool
	LogCommitChannel         chan []LogCommitChannelEntry[T]
	ForceHeartbeatSignal		 chan bool

	Log clog.CustomLog
}

type ReplicatedLogRequest struct {
	Host        string
	AppendEntry *replogrpc.AppendEntry
}

type RLResponseChannels struct {
	BroadcastClose *chan struct{}
	SuccessChan *chan int
	HigherTermDiscovered *chan int64
}


const HeartbeatIntervalInMs = 50