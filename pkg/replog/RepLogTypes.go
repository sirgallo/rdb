package replog

import "sync"
import "time"

import "github.com/sirgallo/raft/pkg/log"
import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/system"


type ReplicatedLogOpts [T log.MachineCommands] struct {
	Port int
	ConnectionPool *connpool.ConnectionPool

	CurrentSystem  *system.System[T]
	SystemsList []*system.System[T]
	Systems *sync.Map
}

type LogCommitChannelEntry [T log.MachineCommands] struct {
	LogEntry *log.LogEntry[T]
	Complete bool
}

type ReplicatedLogService [T log.MachineCommands] struct {
	replogrpc.UnimplementedRepLogServiceServer
	Port string
	ConnectionPool *connpool.ConnectionPool

	// Persistent State
	CurrentSystem *system.System[T]
	Systems *sync.Map

	HeartBeatTimer *time.Timer

	// Module Specific
	AppendLogSignal chan T
	LeaderAcknowledgedSignal chan bool
	LogApplyChan chan []LogCommitChannelEntry[T]
	ResetTimeoutSignal chan bool
	ForceHeartbeatSignal chan bool
	SyncLogChannel chan string
	SignalSnapshot chan bool

	Log clog.CustomLog
}

type ReplicatedLogRequest struct {
	Host string
	AppendEntry *replogrpc.AppendEntry
}

type RLResponseChannels struct {
	BroadcastClose *chan struct{}
	SuccessChan *chan int
	HigherTermDiscovered *chan int64
}


const NAME = "Replicated Log"
const HeartbeatInterval = 50 * time.Millisecond
const RPCTimeout = 200 * time.Millisecond
const AppendLogBuffSize = 1000000