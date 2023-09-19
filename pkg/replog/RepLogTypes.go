package replog

import "sync"
import "time"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/replogrpc"
import "github.com/sirgallo/raft/pkg/statemachine"
import "github.com/sirgallo/raft/pkg/system"


type ReplicatedLogOpts struct {
	Port int
	ConnectionPool *connpool.ConnectionPool

	CurrentSystem  *system.System
	SystemsList []*system.System
	Systems *sync.Map
}

type ReplicatedLogService struct {
	replogrpc.UnimplementedRepLogServiceServer
	Port string
	ConnectionPool *connpool.ConnectionPool

	// Persistent State
	CurrentSystem *system.System
	Systems *sync.Map

	HeartBeatTimer *time.Timer

	// Module Specific
	AppendLogSignal chan statemachine.StateMachineOperation
	LeaderAcknowledgedSignal chan bool
	ResetTimeoutSignal chan bool
	ForceHeartbeatSignal chan bool
	SyncLogChannel chan string
	SignalStartSnapshot chan bool
	SignalCompleteSnapshot chan bool
	PauseReplogSignal chan bool
	SendSnapshotToSystemSignal chan string

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