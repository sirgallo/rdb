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

	CurrentSystem *system.System
	Systems *sync.Map
	
	HeartBeatTimer *time.Timer
	ReplicateLogsTimer *time.Timer

	AppendLogSignal chan *statemachine.StateMachineOperation
	ReadChannel chan *statemachine.StateMachineOperation
	WriteChannel chan *statemachine.StateMachineOperation
	LeaderAcknowledgedSignal chan bool
	ResetTimeoutSignal chan bool
	ForceHeartbeatSignal chan bool
	SyncLogChannel chan string
	SendSnapshotToSystemSignal chan string
	StateMachineResponseChannel chan *statemachine.StateMachineResponse
	ApplyLogsFollowerChannel chan int64
	AppendLogsFollowerRespChannel chan bool
	AppendLogsFollowerChannel chan *replogrpc.AppendEntry

	Log clog.CustomLog
}

type ReplicatedLogRequest struct {
	Host string
	AppendEntry *replogrpc.AppendEntry
}

type RLResponseChannels struct {
	BroadcastClose chan struct{}
	SuccessChan chan int
	HigherTermDiscovered chan int64
}


const NAME = "Replicated Log"
const HeartbeatInterval = 50 * time.Millisecond
const RepLogInterval = 150 * time.Millisecond
const RPCTimeout = 200 * time.Millisecond
const AppendLogBuffSize = 1000000
const ResponseBuffSize = 100000