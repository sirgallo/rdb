package leaderelection

import "sync"
import "time"

import "github.com/sirgallo/raft/pkg/logger"
import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/lerpc"
import "github.com/sirgallo/raft/pkg/system"


type TimeoutRange struct {
	Min int
	Max int
}

type LeaderElectionOpts [T system.MachineCommands] struct {
	Port int
	ConnectionPool *connpool.ConnectionPool
	TimeoutRange TimeoutRange

	CurrentSystem *system.System[T]
	SystemsList []*system.System[T]
	Systems *sync.Map
}

type LeaderElectionService [T system.MachineCommands] struct {
	lerpc.UnimplementedLeaderElectionServiceServer
	Port string
	ConnectionPool *connpool.ConnectionPool

	// Persistent State
	CurrentSystem *system.System[T]
	Systems *sync.Map

	// Module Level State
	Timeout time.Duration
	ElectionTimer *time.Timer

	ResetTimeoutSignal chan bool
	HeartbeatOnElection chan bool

	Log clog.CustomLog
}

type LEResponseChannels struct {
	BroadcastClose *chan struct{}
	VotesChan *chan int
	HigherTermDiscovered *chan int64
}