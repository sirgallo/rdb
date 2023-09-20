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

type LeaderElectionOpts struct {
	Port int
	ConnectionPool *connpool.ConnectionPool
	TimeoutRange TimeoutRange

	CurrentSystem *system.System
	Systems *sync.Map
}

type LeaderElectionService struct {
	lerpc.UnimplementedLeaderElectionServiceServer
	Port string
	ConnectionPool *connpool.ConnectionPool

	CurrentSystem *system.System
	Systems *sync.Map

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


const NAME = "Leader Election"
const RPCTimeout = 30 * time.Millisecond