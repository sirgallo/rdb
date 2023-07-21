package leaderelection

import "time"

import "github.com/sirgallo/raft/pkg/connpool"
import "github.com/sirgallo/raft/pkg/lerpc"
import "github.com/sirgallo/raft/pkg/shared"


type LeaderElectionOpts [T comparable] struct {
	Port int
	ConnectionPool *connpool.ConnectionPool

	CurrentSystem *shared.System[T]
	SystemsList   []*shared.System[T]
}

type LeaderElectionService [T comparable] struct {
	lerpc.UnimplementedLeaderElectionServiceServer
	Port string
	ConnectionPool *connpool.ConnectionPool

	// Persistent State
	CurrentSystem *shared.System[T]
	SystemsList   []*shared.System[T]

	// Module Level State
	VotedFor string
	Timeout  time.Duration

	ResetTimeoutSignal chan bool
}